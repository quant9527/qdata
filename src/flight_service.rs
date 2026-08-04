use crate::flight_error::to_flight_error;
use crate::kline_processor::{KlineProcessor, KlineSeries};

use crate::db::TDEngine;
use arrow::record_batch::RecordBatch;
use arrow_flight::{
    encode::FlightDataEncoderBuilder, flight_service_server::FlightService,
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{
    stream::{BoxStream, StreamExt},
    TryStreamExt,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use taos::tokio::sync::mpsc;
use tonic::server::NamedService;
use tonic::{Request, Response, Status, Streaming};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetTicket {
    pub name: String,
    pub tags: Vec<String>,
    pub start_time: i64,
    pub end_time: i64,
    #[serde(default)]
    pub kline_reverse: bool,
    #[serde(default)]
    pub kline_aggregate: String,
    #[serde(default)]
    pub kline_snapshot: bool,
    #[serde(default)]
    pub recent_seg_n: usize,
}

impl DatasetTicket {
    /// Parse a tag string "exchange_symbol_freq" into its three components.
    /// Uses split_once/rsplit_once to handle symbols that may contain underscores.
    pub fn parse_tag(tag: &str) -> anyhow::Result<(&str, &str, &str)> {
        let (exchange, rest) = tag.split_once('_')
            .ok_or_else(|| anyhow::anyhow!("Invalid tag format '{}': expected exchange_symbol_freq", tag))?;
        let (symbol, freq) = rest.rsplit_once('_')
            .ok_or_else(|| anyhow::anyhow!("Invalid tag format '{}': expected exchange_symbol_freq", tag))?;
        Ok((exchange, symbol, freq))
    }
}

/// 将 KlineDataSeries 转换为 RecordBatch 的函数
async fn kline_data_to_record_batch(
    kline_data: KlineSeries,
    reverse: bool,
    kline_aggregate: String,
    factors: Vec<crate::kline_processor::FactorRecord>,
    kline_snapshot: bool,
    recent_seg_n: usize,
) -> Result<RecordBatch, anyhow::Error> {
    let mut kline_data = kline_data;

    if kline_data.is_empty() && !kline_snapshot {
        return Err(anyhow::anyhow!("Empty kline data"));
    }

    // If kline_snapshot is enabled, try to get the latest snapshot from Redis
    if kline_snapshot {
        // Get Redis client
        match crate::redis_client::get_redis_client().await {
            Ok(redis_client) => {
                // Get snapshot data from Redis
                match redis_client
                    .get_kline_snapshot(&kline_data.exchange, &kline_data.symbol, &kline_data.freq)
                    .await
                {
                    Ok(Some(snapshot)) => {
                        // Check if we have data and the snapshot is newer than the last data point
                        if !kline_data.is_empty() {
                            let last_ts = *kline_data.timestamps.last().unwrap();
                            if snapshot.timestamp > last_ts {
                                // Append the snapshot data
                                kline_data.push(
                                    snapshot.timestamp,
                                    snapshot.open,
                                    snapshot.close,
                                    snapshot.high,
                                    snapshot.low,
                                    snapshot.vol,
                                    snapshot.quote_vol, // Using quote_vol as qv
                                    snapshot.timestamp, // Using timestamp as end_ts
                                );
                            }
                            // If snapshot.timestamp == last_ts, we ignore it (no duplicates)
                            // If snapshot.timestamp < last_ts, we also ignore it (outdated data)
                        } else {
                            // If we have no data, use the snapshot as the only data point
                            kline_data.push(
                                snapshot.timestamp,
                                snapshot.open,
                                snapshot.close,
                                snapshot.high,
                                snapshot.low,
                                snapshot.vol,
                                snapshot.quote_vol, // Using quote_vol as qv
                                snapshot.timestamp, // Using timestamp as end_ts
                            );
                        }
                    }
                    Ok(None) => {
                        // No snapshot found, continue with original data
                        tracing::info!(
                            "No snapshot found for {}_{}_{}",
                            kline_data.exchange,
                            kline_data.symbol,
                            kline_data.freq
                        );
                    }
                    Err(e) => {
                        tracing::warn!("Failed to get kline snapshot from Redis: {}", e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to get Redis client: {}", e);
            }
        }
    }

    if kline_data.is_empty() {
        return Err(anyhow::anyhow!("Empty kline data"));
    }

    // 1. 创建 KlineProcessor 并直接加载数据
    let mut proc = KlineProcessor::new(
        &kline_data.exchange,
        &kline_data.symbol,
        &kline_data.freq,
        factors, // Use the passed factors instead of empty vector
    )
    .load(kline_data);

    let mut qfq_done = false;

    // 如果需要反转或聚合，都需要先应用qfq
    if reverse || !kline_aggregate.is_empty() {
        for _i in 0..proc.len() {
            // Apply qfq
            proc.qfq();
            proc.qfq_idx += 1;
        }
        qfq_done = true;

        // 如果需要反转，则进行反转
        if reverse {
            proc.reverse_prices();
        }

        // 如果需要聚合，则进行聚合
        if !kline_aggregate.is_empty() {
            proc = proc.aggregate(&kline_aggregate);
        }
    }

    // 2. 循环计算指标（如果还没有应用过qfq，则在这里应用）
    if recent_seg_n > 0 {
        proc.init_segments();
    }
    for _i in 0..proc.len() {
        if !qfq_done {
            // Apply qfq
            proc.qfq();
            proc.qfq_idx += 1;
        }

        // Calculate indicators (含段状态机增量更新)
        proc.indicators(recent_seg_n);
        proc.ind_idx += 1;
    }

    // 3. 创建 RecordBatch
    proc.create_record_batch()
}
#[derive(Clone)]
pub struct KlineFlightService {
    db: Arc<TDEngine>,
}

impl NamedService for KlineFlightService {
    const NAME: &'static str = "arrow.flight.protocol.FlightService";
}

impl KlineFlightService {
    pub fn new(db: Arc<TDEngine>) -> Self {
        Self { db }
    }
}

#[tonic::async_trait]
impl FlightService for KlineFlightService {
    type HandshakeStream =
        BoxStream<'static, std::result::Result<HandshakeResponse, tonic::Status>>;
    type ListFlightsStream = BoxStream<'static, std::result::Result<FlightInfo, tonic::Status>>;
    type DoGetStream = BoxStream<'static, std::result::Result<FlightData, tonic::Status>>;
    type DoPutStream = BoxStream<'static, std::result::Result<PutResult, tonic::Status>>;
    type DoActionStream =
        BoxStream<'static, std::result::Result<arrow_flight::Result, tonic::Status>>;
    type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, tonic::Status>>;
    type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, tonic::Status>>;

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        unimplemented!()
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!()
    }
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let start_time = std::time::Instant::now();
        let message = request.into_inner();
        let query = String::from_utf8(message.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let default_request: DatasetTicket =
            serde_json::from_str(&query).map_err(|e| Status::invalid_argument(e.to_string()))?;

        tracing::info!(
            "Processing do_get request: {:?} {:?}",
            default_request.name,
            default_request.tags.len()
        );

        // 创建一个通道，用于从数据库查询任务发送 KlineSeries
        // Create a channel for sending KlineSeries from the database query
        let (tx, rx) = mpsc::channel::<Result<KlineSeries, anyhow::Error>>(100);

        // 生成一个任务来查询数据库并通过通道发送批次
        // Spawn a task to query the database and send batches through the channel
        let producer_handle = tokio::spawn({
            let db = self.db.clone();
            let default_request = default_request.clone();
            async move {
                let error_tx = tx.clone();
                match db.query_kline(&default_request, tx).await {
                    Ok(()) => {
                        // Query completed successfully, batches were sent through the channel
                        tracing::info!("Query completed successfully");
                    }
                    Err(e) => {
                        tracing::error!("Query error: {}", e);
                        // Signal the error through the channel so the flight stream
                        // reports it to the client rather than ending silently.
                        TDEngine::send_or_warn(&error_tx, Err(e)).await;
                    }
                }
            }
        });

        // 将接收器转换为流，并在传输前将 KlineData 转换为 RecordBatch
        // Convert the receiver into a stream and convert KlineData to RecordBatch before transmission
        let rx_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let kline_aggregate = default_request.kline_aggregate.clone();
        let kline_reverse = default_request.kline_reverse;
        let kline_snapshot = default_request.kline_snapshot;
        let recent_seg_n = default_request.recent_seg_n;
        let db = self.db.clone(); // Clone db for use in the async block
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(
                rx_stream
                    .then(move |result| {
                        let db = db.clone();
                        let kline_aggregate = kline_aggregate.clone();
                        let recent_seg_n = recent_seg_n;
                        async move {
                            let kline_data = result.map_err(to_flight_error)?;
                            let factors = db
                                .get_cached_factors(&kline_data.exchange, &kline_data.symbol)
                                .await
                                .map_err(to_flight_error)?;
                            kline_data_to_record_batch(
                                kline_data,
                                kline_reverse,
                                kline_aggregate,
                                factors,
                                kline_snapshot,
                                recent_seg_n,
                            )
                            .await
                            .map_err(to_flight_error)
                        }
                    })
                    .map_err(arrow_flight::error::FlightError::into),
            )
            .map_err(arrow_flight::error::FlightError::into);

        // 等待生产者任务完成
        // Wait for the producer task to complete
        tokio::spawn(async move {
            if let Err(e) = producer_handle.await {
                tracing::error!("Query task error: {}", e);
            }
            let elapsed = start_time.elapsed().as_millis();
            tracing::info!(
                "Request completed - duration: {}ms, dataset: {}, tags: {:?}",
                elapsed,
                default_request.name,
                default_request.tags.len()
            );
        });

        Ok(Response::new(
            Box::pin(flight_data_stream) as Self::DoGetStream
        ))
    }

    // 实现其他必要的FlightService方法
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!()
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        unimplemented!()
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        unimplemented!()
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        unimplemented!()
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        use std::fs;
        use std::path::Path;
        use taos::*;
        use tracing::{error, info};

        // Get a connection from the pool
        let client =
            self.db.pool.get().await.map_err(|e| {
                Status::internal(format!("Failed to get database connection: {}", e))
            })?;

        // Read SQL file
        let sql_file_path = "./python_client/eth_kline_insert.sql";
        if !Path::new(sql_file_path).exists() {
            let error_msg = format!("SQL file not found: {}", sql_file_path);
            error!("{}", error_msg);
            return Err(Status::not_found(error_msg));
        }

        let sql_content = fs::read_to_string(sql_file_path).map_err(|e| {
            let error_msg = format!("Failed to read SQL file: {}", e);
            error!("{}", error_msg);
            Status::internal(error_msg)
        })?;

        info!("Read SQL file with {} characters", sql_content.len());

        // Execute the SQL
        match client.exec(sql_content).await {
            Ok(_) => {
                info!("Successfully inserted ETH kline data into TDengine");
            }
            Err(e) => {
                let error_msg = format!("Failed to insert ETH kline data: {}", e);
                error!("{}", error_msg);
                return Err(Status::internal(error_msg));
            }
        }

        // Create a simple response stream
        let output = futures::stream::once(async { Ok(PutResult::default()) });
        Ok(Response::new(Box::pin(output)))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        unimplemented!()
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }
}
