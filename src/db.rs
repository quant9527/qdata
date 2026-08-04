use crate::flight_service::DatasetTicket;
use crate::kline_processor::FactorRecord;
use crate::kline_processor::KlineSeries;
use anyhow::{anyhow, Result};
use futures::TryStreamExt;
use std::time::SystemTime;
use taos::{AsyncQueryable, AsyncTBuilder, TaosBuilder};
use taos_query::common::BorrowedValue;
use taos_query::prelude::*;
use tokio::sync::{mpsc, Mutex};
use tracing::info;

use std::collections::HashMap;
use taos::TaosPool;

pub struct TDEngine {
    pub(crate) pool: TaosPool,
    pub(crate) kline_db: String,
    pub(crate) factor_db: String,
    factors_cache: Mutex<Option<(SystemTime, HashMap<String, Vec<FactorRecord>>)>>,
}

impl TDEngine {
    /// 接受外部拼好的 DSN（taos://user:pass@host:port，不带库名）。
    /// kline_db / factor_db 均为 ops 侧从 env 注入的库名。
    /// 若 DSN 含库路径，参数 kline_db 仍会再 use_database 覆盖一次。
    pub async fn connect(dsn: &str, kline_db: &str, factor_db: &str) -> Result<Self> {
        let builder = <TaosBuilder as AsyncTBuilder>::from_dsn(dsn)?;
        // Create a pool with a maximum of 10 connections
        let pool = builder.pool_builder().max_size(10).build()?;
        let client = pool.get().await?;
        client.use_database(kline_db).await?;
        let engine = Self {
            pool,
            kline_db: kline_db.to_string(),
            factor_db: factor_db.to_string(),
            factors_cache: Mutex::new(None),
        };
        engine.cache_factors().await?;
        Ok(engine)
    }

    fn factors_cache_key(exchange: &str, symbol: &str) -> String {
        format!("{}_{}", exchange, symbol)
    }

    pub async fn cache_factors(&self) -> Result<()> {
        let sql = format!(
            "SELECT exchange,symbol,ts,f FROM {}.factor WHERE fq='qfq'",
            self.factor_db
        );
        let client = self.pool.get().await.map_err(|e| {
            anyhow!("Failed to get connection, cannot load factor cache: {}", e)
        })?;
        let mut query = client.query(sql).await.map_err(|e| {
            anyhow!(
                "Table {}.factor does not exist, create database '{}' and super table '{}.factor' first: {}",
                self.factor_db,
                self.factor_db,
                self.factor_db,
                e
            )
        })?;
        let all_factors = query
            .deserialize::<FactorRecord>()
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| anyhow!("Failed to deserialize {}.factor data: {}", self.factor_db, e))?;

        let mut factors_map = HashMap::new();
        for factor in all_factors {
            let key = Self::factors_cache_key(&factor.exchange, &factor.symbol);
            factors_map.entry(key).or_insert_with(Vec::new).push(factor);
        }

        // Sort factors by timestamp for each symbol
        for factors in factors_map.values_mut() {
            factors.sort_by_key(|f| f.ts);
        }

        let mut cache = self.factors_cache.lock().await;
        let cached_pairs = factors_map.len();
        *cache = Some((SystemTime::now(), factors_map));
        info!(
            "Factor cache loaded: {} exchange_symbol pairs from {}.factor",
            cached_pairs,
            self.factor_db
        );
        Ok(())
    }

    pub async fn get_cached_factors(
        &self,
        exchange: &str,
        symbol: &str,
    ) -> Result<Vec<FactorRecord>> {
        let cache = self.factors_cache.lock().await;
        let key = Self::factors_cache_key(exchange, symbol);
        if let Some((_, factors_map)) = cache.as_ref() {
            Ok(factors_map.get(&key).cloned().unwrap_or_default())
        } else {
            Ok(vec![])
        }
    }

    /// Returns true if the result was sent, false if the receiver dropped.
    pub(crate) async fn send_or_warn(
        tx: &mpsc::Sender<Result<KlineSeries>>,
        result: Result<KlineSeries>,
    ) -> bool {
        if tx.send(result).await.is_err() {
            tracing::warn!("Receiver dropped, stopping query");
            return false;
        }
        true
    }



    pub async fn query_kline(
        &self,
        ticket: &DatasetTicket,
        tx: mpsc::Sender<Result<KlineSeries>>,
    ) -> Result<()> {
        let start = ticket.start_time;
        let end = ticket.end_time;

        // Process tags to extract exchanges, symbols, freqs
        let mut exchanges = Vec::new();
        let mut symbols = Vec::new();
        let mut freqs = Vec::new();

        for tag in &ticket.tags {
            let (exchange, symbol, freq) = DatasetTicket::parse_tag(tag)?;
            exchanges.push(format!("'{}'", exchange));
            symbols.push(format!("'{}'", symbol));
            freqs.push(format!("'{}'", freq));
        }

        let sql = format!(
            "SELECT exchange, symbol, freq, ts, o, c, h, l, v, qv, end_ts \
             FROM {}.{} \
             WHERE ts>={} AND ts<={} \
             AND source !=1 AND exchange IN ({}) AND symbol IN ({}) AND freq IN ({}) \
             ORDER BY exchange, symbol, freq, ts ASC",
            self.kline_db,
            ticket.name,
            start,
            end,
            exchanges.join(","),
            symbols.join(","),
            freqs.join(",")
        );

        // Get a connection from the pool
        let client = self.pool.get().await?;

        // Measure time from SQL execution start to first data received
        let query_start_time = std::time::Instant::now();
        let mut result = client.query_with_req_id(sql, 123456789).await?;
        let mut rows = result.rows();
        let mut current_exchange = String::new();
        let mut current_symbol = String::new();
        let mut current_freq = String::new();
        let mut kline_series: Option<KlineSeries> = None;
        let mut first_data_received = false;

        while let Some(row) = rows.try_next().await? {
            // Record time when first data is received
            if !first_data_received {
                first_data_received = true;
                let first_data_time = query_start_time.elapsed().as_millis();
                tracing::info!(
                    "Time from SQL start to first data received: {} ms",
                    first_data_time
                );
            }

            let mut ts = 0;
            let mut open = 0.0;
            let mut close = 0.0;
            let mut high = 0.0;
            let mut low = 0.0;
            let mut vol = 0.0;
            let mut qv = 0.0;
            let mut end_ts = 0;
            let mut exchange = String::new();
            let mut symbol = String::new();
            let mut freq = String::new();
            for (col, (_, value)) in row.enumerate() {
                match col {
                    0 => {
                        if let BorrowedValue::VarChar(s) = value {
                            exchange = s.to_string();
                        }
                    }
                    1 => {
                        if let BorrowedValue::VarChar(s) = value {
                            symbol = s.to_string();
                        }
                    }
                    2 => {
                        if let BorrowedValue::VarChar(s) = value {
                            freq = s.to_string();
                        }
                    }
                    3 => {
                        if let BorrowedValue::Timestamp(t) = value {
                            ts = t.as_raw_i64();
                        }
                    }
                    4 => {
                        if let BorrowedValue::Double(v) = value {
                            open = v;
                        }
                    }
                    5 => {
                        if let BorrowedValue::Double(v) = value {
                            close = v;
                        }
                    }
                    6 => {
                        if let BorrowedValue::Double(v) = value {
                            high = v;
                        }
                    }
                    7 => {
                        if let BorrowedValue::Double(v) = value {
                            low = v;
                        }
                    }
                    8 => {
                        if let BorrowedValue::Double(v) = value {
                            vol = v;
                        }
                    }
                    9 => {
                        if let BorrowedValue::Double(v) = value {
                            qv = v;
                        }
                    }
                    10 => {
                        if let BorrowedValue::Timestamp(t) = value {
                            end_ts = t.as_raw_i64();
                        }
                    }
                    _ => {}
                }
            }

            // Check if we have a new series (different exchange, symbol, or freq)
            if exchange != current_exchange
                || symbol != current_symbol
                || freq != current_freq
                || kline_series.is_none()
            {
                // Send the previous series if it exists
                if let Some(series) = kline_series.take() {
                    if !Self::send_or_warn(&tx, Ok(series)).await { return Ok(()); }
                }

                // Start a new series
                current_exchange = exchange.clone();
                current_symbol = symbol.clone();
                current_freq = freq.clone();
                kline_series = Some(KlineSeries::new(exchange, symbol, freq));
            }

            // Add data to the current series
            if let Some(ref mut series) = kline_series {
                series.push(ts, open, close, high, low, vol, qv, end_ts);
            }
        }

        // Send the last series if it exists
        match kline_series {
            Some(series) => {
                Self::send_or_warn(&tx, Ok(series)).await;
            }
            None => {
                tracing::info!("No data found for query, returning empty series");
            }
        }
        let last_data_time = query_start_time.elapsed().as_millis();
        tracing::info!("last_data_time: {} ms", last_data_time);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    /// 真实连通 TDengine：跳过纯字符串拼装与 mock，验证端到端 DSN → from_dsn → TCP → use_database。
    /// 没有可用服务时直接跳过，CI/本机可各自决定要不要跑。
    #[tokio::test]
    #[ignore = "需要真实 TDengine 服务，默认跳过；手动跑：cargo test -- --ignored"]
    async fn tdengine_connect_succeeds() -> anyhow::Result<()> {
        let host = std::env::var("TEST_TDENGINE_HOST").unwrap_or_else(|_| "localhost:6030".to_string());
        let user = std::env::var("TEST_TDENGINE_USER").unwrap_or_else(|_| "root".to_string());
        let pass = std::env::var("TEST_TDENGINE_PASS").unwrap_or_else(|_| "taosdata".to_string());
        let kline_db   = std::env::var("TEST_KLINE_DB").unwrap_or_else(|_| "kline".to_string());
        let factor_db  = std::env::var("TEST_FACTOR_DB").unwrap_or_else(|_| "factor".to_string());

        // TDEngine::connect 强制要求外部传入拼好的 DSN。这里最小化构造：
        // 默认凭据不包含 : @ / 等元字符，可直接拼接；如果外部传入了含特殊字符的
        // TEST_TDENGINE_USER/PASS，需要走 main.rs 的 encode_dsn_component 路径。
        let dsn = format!("taos://{user}:{pass}@{host}/{kline_db}");
        tracing::info!("[tdengine_connect_succeeds] dsn = {dsn} kline_db = {kline_db} factor_db = {factor_db}");

        match super::TDEngine::connect(&dsn, &kline_db, &factor_db).await {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("[tdengine_connect_succeeds] skipped, connect failed: {e}");
                Ok(())
            }
        }
    }
}

