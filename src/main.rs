use arrow_flight::flight_service_server::FlightServiceServer;
use std::env;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{error, info, warn};

mod db;
mod flight_error;
mod flight_service;
mod kline_processor;
mod redis_client;

use db::TDEngine;
use flight_service::KlineFlightService;
use redis_client::init_redis;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO) // 设置日志级别
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Application starting");

    // DSN 仅指向 TDengine 实例（host/port/auth），不带库名。
    // 库名由 KLINE_DB / FACTOR_DB 单独从 env 注入，避免两处重复。
    let (dsn, kline_db, factor_db) = match (
        env::var("TAOS_DSN").ok().filter(|s| !s.is_empty()),
        env::var("KLINE_DB").ok().filter(|s| !s.is_empty()),
        env::var("FACTOR_DB").ok().filter(|s| !s.is_empty()),
    ) {
        (Some(d), Some(k), Some(f)) => (d, k, f),
        _ => {
            error!("TAOS_DSN, KLINE_DB and FACTOR_DB are required (e.g. TAOS_DSN=taos://root:taosdata@localhost:6030, KLINE_DB=kline, FACTOR_DB=factor)");
            std::process::exit(1);
        }
    };

    let addr = env::var("FLIGHT_ADDR").unwrap_or_else(|_| "0.0.0.0:50001".to_string());
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

    // Initialize Redis
    if let Err(e) = init_redis(&redis_url).await {
        error!("Failed to initialize Redis at {}: {}", redis_url, e);
        warn!("Redis initialization failed; service continues without Redis cache");
    } else {
        info!("Redis initialized: {}", redis_url);
    }

    // Connect to TDengine
    let db = match TDEngine::connect(&dsn, &kline_db, &factor_db).await {
        Ok(db) => {
            info!(
                "TDengine connected: kline_db={} factor_db={} dsn={}",
                kline_db, factor_db, dsn
            );
            Arc::new(db)
        }
        Err(e) => {
            error!(
                "Failed to connect to TDengine (kline_db={} factor_db={} dsn={}): {}",
                kline_db, factor_db, dsn, e
            );
            std::process::exit(1);
        }
    };

    // Start Flight service
    let service = KlineFlightService::new(db);
    let svc = FlightServiceServer::new(service);
    let addr = addr.parse()?;
    info!("Starting Flight server on {}", addr);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
