
pub mod migrations;
mod analysis;
mod core;
mod exchange;
mod storage;
mod binance_collector;
mod mathematics;
mod logic;

use std::collections::HashMap;
use std::{env, process};
use std::error::Error;
use std::io::{Write, BufWriter};
use std::sync::{Arc};
use std::time::{Duration, Instant};
use binance_sync::api::*;
use binance_sync::futures::market::*;
use chrono::{Local, NaiveDateTime, Utc};
use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use tokio::task;
use futures::future::join_all;
use ndarray::array;
use sqlx::Error::Database;
use tokio::sync::Mutex;
use tokio::time::interval;
use anyhow::{anyhow, Result};
use tracing::{error, info, subscriber::set_global_default};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::Layer;
use tracing_error::ErrorLayer;
use crate::binance_collector::BinanceCollector;
use crate::exchange::binance::{BinanceExchange, ExchangeInterface};
use crate::storage::database::{DatabaseInterface, PostgresDataBase};

async  fn start()->Result<()>{
    let api_key = env::var("API_KEY")
        .expect("DATABASE_URL must be set in .env file or environment");
    let secret_key = env::var("SECRET_KEY")
        .expect("DATABASE_URL must be set in .env file or environment");
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in .env file or environment");
    let db=PostgresDataBase::new(database_url.as_str()).await?;
    let binance=BinanceExchange::new(api_key,secret_key)?;
    let mut bot = BinanceCollector::new(db,binance).await?;

    loop{
        if bot.is_stationarity_time().await? {

            bot.run_stationarity().await?;
        }
        tokio::time::sleep(Duration::from_millis(5000)).await;
    }
    Ok(())
}


#[tokio::main]
async fn main() -> Result<()>  {
    let program_name = "binance_collector_test";
    let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    let file_name = format!("{}_{}.log", program_name, timestamp);
    let log_dir = "./logs/";
    let file_appender = RollingFileAppender::new(Rotation::NEVER, log_dir, file_name);
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(fmt::layer()
            .with_writer(file_appender)
            .with_ansi(false))
        .with(ErrorLayer::default());
    set_global_default(subscriber).expect("Unable to set a global subscriber");

    dotenv().ok();
    let version = env!("CARGO_PKG_VERSION");
    info!("Program version: {}", version);
    start().await.map_err(|e| {
        error!("Application failed: {:?}", e);
        e
    })?;
    Ok(())
}