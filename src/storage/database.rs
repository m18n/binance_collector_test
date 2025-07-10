use std::collections::HashMap;
use std::sync::Arc;
use sqlx::Executor;
use sqlx::{PgPool, Error, Row, Transaction, Postgres};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use serde::Deserialize;
use sqlx::postgres::{PgListener, PgPoolOptions, PgRow};
use crate::migrations::MIGRATIONS;
use crate::core::types::{Candle, TimeInterval, MarketDataPoint, ConfigId, TradingStrategy, MarketDataSyncLog, TradingInstrument, PairData, SyntheticPair, SyntheticPairFullData, BaseAsset, TradingPair, ValueType, StartInfo, RequestedPair};
use anyhow::{anyhow, Result};
use std::ops::DerefMut;
use mockall::automock;
use crate::core::config::Config;
use tracing::info;
#[automock]
#[async_trait::async_trait]
pub trait DatabaseInterface {

    async fn get_requested_regular_pairs(&self, config_id: i32) -> Result<Vec<RequestedPair>>;
    async fn get_requested_synthetic_pairs(&self, config_id: i32) -> Result<Vec<RequestedPair>>;
    async fn get_active_strategies(&self) -> Result<(HashMap<ConfigId, TradingStrategy>, i32)>;
    async fn get_pairs_by_log_id(&self, log_id: i32, last_configuration: &TradingStrategy) -> Result<Vec<TradingPair>>;
    async fn get_synthetic_pairs_by_log_id(&self, log_id: i32, last_configuration: &TradingStrategy) -> Result<Vec<SyntheticPairFullData>>;
    async fn get_logs_id_by_date(&self, date: NaiveDateTime) -> Result<i32>;
    async fn start_market_analysis(&self, start_date: NaiveDateTime, config_id: ConfigId) -> Result<i32>;
    async fn get_last_analysis_time(&self) -> Result<NaiveDateTime>;
    async fn finish_analysis(&self, end_date: NaiveDateTime, id: i32) -> Result<i32>;
    async fn save_trading_instrument(&self, config: &Config, trading_pair: &mut TradingPair, logs_id: i32) -> Result<()>;
    async fn get_days_count(&self) -> Result<i32>;
    async fn clean_invalid_day_records(&self) -> Result<()>;
    async fn get_start_info(&self) -> Result<StartInfo>;
    async fn get_logs_id(&self) -> Result<(i32, i32)>;
    async fn need_extra_stationaryti(&self) -> Result<bool>;
    async fn save_synthetic_trading_pair(&self, config: &Config, trading_syn_pair: &mut SyntheticPairFullData, logs_id: i32) -> Result<()>;
    async fn save_pair_stationarity_metrics(&self, pair_id: i32, log_id: i32, stationarity: f32) -> Result<i32>;
    async fn insert_pair_transaction<'tx>(&self, name: &str, is_synthetic: bool, tx: &mut Transaction<'tx, Postgres>) -> Result<i32>;
    async fn insert_stationarity_transaction<'tx>(&self, pair_id: i32, log_id: i32, stationarity: f32, tx: &mut Transaction<'tx, Postgres>) -> Result<i32>;
    async fn check_for_new_date_going(&self) -> Result<bool>;
    async fn finish_clear(&self) -> Result<()>;
    async fn run_migrations(&self) -> Result<()>;
    async fn start_update(&self, new_log_id: i32, time_now: NaiveDateTime) -> Result<()>;
    async fn finish_update(&self) -> Result<()>;
    async fn update_last_date_going_log_id(&self) -> Result<bool>;
    async fn clear_analysis_data(&self) -> Result<()>;
    async fn insert_pair_info_transaction<'tx>(&self, candles_h4: &Vec<f32>, zscores_h4: &Vec<f32>,
                                          candles_minutes: &Vec<f32>, dates_h4: &Vec<NaiveDateTime>, dates_minutes: &Vec<NaiveDateTime>,
                                          zscore_minutes: &Vec<f32>, pair_id: i32, configuration_id: ConfigId,
                                          timeframe: TimeInterval, tx: &mut Transaction<'tx, Postgres>) -> Result<i32>;
}
pub struct PostgresDataBase {
    pool:Arc<PgPool>,
}
impl PostgresDataBase{
    pub async fn new(database_url:&str)->Result<Self> {
        let pool=Arc::new( PgPoolOptions::new()
            .max_connections(15)
            .connect(&database_url)
            .await?);
        let mut db = Self {pool:pool.clone()};
        db.run_migrations().await?;
        Ok(db)
    }
}

#[async_trait::async_trait]
impl DatabaseInterface for PostgresDataBase {

    async fn get_requested_regular_pairs(&self, config_id: i32) -> Result<Vec<RequestedPair>> {
        let rows = sqlx::query(
            "SELECT p.id, p.name
         FROM pairs p
         JOIN pair_requests pr ON p.id = pr.pair_id
         WHERE pr.data_configuration_id = $1 AND p.is_synthetic = false
         GROUP BY p.id, p.name"
        )
            .bind(config_id)
            .fetch_all(self.pool.as_ref())
            .await?;

        let mut requested_pairs = Vec::with_capacity(rows.len());
        for row in rows {
            requested_pairs.push(RequestedPair {
                id: row.try_get("id")?,
                name: row.try_get("name")?,
                is_synthetic: false
            });
        }

        Ok(requested_pairs)
    }

     async fn get_requested_synthetic_pairs(&self, config_id: i32) -> Result<Vec<RequestedPair>> {
        let rows = sqlx::query(
            "SELECT p.id, p.name
         FROM pairs p
         JOIN pair_requests pr ON p.id = pr.pair_id
         WHERE pr.data_configuration_id = $1 AND p.is_synthetic = true
         GROUP BY p.id, p.name"
        )
            .bind(config_id)
            .fetch_all(self.pool.as_ref())
            .await?;

        let mut requested_pairs = Vec::with_capacity(rows.len());
        for row in rows {
            requested_pairs.push(RequestedPair {
                id: row.try_get("id")?,
                name: row.try_get("name")?,
                is_synthetic: true,
            });
        }

        Ok(requested_pairs)
    }
    async fn get_active_strategies(&self) -> Result<(HashMap<ConfigId, TradingStrategy>, i32)> {
       let mut configs: Vec<TradingStrategy> = sqlx::query_as::<_, (ConfigId, i32, f32)>(
           "SELECT id, h4, percent FROM configuration WHERE is_activated=true ORDER BY id DESC",
       )
           .fetch_all(self.pool.as_ref())
           .await?
           .into_iter()
           .map(|(id, h4, percent)| TradingStrategy {
               id,
               h1: 500,
               h4,
               percent,
           })
           .collect();
       let last_configuration_id=configs[0].id;
       let config_map = configs
           .into_iter()
           .map(|config| (config.id, config))
           .collect::<HashMap<ConfigId, TradingStrategy>>();

       Ok((config_map,last_configuration_id))
   }
    async fn get_pairs_by_log_id(&self, log_id: i32,last_configuration:&TradingStrategy) -> Result<Vec<TradingPair>> {
       let rows = sqlx::query(
           "SELECT p.id, p.name
            FROM pairs p
            JOIN stationarity_pairs sp ON p.id = sp.pair_id
            WHERE sp.log_id = $1 AND p.is_synthetic = false"
       )
           .bind(log_id)
           .fetch_all(self.pool.as_ref())
           .await?;

       let mut trading_pairs = Vec::with_capacity(rows.len());
       for row in rows {
           let mut pair=TradingPair {
               id: row.try_get("id")?,
               name:  row.try_get("name")?,
               is_request_pair:None,
               pair_data_map: HashMap::new()
           };
           pair.init_pair_data(last_configuration.id);

           trading_pairs.push(pair);
       }

       Ok(trading_pairs)
   }

    async fn get_synthetic_pairs_by_log_id(&self, log_id: i32,last_configuration:&TradingStrategy) -> Result<Vec<SyntheticPairFullData>> {
       let rows = sqlx::query(
           "SELECT p.id, p.name, sp.stationarity
            FROM pairs p
            JOIN stationarity_pairs sp ON p.id = sp.pair_id
            WHERE sp.log_id = $1 AND p.is_synthetic = true"
       )
           .bind(log_id)
           .fetch_all(self.pool.as_ref())
           .await?;

       let mut synthetic_pairs = Vec::with_capacity(rows.len());
       for row in rows {
           let pair_name: String = row.try_get("name")?;
           let parts: Vec<&str> = pair_name.split('/').collect();

           let mut syn_pair = SyntheticPairFullData::new();
           syn_pair.id = row.try_get("id")?;
           syn_pair.name = pair_name.clone();

           syn_pair.first_pair = BaseAsset {
               id: -1,
               name: parts.get(0).unwrap_or(&"").to_string(),
           };
           syn_pair.second_pair = BaseAsset {
               id: -1,
               name: parts.get(1).unwrap_or(&"").to_string(),
           };
           syn_pair.is_request_pair=None;
           syn_pair.synthetic_data=HashMap::new();

           syn_pair.init_pair_data(last_configuration.id);

           let stationarity: f32 = row.try_get("stationarity")?;
           syn_pair.get_timeframe_data_mut(last_configuration.id,&TimeInterval::h4).unwrap().stationarity=stationarity;

           synthetic_pairs.push(syn_pair);
       }

       Ok(synthetic_pairs)
   }

    async fn get_logs_id_by_date(&self, date: NaiveDateTime) -> Result<i32> {
       let date_only = date.date().and_hms_opt(0, 0, 0).unwrap();

       let row = sqlx::query(
           "SELECT id
            FROM date_calculate_logs
            WHERE DATE(start_date) = DATE($1) AND start_date!=end_date
            ORDER BY id DESC
            LIMIT 1"
       )
           .bind(date_only)
           .fetch_optional(self.pool.as_ref())
           .await?;

       match row {
           Some(row) => Ok(row.try_get("id")?),
           None => Ok(-1)
       }
   }

    async fn start_market_analysis(&self, start_date: NaiveDateTime, config_id: ConfigId) -> Result<i32> {
       let row = sqlx::query(
           "INSERT INTO date_calculate_logs (start_date,end_date,configuration_id) VALUES ($1,$2,$3) RETURNING id"
       )
           .bind(start_date).bind(start_date).bind(config_id)
           .fetch_one(self.pool.as_ref())
           .await?;

       let id: i32 = row.get("id");

       Ok(id)
   }

    async fn get_last_analysis_time(&self) -> Result<NaiveDateTime> {
       let row = sqlx::query(
           "SELECT *
   FROM date_calculate_logs WHERE start_date!=end_date
   ORDER BY start_date DESC
   LIMIT 1"
       )
           .fetch_optional(self.pool.as_ref())
           .await?;
       let mut date=NaiveDateTime::default();
       match row {
           None => {}
           Some(row) => {
               let start:NaiveDateTime=row.get("start_date");
               let end:NaiveDateTime=row.get("end_date");

                   date=end.date().and_hms_opt(0, 0, 0).unwrap();

           }
       }
       Ok(date)
   }
    async fn finish_analysis(&self, end_date: NaiveDateTime, id: i32) -> Result<i32> {
       let row = sqlx::query(
           "UPDATE date_calculate_logs SET end_date = $1 WHERE id = $2 RETURNING configuration_id;"
       )
           .bind(end_date)
           .bind(id)
           .fetch_one(self.pool.as_ref())
           .await?;
       let configuration_id:i32=row.get("configuration_id");
       Ok(configuration_id)
   }


    async fn save_trading_instrument(&self, config:&Config, trading_pair:&mut  TradingPair, logs_id:i32) ->Result<()>{
       let mut tx = self.pool.begin().await?;
       trading_pair.id = self.insert_pair_transaction(trading_pair.name.as_str(), false, &mut tx).await?;
       let last_config_id = config.get_last_config_id()?;
       if trading_pair.is_request_pair.unwrap_or(true)!=false{
           self.insert_stationarity_transaction(  trading_pair.id, logs_id, -1.0, &mut tx).await?;
       }
       tx.commit().await?;
       Ok(())
   }


    async fn get_days_count(&self) -> Result<i32> {
       let row = sqlx::query("SELECT COUNT(*) as days_count FROM date_going WHERE count > 0")
           .fetch_one(self.pool.as_ref())
           .await?;

       let days_count: i64 = row.get("days_count");

       Ok(days_count as i32)
   }
    async fn clean_invalid_day_records(&self) -> Result<()> {
       let result = sqlx::query("DELETE FROM date_going WHERE count <= 0")
           .execute(self.pool.as_ref())
           .await?;
       let result = sqlx::query("DELETE FROM date_calculate_logs WHERE start_date=end_date")
           .execute(self.pool.as_ref())
           .await?;

       Ok(())
   }
    async fn get_start_info(&self)->Result<StartInfo>{
       let (logs_id,configuration_id)=self.get_logs_id().await?;
       self.clean_invalid_day_records().await?;
       let count_day=self.get_days_count().await?;
       let mut base_date=NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
       let last_date_bypass=base_date+chrono::Duration::days(count_day as i64);
       Ok(StartInfo{
           last_logs_id: logs_id,
           last_configuration_id: configuration_id,
           base_date,
           last_date_bypass,
           current_day:count_day
       })
   }
    async fn get_logs_id(&self)->Result<(i32,i32)>{
       let row = sqlx::query(
           "SELECT *
   FROM date_calculate_logs WHERE start_date!=end_date
   ORDER BY start_date DESC
   LIMIT 1;"
       )
           .fetch_all(self.pool.as_ref())
           .await?;
       if row.len()==1{
           let id=row[0].try_get("id")?;
           let configuration_id=row[0].try_get("configuration_id")?;
           Ok((id,configuration_id))
       }else{
           Ok((-1,-1))
       }
   }
    async fn need_extra_stationaryti(&self)->Result<bool>{
       let row = sqlx::query(
           "SELECT *
   FROM date_calculate_logs WHERE start_date!=end_date
   ORDER BY start_date DESC
   LIMIT 1;"
       )
           .fetch_all(self.pool.as_ref())
           .await?;
       if row.len()==1{
           let start_date:NaiveDateTime=row[0].try_get("start_date")?;
           let end_date:NaiveDateTime=row[0].try_get("end_date")?;
           if start_date==end_date||end_date==NaiveDateTime::default(){
               return Ok(true);
           }
           Ok(false)
       }else{
           Ok(true)
       }
   }
    async fn save_synthetic_trading_pair(&self, config:&Config, trading_syn_pair:&mut SyntheticPairFullData, logs_id:i32) ->Result<()>{
       let mut tx = self.pool.begin().await?;
       trading_syn_pair.id = self.insert_pair_transaction(trading_syn_pair.name.as_str(), true, &mut tx).await?;

       let last_config_id = config.get_last_config_id()?;

       let stationarity = if let Some(timeframes) = trading_syn_pair.synthetic_data.get(&last_config_id) {
           if let Some(h4_data) = timeframes.iter().find(|data| data.time_interval == TimeInterval::h4) {
               h4_data.stationarity
           } else {
               return Err(anyhow!("Error: h4 dont find"));
           }
       } else {
           return Err(anyhow!("Error: config_id dont find"));
       };
       if trading_syn_pair.is_request_pair.unwrap_or(true)!=false {
           self.insert_stationarity_transaction(trading_syn_pair.id, logs_id, stationarity, &mut tx).await?;
       }

       tx.commit().await?;
       Ok(())
   }

    async fn save_pair_stationarity_metrics(&self, pair_id:i32, log_id:i32, stationarity:f32) -> Result<i32> {
       let row = sqlx::query(
           "INSERT INTO stationarity_pairs (pair_id, log_id, stationarity) VALUES ($1, $2, $3) RETURNING id",

       ).bind(pair_id).bind(log_id).bind(stationarity)
           .fetch_one(self.pool.as_ref())
           .await?;
       let id: i32 = row.get("id");
       Ok(id)
   }


    async fn insert_pair_transaction<'tx>(&self, name: &str, is_synthetic: bool,tx:&mut Transaction<'tx,Postgres>) -> Result<i32> {
       let row = sqlx::query(
           "INSERT INTO pairs (name, is_synthetic) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET name = pairs.name
RETURNING id;
",
       ).bind(name).bind(is_synthetic)
           .fetch_one(tx.deref_mut())
           .await?;
       let mut id= row.get("id");
       Ok(id)
   }

    async fn insert_stationarity_transaction<'tx>(&self, pair_id:i32,log_id:i32,stationarity:f32,tx:&mut Transaction<'tx,Postgres>) -> Result<i32> {
       let row = sqlx::query(
           "INSERT INTO stationarity_pairs (pair_id, log_id, stationarity) VALUES ($1, $2, $3) RETURNING id",

       ).bind(pair_id).bind(log_id).bind(stationarity)
           .fetch_one(tx.deref_mut())
           .await?;
       let id: i32 = row.get("id");
       Ok(id)
   }
    async fn check_for_new_date_going(&self) -> Result<bool> {
       let last_id_row = sqlx::query("SELECT count FROM date_going ORDER BY id DESC LIMIT 1")
           .fetch_optional(self.pool.as_ref())
           .await?;
       if last_id_row.is_some(){
           let row=last_id_row.unwrap();
           let count: i32 = row.get("count");
           if count==36{
               return Ok(true);
           }
       }else{
           return Ok(true);
       }
       Ok(false)
   }
    async fn finish_clear(&self)-> Result<()> {
       let mut tx = self.pool.begin().await?;
       sqlx::query("DELETE FROM date_going")
           .execute(&mut *tx)
           .await?;

       tx.commit().await?;

       info!("All data from stationarity_pairs and pairs_info tables has been deleted");
       Ok(())
   }
    async fn run_migrations(&self) -> Result<()> {
        let create_version_table_commands = vec![
            r#"
        CREATE TABLE IF NOT EXISTS version_binance_collector (
            id SERIAL PRIMARY KEY,
            version_number INTEGER NOT NULL
        )
        "#,
            r#"
        INSERT INTO version_binance_collector (version_number)
        SELECT 0
        WHERE NOT EXISTS (SELECT 1 FROM version_binance_collector)
        "#
        ];

        for command in create_version_table_commands {
            sqlx::query(command).execute(self.pool.as_ref()).await?;
        }

        let current_version: i32 = sqlx::query_scalar("SELECT version_number FROM version_binance_collector LIMIT 1")
            .fetch_one(self.pool.as_ref())
            .await?;

        for migration in MIGRATIONS.iter() {
            if migration.from >= current_version {
                let mut tx = self.pool.begin().await?;

                let mut inside_dollar_quote = false;
                let mut command_buffer = String::new();

                for line in migration.script.lines() {
                    let trimmed_line = line.trim();

                    if trimmed_line.contains("$$") {
                        inside_dollar_quote = !inside_dollar_quote;
                    }

                    command_buffer.push_str(line);
                    command_buffer.push('\n');

                    if !inside_dollar_quote && trimmed_line.ends_with(';') {
                        sqlx::query(&command_buffer).execute(&mut *tx).await?;
                        command_buffer.clear(); // Очищаємо буфер для наступної команди
                    }
                }

                if !command_buffer.trim().is_empty() {
                    sqlx::query(&command_buffer).execute(&mut *tx).await?;
                }

                sqlx::query("UPDATE version_binance_collector SET version_number = $1")
                    .bind(migration.to)
                    .execute(&mut *tx)
                    .await?;

                tx.commit().await?;
            }
        }

        Ok(())
    }
     async fn start_update(&self,new_log_id: i32,time_now:NaiveDateTime)->Result<()>{
        let result = sqlx::query("INSERT INTO date_going (day, count,log_id)
                        VALUES ($1, -1, $2)")
            .bind(time_now)
            .bind(new_log_id)
            .execute(self.pool.as_ref())
            .await?;
        Ok(())
    }
     async fn finish_update(&self)->Result<()>{
        self.update_last_date_going_log_id().await?;
        Ok(())
    }
     async fn update_last_date_going_log_id(&self) -> Result<bool> {

        let last_id_row = sqlx::query("SELECT id FROM date_going ORDER BY id DESC LIMIT 1")
            .fetch_optional(self.pool.as_ref())
            .await?;

        if let Some(row) = last_id_row {
            let last_id: i32 = row.get("id");
            let result = sqlx::query("UPDATE date_going SET count = 0 WHERE id = $1")
                .bind(last_id)
                .execute(self.pool.as_ref())
                .await?;
            return Ok(result.rows_affected() > 0);
        }
        Ok(false)
    }

     async fn clear_analysis_data(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM pairs_info")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        info!("All data from stationarity_pairs and pairs_info tables has been deleted");
        Ok(())
    }
     async fn insert_pair_info_transaction<'tx>(&self, candles_h4:&Vec<f32>, zscores_h4:&Vec<f32>, candles_minutes: &Vec<f32>, dates_h4: &Vec<NaiveDateTime>, dates_minutes: &Vec<NaiveDateTime>, zscore_minutes:&Vec<f32>, pair_id: i32, configuration_id:ConfigId, timeframe: TimeInterval, tx:&mut Transaction<'tx,Postgres>) -> Result<i32> {
        let zscores_h4_last_240 = if zscores_h4.len() > 240 {
            zscores_h4[zscores_h4.len() - 240..].to_vec()
        } else {
            zscores_h4.clone()
        };
        let row = sqlx::query(
            "INSERT INTO pairs_info (candles_minutes, dates_h4, dates_minutes, zscores_minutes, candles_h4, zscores_h4, pair_id, candles_timeframe,configuration_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (pair_id, candles_timeframe, configuration_id) DO NOTHING;",
        ).bind(&candles_minutes).bind(&dates_h4).bind(&dates_minutes).bind(&zscore_minutes).bind(candles_h4).bind(zscores_h4_last_240).bind(pair_id).bind(timeframe.clone()).bind(configuration_id)
            .execute(tx.deref_mut())
            .await?;
        Ok(0)
    }
}