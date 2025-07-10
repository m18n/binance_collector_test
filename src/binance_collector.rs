use std::env;
use std::sync::Arc;
use tokio::time::{sleep};
use binance_async::futures::market::FuturesMarket;
use tokio::sync::Mutex;
use crate::analysis::asset::{AssetAnalyzer};

use crate::core::types::{BaseAsset, SyntheticPair, SyntheticPairFullData, TimeInterval};
use crate::exchange::binance::{BinanceExchange, ExchangeInterface};

use crate::storage::database::{DatabaseInterface, PostgresDataBase};
use anyhow::{anyhow, Result};
use binance_sync::futures::general::FuturesGeneral;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use serde_json::json;

use tokio::task;
use tracing::info;
use crate::core::config::Config;
use crate::logic::{generate_synthetic_pair, remove_duplicate_full_pairs};
use crate::core::types::{ConfigId, TradingPair, PairData};


pub struct BinanceCollector<D: DatabaseInterface,C:ExchangeInterface> {
    exchange: C,
    storage: D,
    asset_analyzer: AssetAnalyzer,
    config:Config,
    current_day:i32,
    logs_id: i32,
    base_date:NaiveDateTime,
    last_date_bypass: NaiveDateTime,
}
const MAX_DAYS: i32 = 270;
impl<D: DatabaseInterface,C:ExchangeInterface> BinanceCollector<D,C>{
    pub async fn new(db:D,exchange:C)-> Result<BinanceCollector<D,C>> {
        let mut start_info =db.get_start_info().await?;
        let mut bot=BinanceCollector{current_day:start_info.current_day,
            last_date_bypass:start_info.last_date_bypass,base_date:start_info.base_date,logs_id:start_info.last_logs_id,
            exchange,config:Config::new(&db).await?,storage:db,asset_analyzer:AssetAnalyzer::new()};
        Ok(bot)
    }
    pub fn get_time_now(&self) ->NaiveDateTime{

        self.last_date_bypass
    }

    async fn start_to_stationarity(&mut self, new_log_id:i32) ->Result<NaiveDateTime>{//%
        info!("start stationarity");
        let time=self.get_time_now();
        let mut log_id=0;
        if new_log_id !=-1 {
            log_id= new_log_id;
        }else {
            let config_id = self.config.get_last_config_id()?;
            let id = self.storage.start_market_analysis(time.clone(), config_id).await?;
            log_id=id;
            self.logs_id = id;
        }
        self.last_date_bypass = time;
        self.storage.start_update(log_id,time).await?;
        Ok(time)
    }
    async fn end_to_stationarity(&mut self,new_log_id:i32)->Result<()> {//%
        info!("end stationarity");
        if new_log_id==-1 {
            let date = self.get_time_now() + chrono::Duration::hours(1);
            self.storage.finish_analysis(date, self.logs_id).await?;

        }
        self.storage.finish_update().await?;
        Ok(())
    }
    fn serialize_trading_pair(&self, pair: &TradingPair) -> Result<String> {
        let config_id = self.config.get_last_config_id()?;

        let h4_data = pair.get_timeframe_data(config_id, &TimeInterval::h4)
            .ok_or_else(|| anyhow!("Немає даних H4 для пари {}", pair.name))?;

        let pair_json = json!({
            "id": pair.id,
            "name": pair.name,
            "is_request_pair": pair.is_request_pair,
            "stationarity": h4_data.stationarity,
            "candles": h4_data.candles_h4.iter().map(|c| {
                json!({
                    "price": c.price,
                    "close_time": c.close_time.to_string()
                })
            }).collect::<Vec<_>>(),
            "z_scores": h4_data.z_score_h4,
            "z_score_minutes": h4_data.z_score_minutes,
            "candles_minutes":h4_data.candles_minutes.iter().map(|c| {

                    c.price

            }).collect::<Vec<_>>(),

            "configuration_id": config_id
        });

        Ok(serde_json::to_string(&pair_json)?)
    }

    fn serialize_synthetic_pair(&self, syn_pair: &SyntheticPairFullData) -> Result<String> {
        let config_id = self.config.get_last_config_id()?;

        let h4_data = syn_pair.get_timeframe_data(config_id, &TimeInterval::h4)
            .ok_or_else(|| anyhow!("Немає даних H4 для синтетичної пари {}", syn_pair.name))?;

        let syn_pair_json = json!({
            "id": syn_pair.id,
            "name": syn_pair.name,
            "is_request_pair": syn_pair.is_request_pair,
            "first_pair": {
                "name": syn_pair.first_pair.name
            },
            "second_pair": {
                "name": syn_pair.second_pair.name
            },
            "stationarity": h4_data.stationarity,
            "candles": h4_data.candles_h4.iter().map(|c| {
                json!({
                    "price": c.price,
                    "close_time": c.close_time.to_string()
                })
            }).collect::<Vec<_>>(),
            "z_scores": h4_data.z_score_h4,
            "z_score_minutes": h4_data.z_score_minutes,
            "candles_minutes":h4_data.candles_minutes.iter().map(|c| {

                    c.price

            }).collect::<Vec<_>>(),
            "configuration_id": config_id
        });

        Ok(serde_json::to_string(&syn_pair_json)?)
    }



    async fn send_pairs_to_http_server(&self, pairs: &Vec<TradingPair>, synthetic_pairs: &Vec<SyntheticPairFullData>) -> Result<()> {
        let client = reqwest::Client::new();
        let base_url = env::var("URL").unwrap_or_else(|_| "http://localhost:3000/upload".to_string());

        const BATCH_SIZE: usize = 100;

        info!("Clearing old data...");
        let clear_response = client.post(&format!("{}/clear", base_url))
            .send()
            .await?;
        if !clear_response.status().is_success() {
            return Err(anyhow!("Error clearing old file: status {}", clear_response.status()));
        }

        info!("Sending trading pairs...");
        for (batch_idx, chunk) in pairs.chunks(BATCH_SIZE).enumerate() {
            let mut batch_json = String::new();
            for (i, pair) in chunk.iter().enumerate() {

                if let Ok(pair_json) = self.serialize_trading_pair(pair) {
                    if i > 0 {
                        batch_json.push_str(",");
                    }
                    batch_json.push_str(&pair_json);
                }
            }

            if batch_json.is_empty() {
                continue;
            }

            let response = client.post(&format!("{}/append_trading", base_url))
                .header("Content-Type", "application/json")
                .body(batch_json.clone()) // Клонуємо для логування або повторних спроб
                .send()
                .await?;

            if !response.status().is_success() {

                return Err(anyhow!("Error sending trading pairs: status {}", response.status()));
            }
            info!("Sent batch {} of trading pairs", batch_idx + 1);
        }

        info!("Starting synthetic pairs section...");
        let start_synthetic_response = client.post(&format!("{}/start_synthetic", base_url))
            .send()
            .await?;
        if !start_synthetic_response.status().is_success() {
            return Err(anyhow!("Error starting synthetic pairs: status {}", start_synthetic_response.status()));
        }

        info!("Sending synthetic pairs...");
        for (batch_idx, chunk) in synthetic_pairs.chunks(BATCH_SIZE).enumerate() {
            let mut batch_json = String::new();
            for (i, pair) in chunk.iter().enumerate() {
                if let Ok(pair_json) = self.serialize_synthetic_pair(pair) {
                    if i > 0 {
                        batch_json.push_str(",");
                    }
                    batch_json.push_str(&pair_json);
                }
            }

            if batch_json.is_empty() {
                continue;
            }

            let response = client.post(&format!("{}/append_synthetic", base_url))
                .header("Content-Type", "application/json")
                .body(batch_json.clone())
                .send()
                .await?;

            if !response.status().is_success() {

                return Err(anyhow!("Error sending synthetic pairs: status {}", response.status()));
            }
            info!("Sent batch {} of synthetic pairs", batch_idx + 1);
        }

        info!("Finalizing data...");
        let finalize_response = client.post(&format!("{}/finalize", base_url))
            .send()
            .await?;
        if !finalize_response.status().is_success() {
            return Err(anyhow!("Error finalizing data: status {}", finalize_response.status()));
        }

        info!("Data successfully sent to HTTP server");
        Ok(())
    }
    async fn update_pair(&mut self,pairs:&mut Vec<TradingPair>,synthetic_pairs: &mut Vec<SyntheticPairFullData>) ->Result<()>{//%


        for i in 0..pairs.len() {
            self.storage.save_trading_instrument(&self.config,&mut pairs[i],self.logs_id).await?;
        }

        for i in 0..synthetic_pairs.len() {
            self.storage.save_synthetic_trading_pair(&self.config,&mut synthetic_pairs[i], self.logs_id).await?;
        }

        self.send_pairs_to_http_server(&pairs,&synthetic_pairs).await?;
        Ok(())
    }
    pub async fn is_stationarity_time(&mut self) -> Result<bool> {//%
        self.config.update_configuration(&self.storage).await?;

        let mut res=self.storage.check_for_new_date_going().await?;
        if self.current_day==MAX_DAYS{
            res=false;
        }
        Ok(res)
    }
    async fn add_requested_regular_pairs(&self, pairs: &mut Vec<TradingPair>, config_id: i32) -> Result<()> {
        let requested_pairs = self.storage.get_requested_regular_pairs(config_id).await?;

        let existing_pairs: std::collections::HashSet<String> = pairs.iter().map(|p| p.name.clone()).collect();

        for req_pair in requested_pairs {
            if !existing_pairs.contains(&req_pair.name) {
                info!("Додаємо запитану звичайну пару: {}", req_pair.name);
                let mut new_pair = TradingPair {
                    id: req_pair.id,
                    name: req_pair.name.clone(),
                    pair_data_map: std::collections::HashMap::new(),
                    is_request_pair:Some(false)
                };
                new_pair.init_pair_data(config_id);
                pairs.push(new_pair);
            }else{
                pairs.iter_mut().find(|pair| pair.name==req_pair.name)
                    .ok_or(anyhow!("Error find pair "))?.is_request_pair=Some(true);
            }
        }

        Ok(())
    }

    async fn add_requested_synthetic_pairs(&self, synthetic_pairs: &mut Vec<SyntheticPairFullData>, config_id: i32) -> Result<()> {
        let requested_pairs = self.storage.get_requested_synthetic_pairs(config_id).await?;

        let existing_synthetic_pairs: std::collections::HashSet<String> = synthetic_pairs.iter().map(|p| p.name.clone()).collect();

        for req_pair in requested_pairs {
            if !existing_synthetic_pairs.contains(&req_pair.name) {
                info!("Додаємо запитану синтетичну пару: {}", req_pair.name);
                let parts: Vec<&str> = req_pair.name.split('/').collect();

                let mut new_pair = SyntheticPairFullData {
                    id: req_pair.id,
                    name: req_pair.name.clone(),
                    is_request_pair:Some(false),
                    first_pair: BaseAsset {
                        id: -1,
                        name: parts.get(0).unwrap_or(&"").to_string(),
                    },
                    second_pair: BaseAsset {
                        id: -1,
                        name: parts.get(1).unwrap_or(&"").to_string(),
                    },
                    synthetic_data: std::collections::HashMap::new(),
                };

                new_pair.init_pair_data(config_id);
                synthetic_pairs.push(new_pair);
            }else{
                synthetic_pairs.iter_mut().find(|pair| pair.name==req_pair.name)
                    .ok_or(anyhow!("Error find syn_pair "))?.is_request_pair=Some(true);
            }
        }

        Ok(())
    }
    fn trim_data_to_last_10_elements(&self, pairs: &mut Vec<TradingPair>, synthetic_pairs: &mut Vec<SyntheticPairFullData>) -> Result<()> {
        info!("Обрізаю дані до останніх 10 елементів");

        for pair in pairs.iter_mut() {
            for (_, timeframes) in pair.pair_data_map.iter_mut() {
                for timeframe_data in timeframes.iter_mut() {
                    if timeframe_data.candles_h4.len() > 10 {
                        let len = timeframe_data.candles_h4.len();
                        timeframe_data.candles_h4 = timeframe_data.candles_h4.split_off(len - 10);
                    }
                    if timeframe_data.z_score_h4.len() > 10 {
                        let len = timeframe_data.z_score_h4.len();
                        timeframe_data.z_score_h4 = timeframe_data.z_score_h4.split_off(len - 10);
                    }
                }
            }
        }

        for syn_pair in synthetic_pairs.iter_mut() {
            for (_, timeframes) in syn_pair.synthetic_data.iter_mut() {
                for timeframe_data in timeframes.iter_mut() {
                    if timeframe_data.candles_h4.len() > 10 {
                        let len = timeframe_data.candles_h4.len();
                        timeframe_data.candles_h4 = timeframe_data.candles_h4.split_off(len - 10);
                    }
                    if timeframe_data.z_score_h4.len() > 10 {
                        let len = timeframe_data.z_score_h4.len();
                        timeframe_data.z_score_h4 = timeframe_data.z_score_h4.split_off(len - 10);
                    }
                }
            }
        }

        info!("Дані успішно обрізано до останніх 10 елементів");
        Ok(())
    }

    pub async fn run_stationarity(&mut self)->Result<()> {//%
        info!("run stationarity");
        self.storage.clear_analysis_data().await?;
        self.current_day+=1;

        self.config.update_configuration(&self.storage).await?;
        let last_configuration_id = self.config.get_last_config_id()?;
        let last_configuration = self.config.get(last_configuration_id)?;
        let log_id=self.storage.get_logs_id_by_date(self.last_date_bypass).await?;
        info!("LOG ID: {}",log_id);

        let mut pairs:Vec<TradingPair>=Vec::new();
        self.start_to_stationarity(log_id).await?;
        if log_id!=-1{

            pairs=self.storage.get_pairs_by_log_id(log_id,&last_configuration).await?;
            info!("Take pair from database");
        }else{
            pairs = self.exchange.get_uninitialized_pair(last_configuration.clone()).await?;
            info!("Generate pair");
        }
        self.add_requested_regular_pairs(&mut pairs, last_configuration_id).await?;
        let load_time=self.get_time_now();
        self.exchange.get_candles_for_pairs(&mut pairs, &self.config, load_time,false).await?;
        if log_id!=-1{
            let mut is_all=false;
            while is_all==false {
                is_all=true;
                for i in 0..pairs.len() {
                    if pairs[i].is_request_pair.is_some() {
                        let pair_data = pairs[i].pair_data_map.get(&last_configuration_id).ok_or(anyhow!("error get pair req"))?;
                        if pair_data[0].candles_h4.is_empty() {
                            is_all=false;
                            info!("\n We dont have pair: {} \n",pairs[i].name);
                            self.exchange.get_candles_for_pairs(&mut pairs, &self.config, load_time,false).await?;

                            break;
                        }
                    }
                }

            }
        }
        let config_id = self.config.get_last_config_id()?;
        pairs.retain(|pair| {
            if let Some(timeframes_data) = pair.pair_data_map.get(&config_id) {
                let has_empty_candles = timeframes_data.iter().any(|data|
                    data.candles_h4.is_empty()
                );

                if has_empty_candles {

                    info!("DELETE PAIR: {}", pair.name.as_str());
                    return false;
                }

                true
            } else {
                info!("NO DATA FOR CONFIG {}: {}", config_id, pair.name.as_str());
                false
            }
        });

        let mut synthetic_pairs:Vec<SyntheticPairFullData>=Vec::new();
        if log_id!=-1{
            synthetic_pairs=self.storage.get_synthetic_pairs_by_log_id(log_id,&last_configuration).await?;

        }else{
            synthetic_pairs = generate_synthetic_pair(&pairs, last_configuration.id)?;
        }
        self.add_requested_synthetic_pairs(&mut synthetic_pairs, last_configuration_id).await?;
        {
            let analyzer = self.asset_analyzer.clone();
            let config_id = last_configuration_id;
            let time = self.get_time_now();

            if log_id!=-1 {
                let (p, syn_p) = tokio::task::spawn_blocking(move || {
                    let result = analyzer.calculate_asset_h4(config_id, &mut pairs, &mut synthetic_pairs, time)?;
                    Ok::<(Vec<TradingPair>, Vec<SyntheticPairFullData>), anyhow::Error>((pairs, synthetic_pairs))
                }).await??;
                pairs=p;
                synthetic_pairs=syn_p;
            }else{
                let (p, syn_p) = tokio::task::spawn_blocking(move || {
                        let result = analyzer.calculate_asset_with_dickyfuller(config_id, &mut pairs, &mut synthetic_pairs,last_configuration.percent)?;
                        Ok::<(Vec<TradingPair>, Vec<SyntheticPairFullData>), anyhow::Error>((pairs, synthetic_pairs))
                    }).await??;
                pairs=p;
                synthetic_pairs=syn_p;

            }

        }
        pairs.shrink_to_fit();
        synthetic_pairs.shrink_to_fit();
        self.exchange.get_candles_for_pairs(&mut pairs, &self.config,load_time,true).await?;
        let mut is_all=false;
        while is_all==false {
            is_all=true;
            for i in 0..pairs.len() {
                if pairs[i].is_request_pair.is_some() {
                    let pair_data = pairs[i].pair_data_map.get(&config_id).ok_or(anyhow!("error get pair req"))?;
                    if pair_data[0].candles_minutes.is_empty() {
                        is_all=false;
                        self.exchange.get_candles_for_pairs(&mut pairs, &self.config, self.get_time_now(),true).await?;
                        break;
                    }
                }
            }

        }
        {
            let analyzer = self.asset_analyzer.clone();
            let config_id = last_configuration_id;
            let time = self.get_time_now();

            let (p, syn_p) = tokio::task::spawn_blocking(move || {
                let result = analyzer.calculate_asset_minutes(config_id, &mut pairs, &mut synthetic_pairs, time)?;
                Ok::<(Vec<TradingPair>, Vec<SyntheticPairFullData>), anyhow::Error>((pairs, synthetic_pairs))
            }).await??;
            pairs=p;
            synthetic_pairs=syn_p;
        }

        let pairs_len = pairs.len();
        let syn_len = synthetic_pairs.len();
        self.trim_data_to_last_10_elements(&mut pairs, &mut synthetic_pairs)?;
        self.update_pair(&mut pairs, &mut synthetic_pairs).await?;
        info!("PAIRS: {} SYNTHETIC PAIR:{}", pairs_len, syn_len);
        self.end_to_stationarity(log_id).await?;
        self.last_date_bypass+=chrono::Duration::days(1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::*;
    use crate::storage::database::{ MockDatabaseInterface};
    use crate::exchange::binance::{ MockExchangeInterface};
    use anyhow::{ Result};
    use chrono::{NaiveDateTime};
    use serde_json::{ Value};
    use std::collections::HashMap;

    use mockall::{ predicate::*};



    #[tokio::test]
    async fn test_serialize_trading_pair() -> Result<()> {
        let mut mock_db = MockDatabaseInterface::new();
        mock_db.expect_get_active_strategies().returning(|| {
            let mut map = HashMap::new();
            map.insert(1, TradingStrategy { id: 1, h1: 500, h4: 1000, percent: 90.0 });
            Ok((map, 1))
        });
        mock_db.expect_get_start_info().returning(|| Ok(StartInfo {
            last_logs_id: 1,
            last_configuration_id: 1,
            base_date: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            last_date_bypass: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            current_day: 0,
        }));


        let mock_exchange = MockExchangeInterface::new();
        let mut collector = BinanceCollector::new(mock_db, mock_exchange).await?;

        let mut pair = TradingPair {
            id: 1,
            name: "BTCUSDT".to_string(),
            pair_data_map: HashMap::new(),
            is_request_pair: Some(true),
        };
        pair.init_pair_data(1);
        let h4_data = pair.get_timeframe_data_mut(1, &TimeInterval::h4).unwrap();
        h4_data.stationarity = 95.5;
        h4_data.candles_h4 = vec![
            Candle { price: 100.0, close_time: NaiveDateTime::from_timestamp_opt(1627849200, 0).unwrap(), open_time: NaiveDateTime::from_timestamp_opt(1627849200, 0).unwrap() },
            Candle { price: 101.0, close_time: NaiveDateTime::from_timestamp_opt(1627863600, 0).unwrap(), open_time: NaiveDateTime::from_timestamp_opt(1627863600, 0).unwrap() },
        ];
        h4_data.z_score_h4 = vec![1.0, 1.1];
        h4_data.candles_minutes = vec![
            Candle { price: 100.5, close_time: NaiveDateTime::from_timestamp_opt(1627849200, 0).unwrap(), open_time: NaiveDateTime::from_timestamp_opt(1627849200, 0).unwrap() },
        ];
        h4_data.z_score_minutes = vec![0.5];

        let serialized = collector.serialize_trading_pair(&pair)?;
        let json_value: Value = serde_json::from_str(&serialized)?;

        assert_eq!(json_value["id"], 1);
        assert_eq!(json_value["name"], "BTCUSDT");
        assert_eq!(json_value["stationarity"], 95.5);
        assert_eq!(json_value["configuration_id"], 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_serialize_synthetic_pair() -> Result<()> {
        let mut mock_db = MockDatabaseInterface::new();
        mock_db.expect_get_active_strategies().returning(|| {
            let mut map = HashMap::new();
            map.insert(1, TradingStrategy { id: 1, h1: 500, h4: 1000, percent: 90.0 });
            Ok((map, 1))
        });
        mock_db.expect_get_start_info().returning(|| Ok(StartInfo {
            last_logs_id: 1,
            last_configuration_id: 1,
            base_date: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            last_date_bypass: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            current_day: 0,
        }));
        let mock_exchange = MockExchangeInterface::new();
        let mut collector = BinanceCollector::new(mock_db, mock_exchange).await?;

        let mut syn_pair = SyntheticPairFullData {
            id: 1,
            name: "BTCUSDT/ETHUSDT".to_string(),
            first_pair: BaseAsset { id: 1, name: "BTCUSDT".to_string() },
            second_pair: BaseAsset { id: 2, name: "ETHUSDT".to_string() },
            synthetic_data: HashMap::new(),
            is_request_pair: Some(true),
        };
        syn_pair.init_pair_data(1);
        let h4_data = syn_pair.get_timeframe_data_mut(1, &TimeInterval::h4).unwrap();
        h4_data.stationarity = 96.0;
        h4_data.candles_h4 = vec![
            Candle { price: 1.5, close_time: NaiveDateTime::from_timestamp_opt(1627849200, 0).unwrap(), open_time: NaiveDateTime::from_timestamp_opt(1627849200, 0).unwrap() },
        ];
        h4_data.z_score_h4 = vec![0.8];
        h4_data.candles_minutes = vec![
            Candle { price: 1.6, close_time: NaiveDateTime::from_timestamp_opt(1627849200, 0).unwrap(), open_time: NaiveDateTime::from_timestamp_opt(1627849200, 0).unwrap() },
        ];
        h4_data.z_score_minutes = vec![0.9];

        let serialized = collector.serialize_synthetic_pair(&syn_pair)?;
        let json_value: Value = serde_json::from_str(&serialized)?;

        assert_eq!(json_value["id"], 1);
        assert_eq!(json_value["name"], "BTCUSDT/ETHUSDT");
        assert_eq!(json_value["stationarity"], 96.0);
        assert_eq!(json_value["first_pair"]["name"], "BTCUSDT");
        assert_eq!(json_value["second_pair"]["name"], "ETHUSDT");

        Ok(())
    }

    #[tokio::test]
    async fn test_trim_data_to_last_10_elements() -> Result<()> {
        let mut mock_db = MockDatabaseInterface::new();
        mock_db.expect_get_active_strategies().returning(|| {
            let mut map = HashMap::new();
            map.insert(1, TradingStrategy { id: 1, h1: 500, h4: 1000, percent: 90.0 });
            Ok((map, 1))
        });

        mock_db.expect_get_start_info().returning(|| Ok(StartInfo {
            last_logs_id: 1,
            last_configuration_id: 1,
            base_date: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            last_date_bypass: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            current_day: 0,
        }));
        let mock_exchange = MockExchangeInterface::new();
        let mut collector = BinanceCollector::new(mock_db, mock_exchange).await?;

        let mut pairs = vec![TradingPair {
            id: 1,
            name: "BTCUSDT".to_string(),
            pair_data_map: HashMap::new(),
            is_request_pair: None,
        }];
        pairs[0].init_pair_data(1);
        let h4_data = pairs[0].get_timeframe_data_mut(1, &TimeInterval::h4).unwrap();
        h4_data.candles_h4 = (0..15).map(|i| Candle { price: i as f32, close_time: NaiveDateTime::default(), open_time: NaiveDateTime::default() }).collect();
        h4_data.z_score_h4 = (0..15).map(|i| i as f32).collect();

        let mut syn_pairs = vec![SyntheticPairFullData {
            id: 1,
            name: "BTC/ETH".to_string(),
            first_pair: BaseAsset::default(),
            second_pair: BaseAsset::default(),
            synthetic_data: HashMap::new(),
            is_request_pair: None,
        }];
        syn_pairs[0].init_pair_data(1);
        let syn_h4_data = syn_pairs[0].get_timeframe_data_mut(1, &TimeInterval::h4).unwrap();
        syn_h4_data.candles_h4 = (0..20).map(|i| Candle { price: i as f32, close_time: NaiveDateTime::default(), open_time: NaiveDateTime::default() }).collect();
        syn_h4_data.z_score_h4 = (0..20).map(|i| i as f32).collect();

        collector.trim_data_to_last_10_elements(&mut pairs, &mut syn_pairs)?;

        assert_eq!(pairs[0].get_timeframe_data(1, &TimeInterval::h4).unwrap().candles_h4.len(), 10);
        assert_eq!(pairs[0].get_timeframe_data(1, &TimeInterval::h4).unwrap().z_score_h4.len(), 10);
        assert_eq!(syn_pairs[0].get_timeframe_data(1, &TimeInterval::h4).unwrap().candles_h4.len(), 10);
        assert_eq!(syn_pairs[0].get_timeframe_data(1, &TimeInterval::h4).unwrap().z_score_h4.len(), 10);

        assert_eq!(pairs[0].get_timeframe_data(1, &TimeInterval::h4).unwrap().candles_h4[0].price, 5.0);
        assert_eq!(pairs[0].get_timeframe_data(1, &TimeInterval::h4).unwrap().candles_h4[9].price, 14.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_requested_regular_pairs() -> Result<()> {
        let mut mock_db = MockDatabaseInterface::new();
        mock_db.expect_get_requested_regular_pairs().with(eq(1)).returning(|_| {
            Ok(vec![
                RequestedPair { id: 2, name: "ETHUSDT".to_string(), is_synthetic: false },
                RequestedPair { id: 3, name: "BNBUSDT".to_string(), is_synthetic: false },
            ])
        });
        mock_db.expect_get_active_strategies().returning(|| {
            let mut map = HashMap::new();
            map.insert(1, TradingStrategy { id: 1, h1: 500, h4: 1000, percent: 90.0 });
            Ok((map, 1))
        });
        mock_db.expect_get_start_info().returning(|| Ok(StartInfo {
            last_logs_id: 1,
            last_configuration_id: 1,
            base_date: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            last_date_bypass: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            current_day: 0,
        }));
        let mock_exchange = MockExchangeInterface::new();
        let mut collector = BinanceCollector::new(mock_db, mock_exchange).await?;

        let mut pairs = vec![TradingPair {
            id: 1,
            name: "BTCUSDT".to_string(),
            pair_data_map: HashMap::new(),
            is_request_pair: None,
        }];
        pairs[0].init_pair_data(1);

        collector.add_requested_regular_pairs(&mut pairs, 1).await?;

        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[1].name, "ETHUSDT");
        assert_eq!(pairs[2].name, "BNBUSDT");
        assert!(pairs[1].pair_data_map.contains_key(&1));

        Ok(())
    }

    #[tokio::test]
    async fn test_add_requested_synthetic_pairs() -> Result<()> {
        let mut mock_db = MockDatabaseInterface::new();
        mock_db.expect_get_requested_synthetic_pairs().with(eq(1)).returning(|_| {
            Ok(vec![
                RequestedPair { id: 1, name: "BTCUSDT/ETHUSDT".to_string(), is_synthetic: true },
            ])
        });
        mock_db.expect_get_active_strategies().returning(|| {
            let mut map = HashMap::new();
            map.insert(1, TradingStrategy { id: 1, h1: 500, h4: 1000, percent: 90.0 });
            Ok((map, 1))
        });
        mock_db.expect_get_start_info().returning(|| Ok(StartInfo {
            last_logs_id: 1,
            last_configuration_id: 1,
            base_date: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            last_date_bypass: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            current_day: 0,
        }));
        let mock_exchange = MockExchangeInterface::new();
        let mut collector = BinanceCollector::new(mock_db, mock_exchange).await?;

        let mut syn_pairs = vec![SyntheticPairFullData {
            id: 2,
            name: "BNBUSDT/SOLUSDT".to_string(),
            first_pair: BaseAsset { id: 3, name: "BNBUSDT".to_string() },
            second_pair: BaseAsset { id: 4, name: "SOLUSDT".to_string() },
            synthetic_data: HashMap::new(),
            is_request_pair: None,
        }];
        syn_pairs[0].init_pair_data(1);

        collector.add_requested_synthetic_pairs(&mut syn_pairs, 1).await?;

        assert_eq!(syn_pairs.len(), 2);
        assert_eq!(syn_pairs[1].name, "BTCUSDT/ETHUSDT");
        assert_eq!(syn_pairs[1].first_pair.name, "BTCUSDT");
        assert_eq!(syn_pairs[1].second_pair.name, "ETHUSDT");
        assert!(syn_pairs[1].synthetic_data.contains_key(&1));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_time_now() -> Result<()> {
        let mut mock_db = MockDatabaseInterface::new();
        let last_data_bypass=NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
        mock_db.expect_get_active_strategies().returning(|| {
            let mut map = HashMap::new();
            map.insert(1, TradingStrategy { id: 1, h1: 500, h4: 1000, percent: 90.0 });
            Ok((map, 1))
        });
        mock_db.expect_get_start_info().returning(|| Ok(StartInfo {
            last_logs_id: 1,
            last_configuration_id: 1,
            base_date: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
             last_date_bypass:NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            current_day: 0,
        }));
        let mock_exchange = MockExchangeInterface::new();
        let collector = BinanceCollector::new(mock_db, mock_exchange).await?;

        let time_now = collector.get_time_now();
        assert_eq!(time_now, last_data_bypass);

        Ok(())
    }

    #[tokio::test]
    async fn test_is_stationarity_time() -> Result<()> {
        let mut mock_db = MockDatabaseInterface::new();
        mock_db.expect_get_active_strategies().returning(|| {
            let mut map = HashMap::new();
            map.insert(1, TradingStrategy { id: 1, h1: 500, h4: 1000, percent: 90.0 });
            Ok((map, 1))
        });
        mock_db.expect_get_start_info().returning(|| Ok(StartInfo {
            last_logs_id: 1,
            last_configuration_id: 1,
            base_date: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            last_date_bypass: NaiveDate::from_ymd_opt(2024, 9, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            current_day: 0,
        }));
        mock_db.expect_check_for_new_date_going().returning(|| Ok(true));
        let mock_exchange = MockExchangeInterface::new();
        let mut collector = BinanceCollector::new(mock_db, mock_exchange).await?;
        collector.current_day = 269; // Менше MAX_DAYS

        let is_time = collector.is_stationarity_time().await?;
        assert!(is_time);

        collector.current_day = 270; // Рівно MAX_DAYS
        let is_time_max = collector.is_stationarity_time().await?;
        assert!(!is_time_max);

        Ok(())
    }

   }