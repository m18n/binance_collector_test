use std::time::Instant;
// src/analysis/pairs.rs
use crate::core::types::{SyntheticPairFullData, TradingPair, PairData, TimeInterval};
use anyhow::{anyhow, Result};
use crate::mathematics;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use log::error;
use tracing::info;
use crate::core::config::Config;

use crate::exchange::binance::BinanceExchange;
use crate::logic::convert_to_candles;
use crate::mathematics::{calculate_synthetic_pair_data_h4, calculate_synthetic_pair_data_minutes, calculate_synthetic_pair_data_with_dickeyfuller, calculate_z_score, calculate_z_score_minutes, calculate_z_score_minutes_from_h4, calculate_z_score_ndarray};
#[derive(Clone)]
pub struct AssetAnalyzer {

}
impl AssetAnalyzer{
    pub fn new() -> Self {
        Self {}
    }

    pub fn calculate_asset_minutes(&self, last_config_id:i32, pairs:&mut Vec<TradingPair>, synthetic_pairs:&mut Vec<SyntheticPairFullData>, time_for_candle:NaiveDateTime) ->Result<()>{
        info!(" calculate pairs minutes");

        for pair in &mut *pairs {
            let name = pair.name.clone();

            if let Some(timeframes) = pair.pair_data_map.get_mut(&last_config_id) {
                for another_timeframe in timeframes {
                    let candles_minutes: Vec<f32> = another_timeframe.candles_minutes.iter().map(|candle|
                        candle.price
                    ).collect();
                    let candles_h4: Vec<f32> = another_timeframe.candles_h4.iter().map(|candle|
                        candle.price
                    ).collect();

                    if !candles_minutes.is_empty(){
                        another_timeframe.z_score_minutes=calculate_z_score_minutes_from_h4(candles_h4,candles_minutes,240)?;

                    } else {
                        info!("C_W| candels is empty in pair: {}", name.as_str());
                    }
                }
            } else {
                info!("C_W| No data for config_id {} in pair {}", last_config_id, name);
            }
        }

        pairs.retain(|pair| {
            if let Some(timeframes) = pair.pair_data_map.get(&last_config_id) {
                if timeframes.iter().all(|data| !data.z_score_minutes.is_empty() ) {
                    true
                } else {
                    info!("C_W|delete pair {}", pair.name.as_str());
                    false
                }
            } else {
                info!("C_W|delete pair {} (no data for config_id {})", pair.name.as_str(), last_config_id);
                false
            }
        });

        let mut last_update_time = Instant::now();

        for (i, synthetic_pair) in synthetic_pairs.iter_mut().enumerate() {
            if last_update_time.elapsed() >= std::time::Duration::from_secs(100) {
                last_update_time = Instant::now();
            }

            if i % 1000 == 0 {
                info!("NUM: {}", i);
            }

            let first_pair = pairs.iter().find(|&pair| pair.name == synthetic_pair.first_pair.name);
            let second_pair = pairs.iter().find(|&pair| pair.name == synthetic_pair.second_pair.name);

            if first_pair.is_none() || second_pair.is_none() {
                synthetic_pair.id = -2;
                continue;
            }

            let first_pair = first_pair.unwrap();
            let second_pair = second_pair.unwrap();

            if !synthetic_pair.synthetic_data.contains_key(&last_config_id) {
                synthetic_pair.init_pair_data(last_config_id);
            }

            if let Some(synthetic_timeframes) = synthetic_pair.synthetic_data.get_mut(&last_config_id) {
                for (j, time_interval) in TimeInterval::all().iter().enumerate() {
                    if let (Some(first_timeframes), Some(second_timeframes)) = (
                        first_pair.pair_data_map.get(&last_config_id),
                        second_pair.pair_data_map.get(&last_config_id)
                    ) {
                        if let (Some(first_data), Some(second_data)) = (
                            first_timeframes.iter().find(|data| data.time_interval == *time_interval),
                            second_timeframes.iter().find(|data| data.time_interval == *time_interval)
                        ) {
                            let mut pair_data;


                                pair_data = calculate_synthetic_pair_data_minutes(
                                    &synthetic_timeframes[0],
                                    &first_data.candles_minutes,
                                    &second_data.candles_minutes,
                                    time_interval.clone(),
                                    last_config_id
                                )?;



                            if j < synthetic_timeframes.len() {
                                synthetic_timeframes[j] = pair_data;
                            } else {
                                synthetic_timeframes.push(pair_data);
                            }
                        }
                    }
                }
            }
        }

        synthetic_pairs.retain(|synthetic_pair| {
            if synthetic_pair.id != -2 {
                if let Some(timeframes) = synthetic_pair.synthetic_data.get(&last_config_id) {
                    if timeframes.iter().all(|data| !data.z_score_minutes.is_empty() && !data.z_score_h4.is_empty()) {
                        return true;
                    }
                }
            }
            info!("C_W|delete pair {}", synthetic_pair.name.as_str());
            false
        });

        Ok(())
    }
    pub fn calculate_asset_h4(&self, last_config_id:i32, pairs:&mut Vec<TradingPair>, synthetic_pairs:&mut Vec<SyntheticPairFullData>, time_for_candle:NaiveDateTime) ->Result<()>{
        info!(" calculate pairs h4 ");

        for pair in &mut *pairs {
            let name = pair.name.clone();

            if let Some(timeframes) = pair.pair_data_map.get_mut(&last_config_id) {
                for another_timeframe in timeframes {
                    let candles_h4: Vec<f32> = another_timeframe.candles_h4.iter().map(|candle|
                        candle.price
                    ).collect();

                    if !candles_h4.is_empty(){
                        another_timeframe.z_score_h4=calculate_z_score(&candles_h4,240)?;
                        let test=10;
                    } else {
                        info!("C_W| candels is empty in pair: {}", name.as_str());
                    }
                }
            } else {
                info!("C_W| No data for config_id {} in pair {}", last_config_id, name);
            }
        }

        pairs.retain(|pair| {
            if let Some(timeframes) = pair.pair_data_map.get(&last_config_id) {
                if timeframes.iter().all(|data| !data.z_score_h4.is_empty() ) {
                    true
                } else {
                    info!("C_W| Remove the pairs that are not suitable {}", pair.name.as_str());
                    false
                }
            } else {
                info!("C_W| delete pair {} (no data for config_id {})", pair.name.as_str(), last_config_id);
                false
            }
        });

        let mut last_update_time = Instant::now();

        for (i, synthetic_pair) in synthetic_pairs.iter_mut().enumerate() {
            if last_update_time.elapsed() >= std::time::Duration::from_secs(100) {
                last_update_time = Instant::now();
            }

            if i % 1000 == 0 {
                info!("NUM: {}", i);
            }

            let first_pair = pairs.iter().find(|&pair| pair.name == synthetic_pair.first_pair.name);
            let second_pair = pairs.iter().find(|&pair| pair.name == synthetic_pair.second_pair.name);

            if first_pair.is_none() || second_pair.is_none() {
                synthetic_pair.id = -2;
                continue;
            }

            let first_pair = first_pair.unwrap();
            let second_pair = second_pair.unwrap();

            if !synthetic_pair.synthetic_data.contains_key(&last_config_id) {
                synthetic_pair.init_pair_data(last_config_id);
            }

            if let Some(synthetic_timeframes) = synthetic_pair.synthetic_data.get_mut(&last_config_id) {
                for (j, time_interval) in TimeInterval::all().iter().enumerate() {
                    if let (Some(first_timeframes), Some(second_timeframes)) = (
                        first_pair.pair_data_map.get(&last_config_id),
                        second_pair.pair_data_map.get(&last_config_id)
                    ) {
                        if let (Some(first_data), Some(second_data)) = (
                            first_timeframes.iter().find(|data| data.time_interval == *time_interval),
                            second_timeframes.iter().find(|data| data.time_interval == *time_interval)
                        ) {
                            let mut pair_data;


                            pair_data = calculate_synthetic_pair_data_h4(
                                &first_data.candles_h4,
                                &second_data.candles_h4,
                                synthetic_timeframes[0].stationarity,
                                time_interval.clone(),
                                last_config_id
                            )?;



                            if j < synthetic_timeframes.len() {
                                synthetic_timeframes[j] = pair_data;
                            } else {
                                synthetic_timeframes.push(pair_data);
                            }
                        }
                    }
                }
            }
        }

        synthetic_pairs.retain(|synthetic_pair| {
            if synthetic_pair.id != -2 {
                if let Some(timeframes) = synthetic_pair.synthetic_data.get(&last_config_id) {
                    if timeframes.iter().all(|data| !data.z_score_h4.is_empty()) {
                        return true;
                    }
                }
            }
            info!("C_W| Remove the synthetic pairs that are not suitable {}", synthetic_pair.name.as_str());
            false
        });

        Ok(())
    }
    pub fn calculate_asset_with_dickyfuller(&self, last_config_id:i32, pairs:&mut Vec<TradingPair>, synthetic_pairs:&mut Vec<SyntheticPairFullData>, stationarity:f32) ->Result<()>{
        info!(" calculate pairs with dickyfuller");

        for pair in &mut *pairs {
            let name = pair.name.clone();

            if let Some(timeframes) = pair.pair_data_map.get_mut(&last_config_id) {
                for another_timeframe in timeframes {
                    let candles_h4: Vec<f32> = another_timeframe.candles_h4.iter().map(|candle|
                    candle.price
                    ).collect();

                    if !candles_h4.is_empty() {
                        another_timeframe.z_score_h4=calculate_z_score(&candles_h4,240)?;
                    } else {
                        info!("C_W| candels is empty in pair: {}", name.as_str());
                    }
                }
            } else {
                info!("C_W| No data for config_id {} in pair {}", last_config_id, name);
            }
        }

        pairs.retain(|pair| {
            if let Some(timeframes) = pair.pair_data_map.get(&last_config_id) {
                if timeframes.iter().all(|data| !data.z_score_h4.is_empty()) {
                    true
                } else {
                    info!("C_W| delete pair {}", pair.name.as_str());
                    false
                }
            } else {
                info!("C_W| delete pair {} (no data for config_id {})", pair.name.as_str(), last_config_id);
                false
            }
        });

        let mut last_update_time = Instant::now();

        for (i, synthetic_pair) in synthetic_pairs.iter_mut().enumerate() {
            if last_update_time.elapsed() >= std::time::Duration::from_secs(100) {
                last_update_time = Instant::now(); // Оновлюємо час останнього запуску
            }

            if i % 1000 == 0 {
                info!("NUM: {}", i);
            }

            let first_pair = pairs.iter().find(|&pair| pair.name == synthetic_pair.first_pair.name);
            let second_pair = pairs.iter().find(|&pair| pair.name == synthetic_pair.second_pair.name);

            if first_pair.is_none() || second_pair.is_none() {
                synthetic_pair.id = -2;
                continue;
            }

            let first_pair = first_pair.unwrap();
            let second_pair = second_pair.unwrap();

            if !synthetic_pair.synthetic_data.contains_key(&last_config_id) {
                synthetic_pair.init_pair_data(last_config_id);
            }

            if let Some(synthetic_timeframes) = synthetic_pair.synthetic_data.get_mut(&last_config_id) {
                for (j, time_interval) in TimeInterval::all().iter().enumerate() {
                    if let (Some(first_timeframes), Some(second_timeframes)) = (
                        first_pair.pair_data_map.get(&last_config_id),
                        second_pair.pair_data_map.get(&last_config_id)
                    ) {
                        if let (Some(first_data), Some(second_data)) = (
                            first_timeframes.iter().find(|data| data.time_interval == *time_interval),
                            second_timeframes.iter().find(|data| data.time_interval == *time_interval)
                        ) {
                            let mut pair_data;


                                pair_data = calculate_synthetic_pair_data_with_dickeyfuller(
                                    &first_data.candles_h4,
                                    &second_data.candles_h4,
                                    time_interval.clone(),
                                    last_config_id
                                )?;

                                if pair_data.stationarity<stationarity&&synthetic_pair.is_request_pair.is_none(){
                                    synthetic_pair.id = -2;
                                    continue;
                                }

                            if j < synthetic_timeframes.len() {
                                synthetic_timeframes[j] = pair_data;
                            } else {
                                synthetic_timeframes.push(pair_data);
                            }
                        }
                    }
                }
            }
        }

        synthetic_pairs.retain(|synthetic_pair| {
            if synthetic_pair.id != -2 {
                if let Some(timeframes) = synthetic_pair.synthetic_data.get(&last_config_id) {
                    if timeframes.iter().all(|data|  !data.z_score_h4.is_empty()) {
                        return true;
                    }
                }
            }
            info!("C_W| Remove the synthetic pairs that are not suitable {}", synthetic_pair.name.as_str());
            false
        });

        Ok(())
    }
}
