
use std::collections::{HashMap, HashSet};
use anyhow::anyhow;
use binance_async::rest_model::{ KlineSummary};
use chrono::{NaiveDateTime};
use binance_sync::api::*;
use crate::core::types::{SyntheticPairFullData, TradingPair, Candle, BaseAsset};
use anyhow::Result;
pub fn convert_to_candles(array:&Vec<KlineSummary>) ->Vec<Candle> {
    let candles: Vec<Candle> = array.iter().map(|candle|

    Candle {price:candle.close as f32,
        close_time:NaiveDateTime::from_timestamp(candle.close_time / 1000, (candle.close_time % 1000 * 1_000_000) as u32),
        open_time:NaiveDateTime::from_timestamp(candle.open_time / 1000, (candle.open_time % 1000 * 1_000_000) as u32)}

    ).collect();

    candles
}

pub fn remove_duplicate_full_pairs(vec: Vec<TradingPair>) -> Vec<TradingPair> {
    let mut seen = HashSet::new();
    vec.into_iter()
        .filter(|item| seen.insert(item.name.clone()))
        .collect()
}


pub fn generate_synthetic_pair(pairs: &Vec<TradingPair>, last_configuration_id:i32) -> Result<Vec<SyntheticPairFullData>> {
    let mut synthetic_pairs = Vec::new();
    let n = pairs.len();
    let mut num=0;
    for i in 0..n {
        if  !pairs[i].is_request_pair.is_none() && pairs[i].is_request_pair.ok_or(anyhow!("didnt find request pair"))?==false{
            continue;
        }
        for j in (i + 1)..n {

            let mut syn_pair=SyntheticPairFullData {
                id: -1,
                name: format!("{}/{}", pairs[i].name.clone(), pairs[j].name.clone()),
                first_pair: BaseAsset { id: pairs[i].id, name: pairs[i].name.clone() },
                second_pair: BaseAsset { id: pairs[j].id, name: pairs[j].name.clone() },
                is_request_pair:None,
                synthetic_data: HashMap::new()
            };
            syn_pair.init_pair_data(last_configuration_id);
            synthetic_pairs.push(syn_pair);

            num+=1;
        }
    }
    Ok(synthetic_pairs)
}

