use std::collections::HashMap;
use chrono::NaiveDateTime;
use anyhow::anyhow;
use sqlx::{FromRow};
use anyhow::Result;



#[macro_export]
macro_rules! anyhow_with_trace {
    ($($arg:tt)*) => {{
        let error=anyhow::anyhow!($($arg)*);
        tracing::error!(error = ?error, "ERROR");
        error

    }};
}
#[derive(Debug, FromRow)]
pub struct MarketDataSyncLog {
    id: i32,
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
}
pub struct RequestedPair {
    pub id: i32,
    pub name: String,
    pub is_synthetic: bool
}
pub struct StartInfo{
    pub last_logs_id:i32,
    pub last_configuration_id:i32,
    pub base_date:NaiveDateTime,
    pub last_date_bypass:NaiveDateTime,
    pub current_day:i32
}
#[derive(Clone)]
pub struct TimeframeConfig {
    pub name_interval:String,
    pub limit:u16,
    pub interval_hour:u16,
}
pub type ConfigId=i32;
#[derive(Debug, FromRow, Clone, Default)]
pub struct TradingStrategy {
    pub id:ConfigId,
    pub h1:i32,
    pub h4:i32,
    pub percent:f32
}
impl TradingStrategy {
    pub fn new()->Self{
        Self{id:-1,h1:-1,h4:-1,percent:-1.0}
    }
    pub fn to_timeframe_config(&self,time_interval: &TimeInterval)->Result<TimeframeConfig>{
        let mut res: anyhow::Result<TimeframeConfig> =Err(anyhow!("error timeframe"));
        if *time_interval==TimeInterval::h4{
            res=Ok(TimeframeConfig{
                limit: self.h4 as u16,
                interval_hour:4,
                name_interval:"4h".to_string()
            });
        }
        res
    }
}
#[derive(Debug,sqlx::Type, Clone, Default,PartialEq, Eq, Hash)]
#[sqlx(type_name = "candle_timeframe")]
pub enum TimeInterval {
    #[default]
    h4,
}
impl TimeInterval {
    pub const fn all() -> &'static [TimeInterval] {
        &[

            TimeInterval::h4,

        ]
    }
}


#[derive(Debug, FromRow)]
pub struct TradingInstrument {
    id: i32,
    name: String,
    data_extraction_log_id: i32,
    is_synthetic: bool,
}

#[derive(Debug, FromRow)]
pub struct MarketDataPoint {
    id: i32,
    value: f32,
    date: NaiveDateTime,
    pair_id: i32,
    value_type: String,
}

#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "value_type")]
pub enum ValueType {
    candle,
    zscore,
}



#[derive(Debug,Default,Clone,sqlx::FromRow)]
pub struct Candle {
    pub price:f32,
    pub close_time:NaiveDateTime,
    pub open_time:NaiveDateTime
}
#[derive(Debug,Default,Clone,sqlx::FromRow)]
pub struct BaseAsset {
    pub id:i32,
    pub name: String
}
impl BaseAsset {
    pub fn new()-> BaseAsset {
        BaseAsset {
            id:-1,
            name:String::new()
        }
    }
}
#[derive(Debug,Default,Clone,sqlx::FromRow)]
pub struct PairData {
    pub stationarity:f32,
    pub candles_h4:Vec<Candle>,
    pub z_score_h4:Vec<f32>,
    pub candles_minutes:Vec<Candle>,
    pub z_score_minutes:Vec<f32>,
    pub time_interval: TimeInterval,
    pub configuration_id:i32
}
impl PairData {
    pub fn new(time_frame: TimeInterval,config_id: ConfigId) -> PairData {
        PairData {stationarity:-1.0, candles_minutes:Vec::new(), z_score_minutes:Vec::new(), candles_h4:Vec::new(),z_score_h4:Vec::new(), time_interval:time_frame,configuration_id:config_id}
    }
    pub fn new_array(config_id: ConfigId)->Vec<PairData>{
        let mut pair_data=Vec::new();
        for element in TimeInterval::all(){
            pair_data.push(PairData::new(element.clone(),config_id));
        }
        pair_data
    }
}
#[derive(Debug,Default,Clone,sqlx::FromRow)]
pub struct TradingPair {
    pub id:i32,
    pub name: String,
    pub pair_data_map: HashMap<ConfigId, Vec<PairData>>,
    pub is_request_pair:Option<bool>
}
impl TradingPair {
    pub fn new()-> TradingPair {
        TradingPair {
            id:-1,
            is_request_pair:None,
            name:String::new(),
            pair_data_map: HashMap::new()
        }
    }

    pub fn init_pair_data(&mut self, config_id: ConfigId) {
        let mut timeframes = Vec::new();
        for time_interval in TimeInterval::all() {
            timeframes.push(PairData::new(time_interval.clone(), config_id));
        }
        self.pair_data_map.insert(config_id, timeframes);
    }

    pub fn get_timeframe_data(&self, config_id: ConfigId, time_interval: &TimeInterval) -> Option<&PairData> {
        if let Some(timeframes) = self.pair_data_map.get(&config_id) {
            timeframes.iter().find(|data| &data.time_interval == time_interval)
        } else {
            None
        }
    }

    pub fn get_timeframe_data_mut(&mut self, config_id: ConfigId, time_interval: &TimeInterval) -> Option<&mut PairData> {
        if let Some(timeframes) = self.pair_data_map.get_mut(&config_id) {
            timeframes.iter_mut().find(|data| &data.time_interval == time_interval)
        } else {
            None
        }
    }
}
#[derive(Debug,Default,Clone,sqlx::FromRow)]
pub struct SyntheticPair {
    pub id:i32,
    pub name:String,
    pub first_pair: BaseAsset,
    pub second_pair: BaseAsset,
    pub is_request_pair:Option<bool>
}
#[derive(Debug,Default,Clone,sqlx::FromRow)]
pub struct SyntheticPairFullData {
    pub id:i32,
    pub name:String,
    pub first_pair: BaseAsset,
    pub second_pair: BaseAsset,
    pub synthetic_data:HashMap<ConfigId, Vec<PairData>>,
    pub is_request_pair:Option<bool>

}
impl SyntheticPairFullData {
    pub fn new()-> SyntheticPairFullData {
        SyntheticPairFullData {
            id:-1,
            name:String::new(),
            first_pair: BaseAsset::new(),
            second_pair: BaseAsset::new(),
            synthetic_data:HashMap::new(),
            is_request_pair:None

        }
    }
    pub fn init_pair_data(&mut self, config_id: ConfigId) {
        let mut timeframes = Vec::new();
        for time_interval in TimeInterval::all() {
            timeframes.push(PairData::new(time_interval.clone(), config_id));
        }
        self.synthetic_data.insert(config_id, timeframes);
    }

    pub fn get_timeframe_data(&self, config_id: ConfigId, time_interval: &TimeInterval) -> Option<&PairData> {
        if let Some(timeframes) = self.synthetic_data.get(&config_id) {
            timeframes.iter().find(|data| &data.time_interval == time_interval)
        } else {
            None
        }
    }

    pub fn get_timeframe_data_mut(&mut self, config_id: ConfigId, time_interval: &TimeInterval) -> Option<&mut PairData> {
        if let Some(timeframes) = self.synthetic_data.get_mut(&config_id) {
            timeframes.iter_mut().find(|data| &data.time_interval == time_interval)
        } else {
            None
        }
    }
}
