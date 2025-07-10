use pyo3::prelude::*;
use pyo3::types::{ PyModule};
use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use numpy::{ PyArray1};
use crate::core::types::{ConfigId, PairData, Candle, TimeInterval};
use ndarray::{Array1, s};


struct AdFullerUtils {
    statsmodels_tsa_stattools: Py<PyModule>,
    adfuller_func: Py<PyAny>,
}

impl AdFullerUtils {
    fn new(py: Python) -> PyResult<Self> {
        let statsmodels_module: Bound<'_, PyModule> = PyModule::import_bound(py, "statsmodels.tsa.stattools")?;
        let adfuller_attr: Bound<'_, PyAny> = statsmodels_module.getattr("adfuller")?;
        Ok(AdFullerUtils {
            statsmodels_tsa_stattools: statsmodels_module.unbind(),
            adfuller_func: adfuller_attr.unbind(),
        })
    }
}

static ADFULLER_UTILS: Lazy<PyResult<AdFullerUtils>> = Lazy::new(|| {
    Python::with_gil(|py| {
        AdFullerUtils::new(py)
    })
});

pub fn dickey_fuller_test(series: Vec<f32>) -> Result<f32> {
    let p=Python::with_gil(|py| {
        let utils_result = ADFULLER_UTILS.as_ref();

        // Обробка помилки ініціалізації Lazy, якщо вона сталася
        let utils = match utils_result {
            Ok(u) => u,
            Err(e) => return Err(e.clone_ref(py)), // Клонуємо помилку для поточного GIL
        };


        let series_py = PyArray1::from_vec_bound(py, series);

        let args = (series_py,);
        let result = utils.adfuller_func.bind(py).call1(args)?;

        let p_value: f32 = result.get_item(1)?.extract()?;
        Ok(p_value)
    })?;
    Ok(p)
}
pub fn calculate_z_score_ndarray(data: &[f32], period: usize) -> Result<Vec<f32>> {
    let arr_f64: Array1<f64> = Array1::from(data.iter().map(|&x| x as f64).collect::<Vec<_>>());

    let mut zscores = Vec::with_capacity(data.len());
    for i in 0..data.len() {
        if i >= period - 1 {
            let window = arr_f64.slice(s![i - (period - 1)..=i]);
            let mean = window.mean().ok_or(anyhow!("Error with mean"))?;
            let std_dev = window.std(0.0); // ddof = 0 для population std dev

            let zscore_f64 = if std_dev != 0.0 {
                (arr_f64[i] - mean) / std_dev
            } else {
                0.0
            };

            zscores.push(zscore_f64 as f32);
        } else {
            zscores.push(0.0);
        }
    }
    Ok(zscores)
}
pub fn calculate_z_score_minutes_from_h4(mut data_h4: Vec<f32>, data_minutes: Vec<f32>, period: usize) -> Result<Vec<f32>> {
    let mut z_score_minutes: Vec<f32> = Vec::new();
    z_score_minutes.resize(data_minutes.len(), 0.0);
    let mut index = 7;
    let h4_len = data_h4.len() - 1;

    // Convert to f64 for better precision

    for i in 0..z_score_minutes.len() {
        if i % 240 == 0 || i == z_score_minutes.len() - 1 {
            index -= 1;
        }

        let last_value = data_h4[h4_len - index];
        data_h4[h4_len - index] = data_minutes[i];

        z_score_minutes[i] = calculate_z_score_minutes(&data_h4[..=(h4_len - index)], period)?;

        data_h4[h4_len - index] = last_value;
    }

    Ok(z_score_minutes)
}
pub fn calculate_z_score_minutes(data_h4: &[f32], period: usize) -> Result<f32> {
    if data_h4.len() < period {
        return Err(anyhow!("the array length is less than the period"));
    }

    // Convert from f32 to f64 for better precision
    let arr_f64: Array1<f64> = Array1::from(data_h4.iter().map(|&x| x as f64).collect::<Vec<_>>());

    // Take the last 'period' elements for calculation
    let window = arr_f64.slice(s![arr_f64.len() - period..]);
    let mean = window.mean().ok_or(anyhow!("Error with mean"))?;
    let std_dev = window.std(0.0); // Population std dev

    let last_value = arr_f64[arr_f64.len() - 1];

    // Calculate Z-score
    let zscore_f64 = if std_dev != 0.0 {
        (last_value - mean) / std_dev
    } else {
        0.0
    };

    // Convert back to f32
    Ok(zscore_f64 as f32)
}

pub fn calculate_z_score(data: &[f32], period: usize) -> Result<Vec<f32>> {
    if data.len() < period {
        return Err(anyhow!("the array length is less than the period"));
    }

    // Convert input data from f32 to f64
    let arr_f64: Array1<f64> = Array1::from(data.iter().map(|&x| x as f64).collect::<Vec<_>>());

    let mut zscores = Vec::with_capacity(data.len());
    for i in 0..data.len() {
        if i >= period - 1 {
            let window = arr_f64.slice(s![i - (period - 1)..=i]);
            let mean = window.mean().ok_or(anyhow!("Error with mean"))?;
            let std_dev = window.std(0.0); // Population std dev

            // Calculate Z-score in f64
            let zscore_f64 = if std_dev != 0.0 {
                (arr_f64[i] - mean) / std_dev
            } else {
                0.0
            };

            // Convert result back to f32
            zscores.push(zscore_f64 as f32);
        } else {
            zscores.push(0.0);
        }
    }

    Ok(zscores)
}
pub fn calculate_synthetic_pair_data_h4(first_market_h4:&[Candle], second_market_h4:&[Candle],stationarity:f32, time_frame: TimeInterval, config_id: ConfigId) -> Result<PairData> {

    let mut result_h4:Vec<f32>=Vec::with_capacity(first_market_h4.len());
    for i in 0..first_market_h4.len(){
        result_h4.push(first_market_h4[i].price / second_market_h4[i].price);
    }
    let z_score_h4 = calculate_z_score(&result_h4,240)?;
    let mut candles_h4: Vec<Candle> = Vec::with_capacity(result_h4.len());
    for i in 0..result_h4.len(){

        candles_h4.push(Candle {price:result_h4[i], close_time:first_market_h4[i].close_time,open_time:first_market_h4[i].open_time });
    }
    Ok(PairData {stationarity:stationarity, candles_h4,z_score_h4, candles_minutes:Vec::new(),z_score_minutes:Vec::new(), time_interval:time_frame,configuration_id:config_id})
}
pub fn calculate_synthetic_pair_data_with_dickeyfuller(first_market_h4:&[Candle], second_market_h4:&[Candle], time_frame: TimeInterval, config_id: ConfigId) -> Result<PairData> {


    let mut result_h4:Vec<f32>=Vec::with_capacity(first_market_h4.len());

    for i in 0..first_market_h4.len(){
        result_h4.push(first_market_h4[i].price / second_market_h4[i].price);
    }
    let dickey=dickey_fuller_test(result_h4.clone())?;
    let z_score_h4 = calculate_z_score(&result_h4,240)?;
    let mut candles_h4: Vec<Candle> = Vec::with_capacity(result_h4.len());
    for i in 0..result_h4.len(){

        candles_h4.push(Candle {price:result_h4[i], close_time:first_market_h4[i].close_time,open_time:first_market_h4[i].open_time });
    }
    Ok(PairData {stationarity:(1.0 - dickey) * 100.0, candles_h4,z_score_h4, candles_minutes:Vec::new(),z_score_minutes:Vec::new(), time_interval:time_frame,configuration_id:config_id})

}
pub fn calculate_synthetic_pair_data_minutes(pair_data:&PairData,first_market_minutes:&[Candle], second_market_minutes:&[Candle], time_frame: TimeInterval, config_id: ConfigId) -> Result<PairData> {
    if first_market_minutes.len()==0|| second_market_minutes.len()==0{
        return Err(anyhow!("all empty all candels"));
    }
    let mut candle_h4:Vec<f32>=pair_data.candles_h4.iter().map(|candle|candle.price).collect();
    let mut result_minutes:Vec<f32>=Vec::with_capacity(first_market_minutes.len());
    for i in 0..first_market_minutes.len(){
        result_minutes.push(first_market_minutes[i].price / second_market_minutes[i].price);
    }
    let z_score_minutes = calculate_z_score_minutes_from_h4(candle_h4.clone(), result_minutes.clone(), 240)?;
    let mut candles_minutes: Vec<Candle> = Vec::with_capacity(result_minutes.len());
    for i in 0..result_minutes.len(){

        candles_minutes.push(Candle {price:result_minutes[i], close_time: first_market_minutes[i].close_time,open_time: first_market_minutes[i].open_time });
    }
    Ok(PairData {stationarity:pair_data.stationarity, candles_h4:pair_data.candles_h4.clone(),z_score_h4:pair_data.z_score_h4.clone(), candles_minutes,z_score_minutes, time_interval:time_frame,configuration_id:config_id})
}
