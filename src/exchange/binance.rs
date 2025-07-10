use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use binance_sync::api::Binance as SyncBinance;
use binance_sync::futures::model::Symbol;
use binance_async::api::Binance as AsyncBinance;
use binance_sync::futures::general::FuturesGeneral;
use binance_async::rest_model::{KlineSummaries,KlineSummary};
use binance_async::futures::market::FuturesMarket;
use anyhow::{Result, anyhow};
use chrono::{DateTime, NaiveDateTime, Utc, TimeZone, Duration, NaiveDate};
use tokio::task;
use crate::core::types::{TimeInterval, TimeframeConfig, Candle, TradingPair, PairData, TradingStrategy, ConfigId};
use crate::logic::{convert_to_candles, remove_duplicate_full_pairs};
use async_trait::async_trait;
use binance_sync::api::Binance;

use crate::core::types::BaseAsset;
use log::info;
use mockall::automock;
use crate::core::config::Config;
#[automock]
#[async_trait::async_trait]
pub trait ExchangeInterface {

    async fn get_candles_for_pairs(&self,
                                   pairs: &mut Vec<TradingPair>, config:&Config, data_today:NaiveDateTime
                                   ,load_only_minute:bool) -> Result<()>;
    async fn set_time_from_server(&self) -> Result<()>;
    async fn get_uninitialized_pair(&self,last_configuration:TradingStrategy)->Result<Vec<TradingPair>>;
    async fn get_minutes_only(&self,
        para: &str,
        candle_parameter: &TimeframeConfig,
        download_date: NaiveDateTime
    ) -> Result<Vec<KlineSummary>>;
    async fn get_candles_manual(&self,
        para: &str,
        limit: u16,
        candle_parameter: &TimeframeConfig,
        download_date: NaiveDateTime,
        include_minutes: bool
    ) -> Result<(Vec<KlineSummary>,Vec<KlineSummary>)>;
}
pub struct BinanceExchange {
    async_binance:Arc<FuturesMarket>,
    api_key:String,
    secret_key:String
}
impl BinanceExchange {
    pub fn new(api_key:String,secret_key:String) ->Result<Self>{
        let market: FuturesMarket = AsyncBinance::new(
            Some(api_key.clone()),
            Some(secret_key.clone())
        );
        Ok(BinanceExchange{async_binance:Arc::new(market),api_key,secret_key})
    }
}
#[async_trait::async_trait]
impl ExchangeInterface for BinanceExchange {


    async fn get_candles_for_pairs(&self,
                                              pairs: &mut Vec<TradingPair>, config:&Config, mut data_today:NaiveDateTime
                                              ,load_only_minute:bool
    ) -> Result<()> {
        data_today=data_today+chrono::Duration::days(1);
        let mut last_update_time_d = Instant::now();
        let last_config_id = config.get_last_config_id()?;
        let chunks_in_same_time=1;
        struct CandleRequest {
            pair_index: usize,
            config_id: ConfigId,
            timeframe_index: usize,
            parameter: TimeframeConfig,
            name: String,
            limit: u16,
        }

        let mut candle_requests = Vec::new();

        for (pair_index, pair) in pairs.iter().enumerate() {
            let name = pair.name.clone();

            for (config_id, timeframes) in &pair.pair_data_map {
                match config.get(*config_id) {
                    Ok(config_data) => {
                        for (timeframe_index, another_timeframe) in timeframes.iter().enumerate() {
                            let h4_is_empty = another_timeframe.candles_h4.is_empty();
                            let minutes_is_empty = another_timeframe.candles_minutes.is_empty();

                            if load_only_minute==false && !h4_is_empty {
                                continue;
                            }

                            if load_only_minute==true && !minutes_is_empty {
                                continue;
                            }

                            let parameter = config_data.to_timeframe_config(&another_timeframe.time_interval)?;

                            let limit=parameter.limit;
                            candle_requests.push(CandleRequest {
                                pair_index,
                                config_id:*config_id,
                                timeframe_index,
                                parameter,
                                name: name.clone(),
                                limit,
                            });
                        }
                    },
                    Err(e) => return Err(e),
                }
            }
        }

        if candle_requests.is_empty() {
            tracing::info!("No empty candles found, skipping data loading");
            return Ok(());
        }

        for chunk in candle_requests.chunks(chunks_in_same_time) {
            let futures = chunk.iter().map(|request| {
                let binance_clone = self.async_binance.clone();
                let name = request.name.clone();
                let parameter = request.parameter.clone();
                let limit = request.limit;
                let data_today = data_today;
                let load_minutes=load_only_minute;
                tracing::info!("GET CANDLES: {}", name);
                async move {
                    let result = if load_minutes {
                        self.get_minutes_only(
                            name.as_str(),
                            &parameter,
                            data_today
                        ).await.map(|minutes| (Vec::new(), minutes))
                    } else {
                        self.get_candles_manual(
                            name.as_str(),
                            limit,
                            &parameter,
                            data_today,
                            false
                        ).await
                    };

                    (result, request.pair_index, request.config_id, request.timeframe_index, name)
                }
            });

            let results = futures::future::join_all(futures).await;
            for (result, pair_index, config_id, timeframe_index, name) in results {
                match result {
                    Ok((candles_h4, candles_minutes)) => {
                        tracing::info!("SUCCESSFUL: {}", name);
                        let arr_h4 = convert_to_candles(&candles_h4);
                        let arr_minutes = convert_to_candles(&candles_minutes);
                        if let Some(timeframe) = pairs[pair_index]
                            .pair_data_map
                            .get_mut(&config_id)
                            .and_then(|timeframes| timeframes.get_mut(timeframe_index))
                        {
                            if !arr_h4.is_empty() && timeframe.candles_h4.is_empty() {
                                timeframe.candles_h4 = arr_h4;
                            }
                            if !arr_minutes.is_empty() && timeframe.candles_minutes.is_empty() {
                                timeframe.candles_minutes = arr_minutes;
                            }
                        }
                    },
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to get candles for {}: {:?}", name, e));
                    }
                }
            }
        }

        tracing::info!("DONE");
        Ok(())
    }
     async fn set_time_from_server(&self) -> Result<()> {
         let api_key=self.api_key.clone();
         let secret_key=self.secret_key.clone();
         task::spawn_blocking(|| -> Result<()> {
             let general: FuturesGeneral = SyncBinance::new( Some(api_key),
                                                             Some(secret_key));
             match general.get_server_time() {
                 Ok(response) => {
                     let server_time = Utc.timestamp_millis(response.server_time as i64);
                     tracing::info!("Time server: {}", server_time);
                     let now = Utc::now();
                     tracing::info!("Time now UTC: {}", now);
                     Ok(())
                 },
                 Err(e) => {
                     Err(anyhow!("Error server: {}", e))
                 }
             }
         }).await??;
         Ok(())
     }
    async fn get_uninitialized_pair(&self,last_configuration:TradingStrategy)->Result<Vec<TradingPair>>{
        // First, initialize with the specific pairs
        // let mut pairs: Vec<TradingPair> = vec![
        //     "1000000BOBUSDT","1000000MOGUSDT","1000BONKUSDT","1000BTTCUSDT","1000CATUSDT","1000CHEEMSUSDT","1000FLOKIUSDT","1000LUNCUSDT","1000PEPEUSDT","1000RATSUSDT","1000SATSUSDT","1000SHIBUSDT","1000WHYUSDT","1000XECUSDT","1000XUSDT","1INCHUSDT","1MBABYDOGEUSDT","AAVEUSDT","ACEUSDT","ACHUSDT","ACTUSDT","ACXUSDT","ADAUSDT","AERGOUSDT","AEROUSDT","AEVOUSDT","AGIXUSDT","AGLDUSDT","AGTUSDT","AI16ZUSDT","AIOTUSDT","AIUSDT","AIXBTUSDT","AKROUSDT","AKTUSDT","ALCHUSDT","ALGOUSDT","ALICEUSDT","ALPACAUSDT","ALPHAUSDT","ALPINEUSDT","ALTUSDT","AMBUSDT","ANCUSDT","ANIMEUSDT","ANKRUSDT","ANTUSDT","APEUSDT","API3USDT","APTUSDT","ARBUSDT","ARCUSDT","ARKMUSDT","ARKUSDT","ARPAUSDT","ARUSDT","ASRUSDT","ASTRUSDT","ATAUSDT","ATHUSDT","ATOMUSDT","AUCTIONUSDT","AUDIOUSDT","AUSDT","AVAAIUSDT","AVAUSDT","AVAXUSDT","AWEUSDT","AXLUSDT","AXSUSDT","B2USDT","B3USDT","BABYUSDT","BADGERUSDT","BAKEUSDT","BALUSDT","BANANAS31USDT","BANANAUSDT","BANDUSDT","BANKUSDT","BANUSDT","BATUSDT","BBUSDT","BCHUSDT","BDXNUSDT","BEAMXUSDT","BELUSDT","BERAUSDT","BICOUSDT","BIDUSDT","BIGTIMEUSDT","BIOUSDT","BLUEBIRDUSDT","BLURUSDT","BLZUSDT","BMTUSDT","BNBUSDT","BNTUSDT","BNXUSDT","BOMEUSDT","BONDUSDT","BRETTUSDT","BROCCOLI714USDT","BROCCOLIF3BUSDT","BRUSDT","BSVUSDT","BSWUSDT","BTCDOMUSDT","BTCSTUSDT","BTSUSDT","BTTUSDT","BUSDT","BZRXUSDT","C98USDT","CAKEUSDT","CATIUSDT","CELOUSDT","CELRUSDT","CETUSUSDT","CFXUSDT","CGPTUSDT","CHESSUSDT","CHILLGUYUSDT","CHRUSDT","CHZUSDT","CKBUSDT","COCOSUSDT","COMBOUSDT","COMPUSDT","COOKIEUSDT","COSUSDT","COTIUSDT","COWUSDT","CRVUSDT","CTKUSDT","CTSIUSDT","CVCUSDT","CVXUSDT","CYBERUSDT","DARUSDT","DASHUSDT","DEEPUSDT","DEFIUSDT","DEGENUSDT","DEGOUSDT","DENTUSDT","DEXEUSDT","DFUSDT","DGBUSDT","DIAUSDT","DMCUSDT","DODOUSDT","DODOXUSDT","DOGEUSDT","DOGSUSDT","DOLOUSDT","DOODUSDT","DOTECOUSDT","DOTUSDT","DRIFTUSDT","DUSDT","DUSKUSDT","DYDXUSDT","DYMUSDT","EDUUSDT","EGLDUSDT","EIGENUSDT","ENAUSDT","ENJUSDT","ENSUSDT","EOSUSDT","EPICUSDT","EPTUSDT","ETCUSDT","ETHFIUSDT","ETHUSDT","ETHWUSDT","FARTCOINUSDT","FETUSDT","FHEUSDT","FIDAUSDT","FILUSDT","FIOUSDT","FISUSDT","FLMUSDT","FLOWUSDT","FLUXUSDT","FOOTBALLUSDT","FORMUSDT","FORTHUSDT","FRONTUSDT","FTMUSDT","FTTUSDT","FUNUSDT","FUSDT","FXSUSDT","GALAUSDT","GALUSDT","GASUSDT","GHSTUSDT","GLMRUSDT","GLMUSDT","GMTUSDT","GMXUSDT","GOATUSDT","GPSUSDT","GRASSUSDT","GRIFFAINUSDT","GRTUSDT","GTCUSDT","GUNUSDT","GUSDT","HAEDALUSDT","HBARUSDT","HEIUSDT",
        //     "HFTUSDT","HIFIUSDT","HIGHUSDT","HIPPOUSDT","HIVEUSDT","HMSTRUSDT","HNTUSDT","HOMEUSDT","HOOKUSDT","HOTUSDT","HUMAUSDT","HUSDT","HYPERUSDT","HYPEUSDT","ICNTUSDT","ICPUSDT","ICXUSDT","IDEXUSDT","IDUSDT","ILVUSDT","IMXUSDT","INITUSDT","INJUSDT","IOSTUSDT","IOTAUSDT","IOTXUSDT","IOUSDT",
        //     "IPUSDT","JASMYUSDT","JELLYJELLYUSDT","JOEUSDT","JSTUSDT","JTOUSDT","JUPUSDT","KAIAUSDT","KAITOUSDT","KASUSDT","KAVAUSDT","KDAUSDT","KEEPUSDT","KERNELUSDT","KEYUSDT","KLAYUSDT","KMNOUSDT","KNCUSDT","KOMAUSDT","KSMUSDT","LAUSDT","LAYERUSDT","LDOUSDT","LENDUSDT","LEVERUSDT","LINAUSDT","LINKUSDT","LISTAUSDT","LITUSDT",
        //     "LOKAUSDT","LOOMUSDT","LPTUSDT","LQTYUSDT","LRCUSDT","LSKUSDT","LTCUSDT","LUMIAUSDT","LUNA2USDT","LUNAUSDT","MAGICUSDT","MANAUSDT","MANTAUSDT","MASKUSDT","MATICUSDT","MAVIAUSDT","MAVUSDT","MBLUSDT","MBOXUSDT",
        //     "MDTUSDT","MELANIAUSDT","MEMEFIUSDT","MEMEUSDT","MERLUSDT","METISUSDT","MEUSDT","MEWUSDT","MILKUSDT","MINAUSDT","MKRUSDT","MLNUSDT","MOCAUSDT",
        //     "MOODENGUSDT","MORPHOUSDT","MOVEUSDT","MOVRUSDT","MTLUSDT","MUBARAKUSDT","MYROUSDT","MYXUSDT","NEARUSDT","NEIROETHUSDT","NEIROUSDT","NEOUSDT","NEWTUSDT","NFPUSDT","NILUSDT","NKNUSDT","NMRUSDT","NOTUSDT","NTRNUSDT","NULSUSDT","NUUSDT","NXPCUSDT","OBOLUSDT","OCEANUSDT","OGNUSDT","OGUSDT","OLUSDT","OMGUSDT","OMNIUSDT","OMUSDT","ONDOUSDT","ONEUSDT","ONGUSDT","ONTUSDT","OPUSDT","ORBSUSDT","ORCAUSDT","ORDIUSDT","OXTUSDT","PARTIUSDT","PAXGUSDT","PENDLEUSDT","PENGUUSDT","PEOPLEUSDT","PERPUSDT","PHAUSDT","PHBUSDT","PIPPINUSDT","PIXELUSDT","PLUMEUSDT","PNUTUSDT","POLUSDT","POLYXUSDT","PONKEUSDT","POPCATUSDT","PORT3USDT",
        //     "PORTALUSDT","POWRUSDT","PROMPTUSDT","PROMUSDT","PUFFERUSDT","PUMPBTCUSDT","PUMPUSDT","PUNDIXUSDT","PYTHUSDT","QNTUSDT","QTUMUSDT","QUICKUSDT","RADUSDT","RAREUSDT","RAYSOLUSDT","RAYUSDT","RDNTUSDT","REDUSDT","REEFUSDT","REIUSDT","RENDERUSDT","RENUSDT","RESOLVUSDT","REZUSDT","RIFUSDT","RLCUSDT","RNDRUSDT","RONINUSDT","ROSEUSDT","RPLUSDT","RSRUSDT","RUNEUSDT","RVNUSDT","SAFEUSDT","SAGAUSDT","SAHARAUSDT","SANDUSDT","SANTOSUSDT","SCRTUSDT","SCRUSDT","SCUSDT","SEIUSDT","SFPUSDT","SHELLUSDT","SIGNUSDT","SIRENUSDT","SKATEUSDT","SKLUSDT","SKYAIUSDT","SLERFUSDT","SLPUSDT","SNTUSDT","SNXUSDT","SOLUSDT","SOLVUSDT","SONICUSDT","SOONUSDT","SOPHUSDT","SPELLUSDT","SPKUSDT","SPXUSDT","SQDUSDT","SRMUSDT","SSVUSDT","STEEMUSDT","STGUSDT","STMXUSDT","STORJUSDT","STOUSDT","STPTUSDT","STRAXUSDT","STRKUSDT","STXUSDT","SUIUSDT","SUNUSDT","SUPERUSDT","SUSDT","SUSHIUSDT","SWARMSUSDT","SWELLUSDT","SXPUSDT","SXTUSDT","SYNUSDT","SYRUPUSDT","SYSUSDT","TAIKOUSDT","TAOUSDT","THETAUSDT","THEUSDT","TIAUSDT","TLMUSDT","TNSRUSDT","TOKENUSDT","TOMOUSDT","TONUSDT","TRBUSDT","TROYUSDT","TRUMPUSDT","TRUUSDT","TRXUSDT","TSTUSDT","TURBOUSDT","TUSDT","TUTUSDT","TWTUSDT","UMAUSDT","UNFIUSDT","UNIUSDT","USDCUSDT","USTCUSDT","USUALUSDT","UXLINKUSDT","VANAUSDT","VANRYUSDT","VELODROMEUSDT","VETUSDT","VICUSDT","VIDTUSDT","VINEUSDT","VIRTUALUSDT","VOXELUSDT","VTHOUSDT","VVVUSDT","WALUSDT","WAVESUSDT","WAXPUSDT","WCTUSDT","WIFUSDT","WLDUSDT","WOOUSDT","WUSDT","XAIUSDT","XCNUSDT","XEMUSDT","XLMUSDT","XMRUSDT","XRPUSDT","XTZUSDT","XVGUSDT","XVSUSDT","YFIIUSDT","YFIUSDT","YGGUSDT","ZECUSDT","ZENUSDT","ZEREBROUSDT","ZETAUSDT","ZILUSDT","ZKJUSDT","ZKUSDT","ZROUSDT","ZRXUSDT",
        // ].into_iter().map(|name| {
        //     let mut pair = TradingPair {
        //         id: -1,
        //         name: name.to_string(),
        //         is_request_pair: None,
        //         pair_data_map: HashMap::new()
        //     };
        //     pair.init_pair_data(last_configuration.id);
        //     pair
        // }).collect();
        let mut pairs: Vec<TradingPair>=Vec::new();
        let api_key=self.api_key.clone();
        let secret_key=self.secret_key.clone();
        let api_pairs = task::spawn_blocking(move || -> Result<Vec<TradingPair>>  {
            let general: FuturesGeneral = SyncBinance::new( Some(api_key),
                                                            Some(secret_key));
            match general.exchange_info() {
                Ok(answer) => {
                    let executions = vec![""];
                    let mut api_pairs: Vec<TradingPair> = Vec::new();

                    for i in 0..answer.symbols.len() {
                        let mut add = true;
                        for execution in executions.iter() {
                            if answer.symbols[i].base_asset == *execution || answer.symbols[i].quote_asset != "USDT"|| answer.symbols[i].status != "TRADING" {
                                add = false;
                                break;
                            }
                        }
                        if add == true {
                            let mut pair=TradingPair {
                                id: -1,
                                name: format!("{}{}", answer.symbols[i].base_asset, answer.symbols[i].quote_asset),
                                is_request_pair:None,
                                pair_data_map: HashMap::new()
                            };
                            pair.init_pair_data(last_configuration.id);
                            api_pairs.push(pair);
                        }
                    }

                    Ok(api_pairs)
                }
                Err(e) => {
                    Err(anyhow::anyhow!(e.to_string()))
                },
            }
        }).await??;

        pairs.extend(api_pairs);
        let pairs = remove_duplicate_full_pairs(pairs);

        Ok(pairs)
    }


    async fn get_minutes_only(
        &self,
        para: &str,
        candle_parameter: &TimeframeConfig,
        download_date: NaiveDateTime
    ) -> Result<Vec<KlineSummary>> {
        let mut all_klines_minutes = Vec::new();
        let mut remaining_limit = 1440;
        let default_timestamp = download_date.and_utc().timestamp_millis() as u64 - 1000;
        let mut time_timestamp = default_timestamp;

        while remaining_limit > 0 {
            let limit = remaining_limit.min(1500);

            let klines = match self.async_binance.get_klines(para, "1m", limit as u16, None, time_timestamp).await {
                Ok(k) => k,
                Err(e) => {
                    if let binance_async::errors::Error::BinanceError { response } = &e {
                        if response.code == -1122 && response.msg.contains("Invalid symbol status") {
                            return Ok(Vec::new());
                        }
                    }
                    return Err(anyhow!("Помилка при отриманні хвилинних свічок: {:?}", e));
                }
            };
            let klines_arr = match klines {
                KlineSummaries::AllKlineSummaries(arr) => arr,
                _ => return Err(anyhow!("Не вдалося розпакувати дані хвилинних свічок.")),
            };

            if klines_arr.is_empty() {
                break;
            }
            time_timestamp = (klines_arr[0].open_time - 1000) as u64;
            all_klines_minutes.splice(0..0, klines_arr);
            remaining_limit -= limit;
        }

        Ok(all_klines_minutes)
    }
    async fn get_candles_manual(
        &self,
        para: &str,
        limit: u16,
        candle_parameter: &TimeframeConfig,
        download_date: NaiveDateTime,
        include_minutes: bool
    ) -> Result<(Vec<KlineSummary>,Vec<KlineSummary>)> {
        let total_limit = limit;
        let mut remaining_limit = total_limit;
        let mut all_klines_h4 = Vec::new();
        let mut all_klines_minutes = Vec::new();

        let mut default_timestamp = download_date.and_utc().timestamp_millis() as u64 - 1000;
        let mut time_timestamp = default_timestamp;

        while remaining_limit > 0 {
            let limit = remaining_limit.min(1500);
            let klines =  match self.async_binance.get_klines(para, candle_parameter.name_interval.clone(), limit, None, time_timestamp).await {
                Ok(k) => k,
                Err(e) => {
                    if let binance_async::errors::Error::BinanceError { response } = &e {
                        if response.code == -1122 && response.msg.contains("Invalid symbol status") {
                            return Ok((Vec::new(),Vec::new())); // Повертаємо пустий масив
                        }
                    }
                    return Err(anyhow!("Помилка при отриманні хвилинних свічок: {:?}", e));
                }
            };
            let klines_arr = match klines {
                KlineSummaries::AllKlineSummaries(arr) => arr,
                _ => return Err(anyhow!("Не вдалося розпакувати дані свічок (manual).")),
            };

            if klines_arr.is_empty() {
                break;
            }
            time_timestamp = (klines_arr[0].open_time - 1000) as u64;
            all_klines_h4.splice(0..0, klines_arr);
            remaining_limit -= limit;
        }

        if include_minutes && all_klines_h4.len() >= total_limit as usize {
            remaining_limit = 1440;
            time_timestamp = default_timestamp;

            while remaining_limit > 0 {
                let limit = remaining_limit.min(1500);

                let klines =match self.async_binance.get_klines(para, "1m", limit, None, time_timestamp).await {
                    Ok(k) => k,
                    Err(e) => {

                        if let binance_async::errors::Error::BinanceError { response } = &e {
                            if response.code == -1122 && response.msg.contains("Invalid symbol status") {
                                return Ok((Vec::new(),Vec::new())); // Повертаємо пустий масив
                            }
                        }
                        return Err(anyhow!("Помилка при отриманні хвилинних свічок: {:?}", e));
                    }
                };
                let klines_arr = match klines {
                    KlineSummaries::AllKlineSummaries(arr) => arr,
                    _ => return Err(anyhow!("Не вдалося розпакувати дані свічок (manual).")),
                };

                if klines_arr.is_empty() {
                    break;
                }
                time_timestamp = (klines_arr[0].open_time - 1000) as u64;
                all_klines_minutes.splice(0..0, klines_arr);
                remaining_limit -= limit;
            }
        }

        if all_klines_h4.len() < total_limit as usize {
            return Ok((Vec::new(), Vec::new()));
        }

        Ok((all_klines_h4, all_klines_minutes))
    }

}
