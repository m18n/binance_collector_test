use std::collections::HashMap;
use crate::core::types::{ConfigId, TradingStrategy};
use crate::storage::database::{DatabaseInterface, PostgresDataBase};
use anyhow::{anyhow, Result};


pub struct Config{
    configuration_strategy:HashMap<ConfigId, TradingStrategy>
}
impl Config{
    pub async fn new<D:DatabaseInterface>(database:&D) ->Result<Config>{
        let mut config=Config{configuration_strategy:HashMap::new()};
        config.update_configuration(database).await?;
        Ok(config)
    }
    pub fn get(&self,config_id: ConfigId)->Result<TradingStrategy>{
        self.configuration_strategy.get(&config_id).ok_or(anyhow!("didn't find config")).cloned()
    }
    pub fn get_copy(&self)->HashMap<ConfigId,TradingStrategy>{
        self.configuration_strategy.clone()
    }
    pub fn get_last_config_id(&self)->Result<ConfigId>{
        let mut keys:Vec<ConfigId>= self.get_keys();
        keys.sort_by(|a, b| b.cmp(&a));
        if keys.len()==0{
            return Err(anyhow!("Dont have configuration"));
        }
        Ok(keys[0])
    }
    pub fn get_keys(&self)->Vec<ConfigId>{
        let keys:Vec<ConfigId>=self.configuration_strategy.keys().cloned().collect();
        keys
    }

    pub async fn update_configuration<D: DatabaseInterface>(&mut self, database:&D) -> Result<()> {
        let (config, _) = database.get_active_strategies().await?;
        self.configuration_strategy=config;
        Ok(())
    }
}
