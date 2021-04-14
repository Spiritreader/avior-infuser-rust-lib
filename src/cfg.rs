use gethostname::gethostname;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::Path;

struct ConfigDeserializeError(String);

impl fmt::Display for ConfigDeserializeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Could not deserialize config: {}", self.0)
    }
}

impl fmt::Debug for ConfigDeserializeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Could not deserialize config: {}", self.0)
    }
}

impl Error for ConfigDeserializeError {}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct Config {
    #[serde(default = "db_url_default")]
    pub db_url: String,
    #[serde(default = "db_name_default")]
    pub db_name: String,
    #[serde(default = "default_client_default")]
    pub default_client: String,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            db_url: "mongodb://localhost:27107".to_string(),
            db_name: "DefaultDB".to_string(),
            default_client: gethostname().into_string().unwrap(),
        }
    }
}

fn db_url_default() -> String {
    Config::default().db_url
}

fn db_name_default() -> String {
    Config::default().db_name
}

fn default_client_default() -> String {
    Config::default().default_client
}

pub fn read(cfg_path: &str) -> Result<Config, Box<dyn Error>> {
    if !Path::new(cfg_path).exists() {
        let cfg_empty = Config::default();
        fs::write(cfg_path, serde_json::to_string_pretty(&cfg_empty)?)?
    }
    let cfg_string = fs::read_to_string(cfg_path)?;
    let cfg: Config = serde_json::from_str(&cfg_string).map_err(|err| ConfigDeserializeError(err.to_string()))?;
    // println!("{:?}", cfg);
    Ok(cfg)
}
