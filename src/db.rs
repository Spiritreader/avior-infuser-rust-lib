use crate::cfg::Config;
use mongodb::{
    bson::{self, doc, Bson},
    error::Error as MongoError,
    sync::Client as MongoClient,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Client {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<bson::oid::ObjectId>,
    pub name: String,
    pub availability_start: String,
    pub availability_end: String,
    pub maximum_jobs: i32,
    pub priority: i32,
    pub online: bool,
    pub ignore_online: bool,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct Job {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<bson::oid::ObjectId>,
    pub name: String,
    pub path: String,
    pub subtitle: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_parameters: Vec<String>,
    pub assigned_client: AssignedClient,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AssignedClient {
    #[serde(rename = "$ref")]
    pub collection: String,
    #[serde(rename = "$id")]
    pub id: bson::oid::ObjectId,
    #[serde(rename = "$db", default, skip_serializing_if = "String::is_empty")]
    pub db: String,
}

pub fn connect(cfg: &Config) -> Result<MongoClient, MongoError> {
    //let conn_url = format!("mongodb://{}/", cfg.db_url);
    //println!("connecting to {}", cfg.db_url);
    let client = MongoClient::with_uri_str(&cfg.db_url)?;
    Ok(client)
}

pub fn get_clients(mongo_client: &MongoClient, db: &String) -> Result<Vec<Client>, MongoError> {
    let db = mongo_client.database(&db);
    let collection = db.collection("clients");
    let cur = collection.find(doc! {}, None)?;
    let mut clients = Vec::new();
    for result in cur {
        match result {
            Ok(doc) => {
                let client: Client = bson::from_bson(Bson::Document(doc))?;
                clients.push(client);
            }
            Err(e) => eprintln!("error reading clients from db in db::get_clients: {:?}", e),
        }
    }
    Ok(clients)
}

pub fn get_jobs(mongo_client: &MongoClient, db: &String) -> Result<Vec<Job>, MongoError> {
    let mut jobs = Vec::new();
    for result in mongo_client.database(&db).collection("jobs").find(doc! {}, None)? {
        match result {
            Ok(doc) => {
                let job: Job = bson::from_bson(Bson::Document(doc))?;
                jobs.push(job);
            }
            Err(e) => eprintln!("error retrieving jobs list in db::get_jobs: {:?}", e),
        }
    }
    Ok(jobs)
}

pub fn job_exists(mongo_client: &MongoClient, db: &String, job_pathstring: &str) -> Result<bool, MongoError> {
    let filter = doc! { "Path": { "$eq" : job_pathstring } };
    let result = mongo_client.database(&db).collection("jobs").find_one(filter, None)?;
    match result {
        Some(_) => {
            return Ok(true);
        }
        None => {}
    }
    Ok(false)
}

/// Inserts a job into the database and returns the insert id if successful
pub fn insert_job(
    mongo_client: &MongoClient,
    db: &String,
    client: &Client,
    job: &mut Job,
) -> Result<String, MongoError> {
    job.assigned_client = AssignedClient {
        collection: "clients".to_string(),
        db: "".to_string(),
        id: client.id.to_owned().unwrap(),
    };
    let serialized = bson::to_bson(&job)?;
    let document = serialized.as_document().unwrap();
    let result = mongo_client
        .database(&db)
        .collection("jobs")
        .insert_one(document.to_owned(), None)?;
    let insert_id = result.inserted_id.to_string();
    Ok(insert_id)
}

pub fn get_machine_jobcount(mongo_client: &MongoClient, db: &String) -> Result<HashMap<String, i32>, Box<dyn Error>> {
    let query = vec![
        doc! {
           "$addFields":{
              "AssignedClient":{
                 "$arrayElemAt":[
                    {
                       "$objectToArray":"$AssignedClient"
                    },
                    1
                 ]
              }
           }
        },
        doc! {
           "$addFields":{
              "AssignedClient":"$AssignedClient.v"
           }
        },
        doc! {
           "$group":{
              "_id":{
                 "AssignedClient":"$AssignedClient"
              },
              "count":{
                 "$sum":1
              }
           }
        },
    ];
    let mut cur = mongo_client.database(&db).collection("jobs").aggregate(query, None)?;
    let mut job_counts = HashMap::new();
    while let Some(res) = cur.next() {
        let doc = res?;
        let count = doc.get_i32("count")?;
        let oid_bson = doc
            .get_document("_id")?
            .get("AssignedClient")
            .expect("Error aggregating jobs: AssignedClients are required to have an id");
        let oid: bson::oid::ObjectId = bson::from_bson(oid_bson.to_owned())?;
        job_counts.insert(oid.to_string(), count);
    }
    Ok(job_counts)
}
