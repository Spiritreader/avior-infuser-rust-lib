pub mod db;
pub mod log;

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt,
};
use serde::{Deserialize, Serialize, Serializer};

pub struct InfuserError {
    pub message: String,
}

impl fmt::Debug for InfuserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Display for InfuserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for InfuserError {}

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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct JobJson {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<bson::oid::ObjectId>,
    pub name: String,
    pub path: String,
    pub subtitle: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_parameters: Vec<String>,
    pub assigned_client: AssignedClientJson,
}

impl From<Job> for JobJson {
    fn from(job: Job) -> Self {
        JobJson {
            id: job.id,
            assigned_client: job.assigned_client.into(),
            name: job.name,
            path: job.path,
            subtitle: job.subtitle,
            custom_parameters: job.custom_parameters
        }
    }
}

impl JobJson {
    pub fn to_json(self) -> String {
        let mut wrapper = Vec::new();
        wrapper.push(self);
        serde_json::to_string_pretty(&wrapper).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssignedClient {
    #[serde(rename = "$ref")]
    pub collection: String,
    #[serde(rename = "$id")]
    pub id: bson::oid::ObjectId,
    #[serde(rename = "$db", default, skip_serializing_if = "String::is_empty")]
    pub db: String,
}
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssignedClientJson {
    #[serde(rename = "Ref")]
    pub collection: String,
    #[serde(rename = "ID", serialize_with = "convert_oid")]
    pub id: bson::oid::ObjectId,
    #[serde(rename = "DB", default, skip_serializing_if = "String::is_empty")]
    pub db: String,
}

impl From<AssignedClient> for AssignedClientJson {
    fn from(ac: AssignedClient) -> Self {
        AssignedClientJson { id: ac.id, collection: ac.collection, db: ac.db } 
    }
}

impl From<Client> for AssignedClient {
    fn from(client: Client) -> Self {
        AssignedClient {
            collection: "clients".to_string(),
            db: "".to_string(),
            id: client.id.unwrap(),
        }
    }
}

fn convert_oid<S>(x: &bson::oid::ObjectId, s: S) -> Result<S::Ok, S::Error> where S: Serializer {
    s.serialize_str(&x.to_string())
}

/// loop over every client within a priority group
///
/// rules: get the client...
/// - with the lowest jobcount
/// - that is online or has the ignore_online flag enabled
/// - that hasn't reached its maximum job count
pub fn get_eligible_client(
    grouped_clients: BTreeMap<i32, Vec<&Client>>,
    machine_jobcounts: HashMap<String, i32>,
) -> Result<(String, i32, i32), Box<dyn Error>> {
    for (_, clients) in grouped_clients {
        let mut job_count = i32::MAX;
        let mut eligible: Option<&Client> = None;
        for client in clients {
            let key = client
                .id
                .to_owned()
                .ok_or(InfuserError {
                    message: "a client in the database has no id, could not determine eligible clients".to_string(),
                })?
                .to_string();
            if !client.online && !client.ignore_online {
                continue;
            }
            if let Some(count) = machine_jobcounts.get(&key) {
                if *count < job_count && *count < client.maximum_jobs {
                    eligible = Some(client);
                    job_count = *count;
                }
            } else {
                eligible = Some(client);
                job_count = 0;
            }
        }
        // if a client was found within the priority group,
        // return it, otherwise move on to the next one
        match eligible {
            Some(client) => {
                return Ok((client.id.to_owned().unwrap().to_string(), job_count, client.maximum_jobs));
            }
            None => (),
        }
    }
    // if no client has been found, return an error
    Err(Box::new(InfuserError {
        message: "no eligible client found".to_string(),
    }))
}

pub fn group_clients(client_vec: &Vec<Client>) -> BTreeMap<i32, Vec<&Client>> {
    let mut dict = BTreeMap::new();
    for client in client_vec {
        let prio = client.priority;
        dict.entry(prio).or_insert(Vec::new()).push(client);
        /*
        match dict.entry(prio) {
            Entry::Vacant(e) => e.insert(vec![client]);
            Entry::Occupied(mut e) => {}
        }
         */
    }
    dict
}

#[cfg(test)]
mod tests {
    use crate::db;
    use std::error::Error;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_insert() -> Result<(), Box<dyn Error>> {
        let mongo_client = db::connect("mongodb://192.168.178.75:27107")?;
        if let Some(res) = db::get_clients(&mongo_client, &config.db_name)?.get(0) {
            let iid = db::insert_job(&mongo_client, &config.db_name, res, 
                &mut db::Job {
                    id: None,
                    path: "\\\\vdr-u\\SDuRec\\Recording\\exists\\Geheimnisvolle Wildblumen_2021-04-10-14-58-01-arte HD (AC3,deu).ts".to_string(),
                    name: "Geheimnisvolle Wildblumen".to_string(),
                    subtitle: "Blütenpracht im Wald".to_string(),
                    assigned_client: res.into(),
                    custom_parameters: Vec::new()
            })?;
            println!("{}", iid);
        }
        Ok(())

    }
}
