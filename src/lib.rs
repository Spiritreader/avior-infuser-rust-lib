pub mod cfg;
pub mod db;
pub mod log;

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt,
};

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

pub fn get_eligible_client(
    grouped_clients: BTreeMap<i32, Vec<&db::Client>>,
    machine_jobcounts: HashMap<String, i32>,
) -> Result<(String, i32, i32), Box<dyn Error>> {
    for (_, clients) in grouped_clients {
        let mut job_count = i32::MAX;
        let mut eligible: Option<db::Client> = None;
        // loop over every client within a priority group
        // rules: get the client...
        // - with the lowest jobcount
        // - that is online or has the ignore_online flag enabled
        // - that hasn't reached its maximum job count
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
                    eligible = Some(client.to_owned());
                    job_count = *count;
                }
            } else {
                eligible = Some(client.to_owned());
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

pub fn group_clients(client_vec: &Vec<db::Client>) -> BTreeMap<i32, Vec<&db::Client>> {
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
    use crate::cfg;
    use crate::db;
    use std::error::Error;
    const CFG_PATH: &str = "../config.json";

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test_insert() -> Result<(), Box<dyn Error>> {
        let config = cfg::read(CFG_PATH)?;
        let mongo_client = db::connect(&config)?;
        if let Some(res) = db::get_clients(&mongo_client, &config.db_name)?.get(0) {
            let iid = db::insert_job(&mongo_client, &config.db_name, res, 
                &mut db::Job {
                    id: None,
                    path: "\\\\vdr-u\\SDuRec\\Recording\\exists\\Geheimnisvolle Wildblumen_2021-04-10-14-58-01-arte HD (AC3,deu).ts".to_string(),
                    name: "Geheimnisvolle Wildblumen".to_string(),
                    subtitle: "Blütenpracht im Wald".to_string(),
                    assigned_client: db::AssignedClient::default(),
                    custom_parameters: Vec::new()
            })?;
            println!("{}", iid);
        }
        Ok(())

    }
}
