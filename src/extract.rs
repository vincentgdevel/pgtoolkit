use std::{collections::HashMap, error::Error, fs, io::ErrorKind};

use log::info;
use postgres::{Client, NoTls};

use crate::{
    view_detail::{Index, ViewDetail, ALL_VIEWS_QUERY, DEPEND_QUERY},
    DDL_OUTPUT_PATH, EXTRACTION_PATH,
};

pub fn extract_dependency_ddls(
    obj_ref: Option<&String>,
    db_uri: &str,
) -> Result<Vec<ViewDetail>, Box<dyn Error>> {
    let mut client: Client = Client::connect(db_uri, NoTls)?;

    recreate_dir(EXTRACTION_PATH);
    recreate_dir(DDL_OUTPUT_PATH);

    let query = match obj_ref {
        // Work-around as tokio_postgres doesn't support parameterized queries for Regclass params
        // Note: May be vulnerable to SQL Injection
        Some(s) => DEPEND_QUERY.replace("$1", &format!("'{}'", s)),
        None => ALL_VIEWS_QUERY.to_string(),
    };

    let mut view_deets: HashMap<String, ViewDetail> = HashMap::new();

    for row in client.query(&query, &[])? {
        let mut indexes: Vec<Index> = vec![];
        let index_name: Option<String> = row.get(6);
        if index_name.is_some() {
            indexes = vec![Index::new(row.get(6), row.get(7))];
        }

        let mut view_detail = ViewDetail::new(
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
            indexes,
        );

        let key = format!("{}.{}", view_detail.schema_name, view_detail.view);
        if let Some(vd) = view_deets.get_mut(&key) {
            vd.indexes.append(&mut view_detail.indexes);
        } else {
            view_deets.insert(key, view_detail.clone());
        }
    }

    for (k, v) in view_deets.clone() {
        v.write(EXTRACTION_PATH);
        v.write_ddls(DDL_OUTPUT_PATH);
        info!("extracted {}", k);
    }

    Ok(view_deets.values().cloned().collect::<Vec<ViewDetail>>())
}

fn recreate_dir(path: &str) {
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(err) => match err.kind() {
            // delete and recreate directory
            ErrorKind::AlreadyExists => {
                fs::remove_dir_all(path).unwrap();
                fs::create_dir(path).unwrap();
            }
            _ => {
                panic!("Unable to prepare extraction folder!\n{:?}", err)
            }
        },
    };
}
