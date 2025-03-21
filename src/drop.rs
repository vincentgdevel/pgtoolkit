use std::{error::Error, fs, io::ErrorKind};

use log::info;
use postgres::{Client, NoTls};

use crate::{
    extract::extract_dependency_ddls, view_detail::ViewDetail, DDL_OUTPUT_PATH, EXTRACTION_PATH,
    REORDERED_DDL_OUTPUT_PATH, REORDERED_EXTRACTION_PATH,
};

const DROP_MV: &str = "DROP MATERIALIZED VIEW $1";
const DROP_V: &str = "DROP VIEW $1";

pub fn drop_views(
    obj_ref: Option<&String>,
    db_uri: &str,
    reorder: bool,
    backup: bool,
) -> Result<(), Box<dyn Error>> {
    let mut client: Client = Client::connect(db_uri, NoTls)?;

    let mut deps: Vec<ViewDetail> = match obj_ref {
        Some(_) => extract_dependency_ddls(obj_ref, db_uri).unwrap(),
        None => ViewDetail::from_files(EXTRACTION_PATH),
    };

    if deps.is_empty() {
        info!("Nothing to drop!");
    }

    let mut ordered_deps: Vec<ViewDetail> = vec![];
    let mut order_counter = 1;

    while !deps.is_empty() {
        let dep_count = deps.len();
        let mut idx = 0_usize;

        while idx < deps.len() {
            let view = deps[idx].view.to_owned();
            let schema_name = deps[idx].schema_name.to_owned();
            let kind = deps[idx].kind.to_owned();

            let query = (if kind.eq("m") { DROP_MV } else { DROP_V })
                .replace("$1", &format!("{}.{}", schema_name, view));

            if client.execute(&query, &[]).is_ok() {
                info!("Dropped {}.{}", schema_name, view);

                let mut updated_view_detail = deps[idx].clone();
                updated_view_detail.level = order_counter;
                ordered_deps.push(updated_view_detail);
                order_counter += 1;

                deps.remove(idx);
                continue;
            }
            idx += 1;
        }

        if deps.len() == dep_count {
            // Nothing was removed form deps!
            let erring_views = deps
                .iter()
                .map(|d| format!("{}.{}", d.schema_name.clone(), d.view.clone()))
                .collect::<Vec<String>>()
                .join("\n");

            panic!(
                "Unable to drop {} views were not dropped! \n erring views: \n {}",
                deps.len(),
                erring_views
            )
        }
    }

    if backup {
        let backup_dir = format!("{}_bak", EXTRACTION_PATH);
        recreate_dir(&backup_dir);
        for entry in fs::read_dir(EXTRACTION_PATH)? {
            let entry = entry?;
            let backup_file_path =
                format!("{}/{}", &backup_dir, entry.file_name().to_str().unwrap());
            fs::copy(entry.path(), backup_file_path)?;
        }
    }

    if reorder {
        recreate_dir(REORDERED_EXTRACTION_PATH);
        recreate_dir(REORDERED_DDL_OUTPUT_PATH);

        for dep in ordered_deps {
            dep.write(REORDERED_EXTRACTION_PATH);
            dep.write_ddls(REORDERED_DDL_OUTPUT_PATH);
        }

        fs::remove_dir_all(EXTRACTION_PATH)?;
        fs::remove_dir_all(DDL_OUTPUT_PATH)?;

        fs::rename(REORDERED_EXTRACTION_PATH, EXTRACTION_PATH)?;
        fs::rename(REORDERED_DDL_OUTPUT_PATH, DDL_OUTPUT_PATH)?;
    }

    Ok(())
}

fn recreate_dir(dir: &str) {
    match fs::create_dir(dir) {
        Ok(()) => {}
        Err(err) => match err.kind() {
            // delete and recreate directory
            ErrorKind::AlreadyExists => {
                fs::remove_dir_all(dir).unwrap();
                fs::create_dir(dir).unwrap();
            }
            _ => {
                panic!("Unable to prepare folder!\n{:?}", err)
            }
        },
    };
}
