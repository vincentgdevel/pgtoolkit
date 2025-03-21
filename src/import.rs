use std::error::Error;

use log::info;
use postgres::{Client, NoTls};
use substring::Substring;

use crate::{view_detail::ViewDetail, EXTRACTION_PATH};

pub fn import_scripts(db_uri: &str, with_no_data: bool) -> Result<(), Box<dyn Error>> {
    info!("Importing scripts using with_no_data={}", with_no_data);

    let mut client: Client = Client::connect(db_uri, NoTls)?;
    let mut view_deets = ViewDetail::from_files(EXTRACTION_PATH);

    view_deets.sort_by(|a, b| b.level.cmp(&a.level));

    let mut imported: Vec<ViewDetail> = Vec::new();

    while !view_deets.is_empty() {
        let dep_count = view_deets.len();
        let mut idx = 0_usize;

        while idx < view_deets.len() {
            let mut definition = view_deets[idx].definition.to_owned();

            if with_no_data && view_deets[idx].kind.eq("m") {
                definition =
                    definition.substring(0, definition.len() - 1).to_owned() + " WITH NO DATA;";
            }

            if client.execute(&definition, &[]).is_ok() {
                info!(
                    "imported {}.{} ",
                    &view_deets[idx].schema_name, &view_deets[idx].view
                );
                for pg_index in &view_deets[idx].indexes {
                    client.execute(&pg_index.definition, &[])?;
                    info!("\t > imported index {}", &pg_index.name);
                }
                imported.push(view_deets[idx].clone());
                view_deets.remove(idx);
                continue;
            }

            idx += 1;
        }

        if view_deets.len() == dep_count {
            panic!("Unable to import {} views!", view_deets.len());
        }
    }

    Ok(())
}
