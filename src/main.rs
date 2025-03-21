use clap::{arg, command, ArgAction};
use log::{error, LevelFilter};
use pgtk::{
    drop::drop_views, extract::extract_dependency_ddls, import::import_scripts, log::SimpleLogger,
};

use pgtk::DEFAULT_DATABASE_URI;

static LOGGER: SimpleLogger = SimpleLogger;

fn main() {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
        .unwrap();

    let cmd = clap::Command::new("pgtk")
        .bin_name("pgtk")
        .subcommand_required(true)
        .subcommand(
            command!("extract")
                .about("Extracts the DDLs for all dependencies of a given pg object")
                .arg(
                    arg!(--"object_ref" <OBJECT_REFERENCE>)
                        .help("Table, View or Materialized View. If empty, will extract DDLs for all views and materialized views")
                        .short('o')
                        .required(false),
                )
                .arg(
                    arg!(--"db_uri" <DATABASE_URI>)
                        .short('d')
                        .default_value(DEFAULT_DATABASE_URI.as_str())
                        .required(false)
                ),
        )
        .subcommand(
            command!("import")
                .about("Import all pg objects extracted using this tool")
                .arg(
                    arg!(--"db_uri" <DATABASE_URI>)
                        .short('d')
                        .default_value(DEFAULT_DATABASE_URI.as_str())
                        .required(false),
                )
                .arg(
                    arg!(--"with_no_data" <DATABASE_URI>)
                        .help("Create Materialized View with 'NO DATA' flag")
                        .action(ArgAction::SetFalse)
                        .required(false),
                ),
        )
        .subcommand(
            command!("drop")
                .about("Drop all dependencies for a given pg object")
                .arg(
                    arg!(--"object_ref" <OBJECT_REFERENCE>)
                        .help("Table, View or Materialized View. If empty will drop all extracted views and materialized views")
                        .short('o')
                        .required(false),
                )
                .arg(
                    arg!(--"db_uri" <DATABASE_URI>)
                        .short('d')
                        .default_value(DEFAULT_DATABASE_URI.as_str())
                        .required(false),
                )
                .arg(
                    arg!(--"reorder" <DATABASE_URI>)
                        .help("Reorder extracted objects")
                        .action(ArgAction::SetFalse)
                        .required(false),
                )
                .arg(
                    arg!(--"backup" <DATABASE_URI>)
                        .help("Create backup of extracted objects")
                        .action(ArgAction::SetFalse)
                        .required(false),
                ),
        );

    let matches = cmd.get_matches();

    match matches.subcommand() {
        Some(("extract", matches)) => {
            extract_dependency_ddls(
                matches.get_one::<String>("object_ref"),
                matches.get_one::<String>("db_uri").unwrap(),
            )
            .unwrap();
        }
        Some(("import", matches)) => {
            import_scripts(
                matches.get_one::<String>("db_uri").unwrap(),
                matches.contains_id("with_no_data"),
            )
            .unwrap();
        }
        Some(("drop", matches)) => {
            drop_views(
                matches.get_one::<String>("object_ref"),
                matches.get_one::<String>("db_uri").unwrap(),
                matches.contains_id("reorder"),
                matches.contains_id("backup"),
            )
            .unwrap();
        }
        _ => error!("This Feature does not exist!"),
    };
}
