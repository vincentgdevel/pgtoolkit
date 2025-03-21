use std::env::var;

use regex::Regex;

pub mod drop;
pub mod extract;
pub mod import;
pub mod log;
pub mod view_detail;

pub const EXTRACTION_PATH: &str = "extracted";
pub const DDL_OUTPUT_PATH: &str = "extracted_ddl";
pub const REORDERED_EXTRACTION_PATH: &str = "extracted_tmp";
pub const REORDERED_DDL_OUTPUT_PATH: &str = "extracted_ddl_tmp";

#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref RE: Regex = Regex::new(r"(\d+)-([m|v])-((.+)\.sql)").unwrap();
    pub static ref DEFAULT_DATABASE_URI: String =
        var("DEFAULT_DATABASE_URI").unwrap_or_else(|_| {
            "postgresql://postgres:password@localhost:5432/public".to_string()
        });
}