use std::{
    fs::{self, File},
    io::{BufReader, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct ViewDetail {
    pub view: String,
    pub schema_name: String,
    pub level: i32,
    pub kind: String,
    pub definition: String,
    pub indexes: Vec<Index>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct Index {
    pub name: String,
    pub definition: String,
}

pub const ALL_VIEWS_QUERY: &str = "
    SELECT v.*, i.indexname as index_name, i.indexdef as index_def
    FROM
    (
        SELECT matviewname as view,
            schemaname as schema_name,
            1 as level,
            'm' as relkind,
            CONCAT('CREATE MATERIALIZED VIEW ', schemaname, '.', matviewname, ' AS', E'\n', definition) as definition
        FROM pg_matviews
        WHERE schemaname not in ('information_schema', 'pg_catalog')
        UNION
        SELECT viewname as view,
            schemaname as schema_name,
            1 as level,
            'v' as relkind,
            CONCAT('CREATE VIEW ', schemaname, '.', viewname, ' AS', E'\n', definition) as definition
        FROM pg_views
        WHERE schemaname not in ('information_schema', 'pg_catalog')
    ) v left join pg_indexes i on v.view = i.tablename and v.schema_name = i.schemaname
";

pub const DEPEND_QUERY: &str = "
    WITH RECURSIVE views AS (
        -- get the directly depending views
        SELECT DISTINCT
            v.oid :: regclass AS view_class,
            v.relname as view,
            n.nspname as schema_name,
            v.relkind,
            1 AS level
        FROM
            pg_depend AS d
            JOIN pg_rewrite AS r ON r.oid = d.objid
            JOIN pg_class AS v ON v.oid = r.ev_class
            JOIN pg_namespace n ON n.oid = v.relnamespace

        WHERE
            v.relkind IN ('v', 'mv')
            AND d.classid = 'pg_rewrite' :: regclass
            AND d.refclassid = 'pg_class' :: regclass
            AND d.deptype = 'n'
            AND d.refobjid = $1 :: regclass
        UNION
            -- add the views that depend on these
        SELECT
            v.oid :: regclass,
            v.relname as view,
            n.nspname as schema_name,
            v.relkind,
            views.level + 1
        FROM
            views
            JOIN pg_depend AS d ON d.refobjid = views.view_class
            JOIN pg_rewrite AS r ON r.oid = d.objid
            JOIN pg_class AS v ON v.oid = r.ev_class
            JOIN pg_namespace n ON n.oid = v.relnamespace

        WHERE
            v.relkind IN ('v', 'mv')
            AND d.classid = 'pg_rewrite' :: regclass
            AND d.refclassid = 'pg_class' :: regclass
            AND d.deptype = 'n'
            AND v.oid <> views.view_class -- avoid loop
    )
    SELECT
        t.view_class :: text,
        t.view,
        t.schema_name,
        t.level :: integer,
        t.relkind,
        t.definition,
        i.indexname as index_name,
        i.indexdef as index_def
    FROM (
    SELECT DISTINCT
        view_class,
        view,
        schema_name,
        max(level) as level,
        cast(relkind as text) as relkind,
        (
            CASE WHEN relkind = 'v' THEN CONCAT(
                'CREATE VIEW ',
                schema_name,
                '.',
                view,
                ' AS',
                E'\n',
                (
                    SELECT pg_get_viewdef(view_class)
                )
            ) WHEN relkind = 'mv' THEN CONCAT(
                'CREATE MATERIALIZED VIEW ',
                schema_name,
                '.',
                view,
                ' AS',
                E'\n',
                (
                    SELECT
                        pg_get_viewdef(view_class)
                )
            ) ELSE '' END
        ) as definition
    FROM
        views
    GROUP BY view_class, view, schema_name, relkind
    ) t LEFT JOIN pg_indexes i on t.view = i.tablename and t.schema_name = i.schemaname
    GROUP BY view_class, view, schema_name, level, relkind, definition, index_name, index_def
    ORDER BY
        view,
        level,
        relkind DESC
";

impl Index {
    pub fn new(name: String, definition: String) -> Self {
        Self { name, definition }
    }
}

impl ViewDetail {
    pub fn new(
        view: String,
        schema_name: String,
        level: i32,
        kind: String,
        definition: String,
        indexes: Vec<Index>,
    ) -> Self {
        Self {
            view: view.to_owned(),
            schema_name: schema_name.to_owned(),
            level,
            kind: kind.to_owned(),
            definition,
            indexes,
        }
    }

    pub fn get_filename(&self) -> String {
        format!(
            "{}-{}-{}-{}.dat",
            self.level, self.kind, self.schema_name, self.view
        )
    }

    pub fn from_file(path: &Path) -> ViewDetail {
        // return serde_json::from_str(fs::read_to_string(path).unwrap().as_str()).unwrap();
        let decoded: ViewDetail =
            bincode::deserialize_from(BufReader::new(File::open(path).unwrap())).unwrap();
        decoded
    }

    pub fn from_files(extraction_path: &str) -> Vec<ViewDetail> {
        let mut scripts = fs::read_dir(extraction_path)
            .unwrap()
            .map(|entry| ViewDetail::from_file(entry.unwrap().path().as_path()))
            .collect::<Vec<ViewDetail>>();

        scripts.sort_by(|a, b| b.level.cmp(&a.level));
        scripts
    }

    pub fn write(&self, extraction_path: &str) {
        let mut script =
            File::create(format!("{}/{}", extraction_path, self.get_filename())).unwrap();
        script
            // .write_all(serde_json::to_string_pretty(&self).unwrap().as_bytes())
            .write_all(&bincode::serialize(&self).unwrap())
            .expect("Failed to write to file!");
    }

    pub fn write_ddls(&self, ddl_output_path: &str) {
        let mut script = File::create(format!(
            "{}/{}",
            ddl_output_path,
            self.get_filename().replace(".dat", ".sql")
        ))
        .unwrap();
        let output = format!(
            "{}\n\n{}",
            self.definition,
            self.indexes
                .iter()
                .map(|i| i.definition.clone())
                .collect::<Vec<String>>()
                .join(";\n")
        );
        script
            .write_all(output.as_bytes())
            .expect("Failed to write to file!");
    }
}
