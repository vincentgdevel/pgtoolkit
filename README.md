Postgres Toolkit

A CLI tool that:
1. exports the DDL and Indexes of all the views or materialized views of a given table, view or materialized view.
2. import DDLs and indexes from exported view/s, materalized view/s
3. drops DDLs and indexes from exported view/s, materalized view/s




`pgtk extract` - Extracts the DDLs for all dependencies of a given pg object

|  arg | notes |
|---|---|
|  `object_ref` or `o` | Table, View or Materialized View. If empty, will extract DDLs for all views and materialized views |
| `db_uri` | DB URI connection string |

<br>
<br>
<br>

`pgtk import` - Extracts the DDLs for all dependencies of a given pg object

|  arg | notes |
|---|---|
|  `with_no_data` (swtich)| Create Materialized View with 'NO DATA' flag |
| `db_uri` | DB URI connection string |

<br>
<br>
<br>

