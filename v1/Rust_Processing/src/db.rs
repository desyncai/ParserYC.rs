//! Database layer - connection, tables, insert, update.

use anyhow::{Context, Result};
use rusqlite::{Connection, ToSql};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;

const SCHEMA_JSON: &str = include_str!("../schema.json");

#[derive(Deserialize)]
struct Schema {
    tables: HashMap<String, Vec<String>>,
    indexes: Vec<(String, String, String)>,
}

pub fn path() -> PathBuf {
    if let Ok(p) = env::var("YC_DB_PATH") {
        return PathBuf::from(p);
    }
    [
        "../Sqlite_Database/data/yc.sqlite",
        "../../Sqlite_Database/data/yc.sqlite",
        "Sqlite_Database/data/yc.sqlite",
    ]
    .iter()
    .map(PathBuf::from)
    .find(|p| p.exists())
    .unwrap_or_else(|| PathBuf::from("../Sqlite_Database/data/yc.sqlite"))
}

pub fn connect() -> Result<Connection> {
    let p = path();
    Connection::open(&p).with_context(|| format!("Failed to open {:?}", p))
}

pub fn create_tables(conn: &Connection) -> Result<()> {
    let schema: Schema = serde_json::from_str(SCHEMA_JSON)?;
    for (name, cols) in &schema.tables {
        let sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", name, cols.join(", "));
        conn.execute(&sql, [])?;
    }
    for (idx, table, cols) in &schema.indexes {
        let sql = format!("CREATE INDEX IF NOT EXISTS {} ON {} ({})", idx, table, cols);
        conn.execute(&sql, [])?;
    }
    Ok(())
}

pub fn insert(conn: &Connection, table: &str, data: &[(&str, &dyn ToSql)]) -> Result<i64> {
    if data.is_empty() {
        return Ok(0);
    }
    let cols: Vec<&str> = data.iter().map(|(k, _)| *k).collect();
    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("?{}", i)).collect();
    let sql = format!(
        "INSERT OR IGNORE INTO {} ({}) VALUES ({})",
        table,
        cols.join(", "),
        placeholders.join(", ")
    );
    let params: Vec<&dyn ToSql> = data.iter().map(|(_, v)| *v).collect();
    conn.execute(&sql, params.as_slice())?;
    Ok(conn.last_insert_rowid())
}

#[allow(dead_code)]
pub fn insert_batch(
    conn: &Connection,
    table: &str,
    cols: &[&str],
    rows: Vec<Vec<Box<dyn ToSql>>>,
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }

    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("?{}", i)).collect();
    let sql = format!(
        "INSERT OR IGNORE INTO {} ({}) VALUES ({})",
        table,
        cols.join(", "),
        placeholders.join(", ")
    );

    let mut inserted = 0;
    let mut stmt = conn.prepare(&sql)?;

    for row in rows {
        let params: Vec<&dyn ToSql> = row.iter().map(|v| v.as_ref()).collect();
        inserted += stmt.execute(params.as_slice())?;
    }

    Ok(inserted)
}

pub fn update(
    conn: &Connection,
    table: &str,
    data: &[(&str, &dyn ToSql)],
    key: &str,
    key_val: &dyn ToSql,
) -> Result<usize> {
    if data.is_empty() {
        return Ok(0);
    }
    let sets: Vec<String> = data
        .iter()
        .enumerate()
        .map(|(i, (k, _))| format!("{} = ?{}", k, i + 1))
        .collect();
    let sql = format!(
        "UPDATE {} SET {} WHERE {} = ?{}",
        table,
        sets.join(", "),
        key,
        data.len() + 1
    );
    let mut params: Vec<&dyn ToSql> = data.iter().map(|(_, v)| *v).collect();
    params.push(key_val);
    Ok(conn.execute(&sql, params.as_slice())?)
}

pub fn fetch_pages(conn: &Connection) -> Result<Vec<(String, Option<String>, Option<String>)>> {
    let mut stmt = conn.prepare(
        "SELECT url, text_content, external_links FROM pagedataobjects
         WHERE url LIKE 'https://www.ycombinator.com/companies/%'
         AND url NOT LIKE '%/industry/%'
         AND url NOT LIKE '%/location/%'
         AND url NOT LIKE '%/batch/%'
         AND url NOT LIKE '%/tags/%'
         AND text_content IS NOT NULL
         AND text_content NOT LIKE '%Startups funded by Y Combinator%'
         AND text_content NOT LIKE '%404%File Not Found%'",
    )?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}

/// Return all company slugs currently in the companies table.
pub fn company_slug_set(conn: &Connection) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare("SELECT slug FROM companies")?;
    let slugs = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(slugs)
}

pub fn count(conn: &Connection, table: &str) -> Result<i64> {
    Ok(conn.query_row(&format!("SELECT COUNT(*) FROM {}", table), [], |r| r.get(0))?)
}

pub fn stats(conn: &Connection) -> Result<()> {
    let schema: Schema = serde_json::from_str(SCHEMA_JSON)?;
    println!("\n=== Stats ===");
    for name in schema.tables.keys() {
        if let Ok(n) = count(conn, name) {
            println!("{}: {}", name, n);
        }
    }
    Ok(())
}
