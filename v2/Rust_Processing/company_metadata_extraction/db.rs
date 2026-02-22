use anyhow::{Context, Result};
use rusqlite::{params, types::Value as SqlValue, Connection, ToSql};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;


const SCHEMA_JSON: &str = include_str!("../schema.json");

#[derive(Deserialize)]
struct Schema {
    tables: std::collections::HashMap<String, Vec<String>>,
    indexes: Vec<(String, String, String)>,
}

#[derive(Debug, Clone)]
pub struct PageInput {
    pub url: String,
    pub text: String,
    pub external_links: Vec<String>,
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
    let conn = Connection::open(&p).with_context(|| format!("Failed to open {:?}", p))?;
    conn.execute("PRAGMA foreign_keys = ON", [])?;
    Ok(conn)
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

pub fn fetch_pages(conn: &Connection) -> Result<Vec<PageInput>> {
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
        .query_map([], |row| {
            let url: String = row.get(0)?;
            let text: String = row.get(1)?;
            let external_links_raw: Option<String> = row.get(2)?;
            Ok(PageInput {
                url,
                text,
                external_links: parse_links(external_links_raw),
            })
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}

fn parse_links(raw: Option<String>) -> Vec<String> {
    let mut links = Vec::new();
    if let Some(text) = raw {
        if let Ok(val) = serde_json::from_str::<Value>(&text) {
            match val {
                Value::Array(arr) => {
                    for v in arr {
                        if let Some(s) = v.as_str() {
                            links.push(s.to_string());
                        }
                    }
                }
                Value::Object(obj) => {
                    for (_k, v) in obj {
                        if let Some(s) = v.as_str() {
                            links.push(s.to_string());
                        }
                    }
                }
                _ => {}
            }
        }
    }
    links
}

pub fn insert_company(conn: &Connection, slug: &str, name: &str, url: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO companies (slug, name, source_url) VALUES (?1, ?2, ?3)",
        params![slug, name, url],
    )?;
    Ok(())
}

pub fn update_company(conn: &Connection, slug: &str, fields: Vec<(&str, SqlValue)>) -> Result<()> {
    if fields.is_empty() {
        return Ok(());
    }
    let set_clause: Vec<String> = fields
        .iter()
        .enumerate()
        .map(|(i, (k, _))| format!("{} = ?{}", k, i + 1))
        .collect();
    let sql = format!(
        "UPDATE companies SET {} WHERE slug = ?{}",
        set_clause.join(", "),
        fields.len() + 1
    );

    let params_owned: Vec<SqlValue> = fields.into_iter().map(|(_, v)| v).collect();
    let mut params: Vec<&dyn ToSql> = params_owned.iter().map(|v| v as &dyn ToSql).collect();
    params.push(&slug as &dyn ToSql);
    conn.execute(&sql, params.as_slice())?;
    Ok(())
}

pub fn insert_tag(conn: &Connection, slug: &str, tag: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO tags (company_slug, tag) VALUES (?1, ?2)",
        params![slug, tag],
    )?;
    Ok(())
}

pub fn insert_founder(conn: &Connection, slug: &str, name: &str, title: Option<&str>) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO founders (company_slug, name, title) VALUES (?1, ?2, ?3)",
        params![slug, name, title],
    )?;
    Ok(())
}

pub fn insert_news(conn: &Connection, slug: &str, title: &str, source: Option<&str>) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO news (company_slug, title, source) VALUES (?1, ?2, ?3)",
        params![slug, title, source],
    )?;
    Ok(())
}

pub fn insert_link(
    conn: &Connection,
    slug: &str,
    founder_id: Option<i64>,
    url: &str,
    pattern: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO links (company_slug, founder_id, url, pattern) VALUES (?1, ?2, ?3, ?4)",
        params![slug, founder_id, url, pattern],
    )?;
    Ok(())
}

pub fn insert_pass_metric(
    conn: &Connection,
    run_id: &str,
    pass_name: &str,
    pages: usize,
    before: usize,
    after: usize,
) -> Result<()> {
    let removed = before.saturating_sub(after);
    conn.execute(
        "INSERT OR REPLACE INTO company_pass_metrics (run_id, pass_name, pages, chars_before, chars_after, chars_removed) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![run_id, pass_name, pages as i64, before as i64, after as i64, removed as i64],
    )?;
    Ok(())
}

pub fn insert_residual(conn: &Connection, run_id: &str, slug: &str, pass_name: &str, chars: usize, sample: &str) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO company_text_residual (run_id, company_slug, pass_name, remaining_chars, sample) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![run_id, slug, pass_name, chars as i64, sample],
    )?;
    Ok(())
}

#[allow(dead_code)]
pub fn existing_company_slugs(conn: &Connection) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare("SELECT slug FROM companies")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}
