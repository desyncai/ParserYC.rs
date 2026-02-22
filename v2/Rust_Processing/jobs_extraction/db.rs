use anyhow::{Context, Result};
use rusqlite::Connection;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct JobPage {
    pub job_id: i64,
    pub url: String,
    pub text_content: String,
    pub scraped_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ShortenedRow {
    pub job_id: i64,
    pub url: String,
    pub company_slug: Option<String>,
    pub job_slug: Option<String>,
    pub text_shortened: String,
    pub is_blank: bool,
    pub is_404: bool,
}

#[derive(Debug, Clone)]
pub struct MetaRow {
    pub job_id: i64,
    pub role_bucket: String,
    pub has_emoji: bool,
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
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS job_text_shortened (
            job_id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            company_slug TEXT,
            job_slug TEXT,
            text_shortened TEXT,
            raw_len INTEGER,
            shortened_len INTEGER,
            is_blank INTEGER,
            is_404 INTEGER,
            nav_removed INTEGER,
            similar_removed INTEGER,
            footer_removed INTEGER,
            founder_removed INTEGER,
            scraped_at TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs_page_data(job_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_job_text_company ON job_text_shortened(company_slug);

        CREATE TABLE IF NOT EXISTS job_meta (
            job_id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            company_slug TEXT,
            job_slug TEXT,
            job_title TEXT,
            role_raw TEXT,
            role_bucket TEXT,
            job_type TEXT,
            position_type TEXT,
            location_raw TEXT,
            pay_raw TEXT,
            experience_raw TEXT,
            visa_raw TEXT,
            has_emoji INTEGER,
            header_ok INTEGER,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs_page_data(job_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_job_meta_bucket ON job_meta(role_bucket);

        CREATE TABLE IF NOT EXISTS job_body (
            job_id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            role_description TEXT,
            body_ok INTEGER,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs_page_data(job_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS job_stats (
            metric TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS job_sections (
            job_id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            responsibilities TEXT,
            requirements TEXT,
            nice_to_have TEXT,
            benefits TEXT,
            summary TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs_page_data(job_id) ON DELETE CASCADE
        );
        "#,
    )?;
    Ok(())
}

pub fn fetch_jobs(conn: &Connection) -> Result<Vec<JobPage>> {
    let mut stmt = conn.prepare(
        "SELECT job_id, url, text_content, scraped_at
         FROM jobs_page_data
         WHERE text_content IS NOT NULL",
    )?;

    let rows = stmt
        .query_map([], |row| {
            Ok(JobPage {
                job_id: row.get(0)?,
                url: row.get(1)?,
                text_content: row.get(2)?,
                scraped_at: row.get(3)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(rows)
}

pub fn fetch_shortened(conn: &Connection) -> Result<Vec<ShortenedRow>> {
    let mut stmt = conn.prepare(
        "SELECT job_id, url, company_slug, job_slug, text_shortened, is_blank, is_404
         FROM job_text_shortened",
    )?;

    let rows = stmt
        .query_map([], |row| {
            Ok(ShortenedRow {
                job_id: row.get(0)?,
                url: row.get(1)?,
                company_slug: row.get(2)?,
                job_slug: row.get(3)?,
                text_shortened: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                is_blank: row.get::<_, Option<i64>>(5)?.unwrap_or(0) != 0,
                is_404: row.get::<_, Option<i64>>(6)?.unwrap_or(0) != 0,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(rows)
}

pub fn fetch_text_map(conn: &Connection) -> Result<HashMap<i64, String>> {
    let mut stmt = conn.prepare("SELECT job_id, text_shortened FROM job_text_shortened")?;
    let mut map = HashMap::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let job_id: i64 = row.get(0)?;
        let text: Option<String> = row.get(1)?;
        if let Some(t) = text {
            map.insert(job_id, t);
        }
    }
    Ok(map)
}

pub fn fetch_meta_rows(conn: &Connection) -> Result<Vec<MetaRow>> {
    let mut stmt = conn.prepare("SELECT job_id, role_bucket, has_emoji FROM job_meta")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(MetaRow {
                job_id: row.get(0)?,
                role_bucket: row
                    .get::<_, Option<String>>(1)?
                    .unwrap_or_else(|| "Other".to_string()),
                has_emoji: row.get::<_, Option<i64>>(2)?.unwrap_or(0) != 0,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}
