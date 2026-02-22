use anyhow::Result;
use rusqlite::Connection;

const DB_PATH: &str = "data/yc.sqlite";

pub fn connect() -> Result<Connection> {
    let conn = Connection::open(DB_PATH)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
    Ok(conn)
}

pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS pages (
            id         INTEGER PRIMARY KEY,
            url        TEXT UNIQUE NOT NULL,
            slug       TEXT NOT NULL,
            visited    BOOLEAN NOT NULL DEFAULT 0,
            visited_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_pages_visited ON pages(visited);

        CREATE TABLE IF NOT EXISTS page_data (
            id         INTEGER PRIMARY KEY,
            page_id    INTEGER NOT NULL REFERENCES pages(id),
            url        TEXT NOT NULL,
            slug       TEXT NOT NULL,
            markdown   TEXT,
            status     INTEGER,
            error      TEXT,
            latency_ms INTEGER,
            scraped_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_page_data_slug ON page_data(slug);

        CREATE TABLE IF NOT EXISTS company_sections (
            id           INTEGER PRIMARY KEY,
            page_id      INTEGER NOT NULL REFERENCES page_data(id),
            slug         TEXT NOT NULL,
            url          TEXT NOT NULL,
            navbar       TEXT,
            header       TEXT,
            description  TEXT,
            news         TEXT,
            jobs         TEXT,
            footer       TEXT,
            founders_raw TEXT,
            launches     TEXT,
            extras       TEXT,
            processed_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sections_slug ON company_sections(slug);

        -- Extracted structured data
        CREATE TABLE IF NOT EXISTS companies (
            slug          TEXT PRIMARY KEY,
            url           TEXT NOT NULL,
            name          TEXT,
            tagline       TEXT,
            batch         TEXT,
            batch_season  TEXT,
            batch_year    INTEGER,
            status        TEXT CHECK(status IN ('Active','Public','Acquired','Inactive')),
            is_active     BOOLEAN GENERATED ALWAYS AS (status IN ('Active','Public')) STORED,
            homepage      TEXT,
            founded_year  INTEGER,
            team_size     INTEGER,
            location      TEXT,
            primary_partner TEXT,
            tags          TEXT,
            job_count     INTEGER DEFAULT 0,
            linkedin      TEXT,
            twitter       TEXT,
            facebook      TEXT,
            crunchbase    TEXT,
            github        TEXT,
            created_at    TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS founders (
            id            INTEGER PRIMARY KEY,
            company_slug  TEXT NOT NULL REFERENCES companies(slug),
            name          TEXT NOT NULL,
            title         TEXT,
            bio           TEXT,
            is_active     BOOLEAN NOT NULL DEFAULT 1,
            linkedin      TEXT,
            twitter       TEXT,
            UNIQUE(company_slug, name)
        );
        CREATE INDEX IF NOT EXISTS idx_founders_company ON founders(company_slug);

        CREATE TABLE IF NOT EXISTS news (
            id            INTEGER PRIMARY KEY,
            company_slug  TEXT NOT NULL REFERENCES companies(slug),
            title         TEXT NOT NULL,
            url           TEXT NOT NULL,
            published     TEXT,
            UNIQUE(company_slug, url)
        );
        CREATE INDEX IF NOT EXISTS idx_news_company ON news(company_slug);

        CREATE TABLE IF NOT EXISTS company_jobs (
            id            INTEGER PRIMARY KEY,
            company_slug  TEXT NOT NULL REFERENCES companies(slug),
            title         TEXT NOT NULL,
            url           TEXT NOT NULL,
            location      TEXT,
            salary        TEXT,
            experience    TEXT,
            apply_url     TEXT,
            UNIQUE(company_slug, url)
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_company ON company_jobs(company_slug);

        CREATE TABLE IF NOT EXISTS company_links (
            id            INTEGER PRIMARY KEY,
            company_slug  TEXT NOT NULL REFERENCES companies(slug),
            url           TEXT NOT NULL,
            domain        TEXT NOT NULL,
            link_type     TEXT,
            UNIQUE(company_slug, url)
        );
        CREATE INDEX IF NOT EXISTS idx_links_company ON company_links(company_slug);
        CREATE INDEX IF NOT EXISTS idx_links_domain ON company_links(domain);

        CREATE TABLE IF NOT EXISTS meeting_links (
            id            INTEGER PRIMARY KEY,
            company_slug  TEXT NOT NULL REFERENCES companies(slug),
            url           TEXT NOT NULL,
            domain        TEXT NOT NULL,
            link_type     TEXT NOT NULL,
            UNIQUE(company_slug, url)
        );
        CREATE INDEX IF NOT EXISTS idx_meeting_company ON meeting_links(company_slug);
        CREATE INDEX IF NOT EXISTS idx_meeting_type ON meeting_links(link_type);

        CREATE TABLE IF NOT EXISTS partners (
            slug        TEXT PRIMARY KEY,
            url         TEXT NOT NULL,
            name        TEXT NOT NULL,
            title       TEXT,
            bio         TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS company_partners (
            company_slug  TEXT NOT NULL REFERENCES companies(slug),
            partner_slug  TEXT NOT NULL REFERENCES partners(slug),
            match_method  TEXT NOT NULL CHECK(match_method IN ('url','name')),
            UNIQUE(company_slug, partner_slug)
        );
        CREATE INDEX IF NOT EXISTS idx_cp_company ON company_partners(company_slug);
        CREATE INDEX IF NOT EXISTS idx_cp_partner ON company_partners(partner_slug);
        ",
    )?;
    Ok(())
}

// ── Scraping ──

pub fn insert_pages(conn: &Connection, pages: &[(String, String)]) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;
    let mut count = 0;
    {
        let mut stmt = tx.prepare("INSERT OR IGNORE INTO pages (url, slug) VALUES (?1, ?2)")?;
        for (url, slug) in pages {
            count += stmt.execute(rusqlite::params![url, slug])?;
        }
    }
    tx.commit()?;
    Ok(count)
}

pub fn fetch_unvisited(
    conn: &Connection,
    limit: Option<usize>,
) -> Result<Vec<(i64, String, String)>> {
    let sql = match limit {
        Some(n) => format!(
            "SELECT id, url, slug FROM pages WHERE visited = 0 ORDER BY id LIMIT {}",
            n
        ),
        None => "SELECT id, url, slug FROM pages WHERE visited = 0 ORDER BY id".to_string(),
    };
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub struct ScrapeRow {
    pub page_id: i64,
    pub url: String,
    pub slug: String,
    pub markdown: Option<String>,
    pub status: Option<i32>,
    pub error: Option<String>,
    pub latency_ms: Option<i64>,
}


// ── Processing ──

pub struct ScrapedPage {
    pub page_data_id: i64,
    pub slug: String,
    pub url: String,
    pub markdown: String,
}

pub fn fetch_unprocessed(conn: &Connection, limit: Option<usize>) -> Result<Vec<ScrapedPage>> {
    let sql = format!(
        "SELECT pd.id, pd.slug, pd.url, pd.markdown
         FROM page_data pd
         LEFT JOIN companies c ON c.slug = pd.slug
         WHERE pd.markdown IS NOT NULL AND c.slug IS NULL
         ORDER BY pd.id{}",
        match limit {
            Some(n) => format!(" LIMIT {}", n),
            None => String::new(),
        }
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ScrapedPage {
                page_data_id: row.get(0)?,
                slug: row.get(1)?,
                url: row.get(2)?,
                markdown: row.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub struct SectionRow {
    pub page_data_id: i64,
    pub slug: String,
    pub url: String,
    pub navbar: Option<String>,
    pub header: Option<String>,
    pub description: Option<String>,
    pub news: Option<String>,
    pub jobs: Option<String>,
    pub footer: Option<String>,
    pub founders_raw: Option<String>,
    pub launches: Option<String>,
    pub extras: Option<String>,
}

pub fn save_sections(conn: &Connection, rows: &[SectionRow]) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO company_sections
             (page_id, slug, url, navbar, header, description, news, jobs, footer, founders_raw, launches, extras)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        )?;
        for r in rows {
            stmt.execute(rusqlite::params![
                r.page_data_id, r.slug, r.url, r.navbar, r.header, r.description,
                r.news, r.jobs, r.footer, r.founders_raw, r.launches, r.extras,
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

// ── Extracted data ──

pub struct CompanyRow {
    pub slug: String,
    pub url: String,
    pub name: Option<String>,
    pub tagline: Option<String>,
    pub batch: Option<String>,
    pub batch_season: Option<String>,
    pub batch_year: Option<i32>,
    pub status: Option<String>,
    pub homepage: Option<String>,
    pub founded_year: Option<i32>,
    pub team_size: Option<i32>,
    pub location: Option<String>,
    pub primary_partner: Option<String>,
    pub tags: Option<String>,
    pub job_count: i32,
    pub linkedin: Option<String>,
    pub twitter: Option<String>,
    pub facebook: Option<String>,
    pub crunchbase: Option<String>,
    pub github: Option<String>,
}

pub struct FounderRow {
    pub company_slug: String,
    pub name: String,
    pub title: Option<String>,
    pub bio: Option<String>,
    pub is_active: bool,
    pub linkedin: Option<String>,
    pub twitter: Option<String>,
}

pub struct NewsRow {
    pub company_slug: String,
    pub title: String,
    pub url: String,
    pub published: Option<String>,
}

pub struct JobRow {
    pub company_slug: String,
    pub title: String,
    pub url: String,
    pub location: Option<String>,
    pub salary: Option<String>,
    pub experience: Option<String>,
    pub apply_url: Option<String>,
}

pub struct LinkRow {
    pub company_slug: String,
    pub url: String,
    pub domain: String,
    pub link_type: Option<String>,
}

pub fn save_extracted(
    conn: &Connection,
    companies: &[CompanyRow],
    founders: &[FounderRow],
    news: &[NewsRow],
    jobs: &[JobRow],
    links: &[LinkRow],
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    {
        let mut c_stmt = tx.prepare(
            "INSERT OR REPLACE INTO companies
             (slug, url, name, tagline, batch, batch_season, batch_year, status,
              homepage, founded_year, team_size, location, primary_partner, tags,
              job_count, linkedin, twitter, facebook, crunchbase, github)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
        )?;
        for c in companies {
            c_stmt.execute(rusqlite::params![
                c.slug, c.url, c.name, c.tagline, c.batch, c.batch_season, c.batch_year,
                c.status, c.homepage, c.founded_year, c.team_size, c.location,
                c.primary_partner, c.tags, c.job_count, c.linkedin, c.twitter,
                c.facebook, c.crunchbase, c.github,
            ])?;
        }

        let mut f_stmt = tx.prepare(
            "INSERT OR IGNORE INTO founders
             (company_slug, name, title, bio, is_active, linkedin, twitter)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for f in founders {
            f_stmt.execute(rusqlite::params![
                f.company_slug, f.name, f.title, f.bio, f.is_active, f.linkedin, f.twitter,
            ])?;
        }

        let mut n_stmt = tx.prepare(
            "INSERT OR IGNORE INTO news (company_slug, title, url, published)
             VALUES (?1, ?2, ?3, ?4)",
        )?;
        for n in news {
            n_stmt.execute(rusqlite::params![n.company_slug, n.title, n.url, n.published])?;
        }

        let mut j_stmt = tx.prepare(
            "INSERT OR IGNORE INTO company_jobs
             (company_slug, title, url, location, salary, experience, apply_url)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for j in jobs {
            j_stmt.execute(rusqlite::params![
                j.company_slug, j.title, j.url, j.location, j.salary, j.experience, j.apply_url,
            ])?;
        }

        let mut l_stmt = tx.prepare(
            "INSERT OR IGNORE INTO company_links (company_slug, url, domain, link_type)
             VALUES (?1, ?2, ?3, ?4)",
        )?;
        for l in links {
            l_stmt.execute(rusqlite::params![l.company_slug, l.url, l.domain, l.link_type])?;
        }
    }
    tx.commit()?;
    Ok(())
}

// ── Meeting links ──

pub struct MeetingLinkRow {
    pub company_slug: String,
    pub url: String,
    pub domain: String,
    pub link_type: String, // "calendly", "cal.com", "motion", "hubspot", "other"
}

pub fn save_meeting_links(conn: &Connection, rows: &[MeetingLinkRow]) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR IGNORE INTO meeting_links (company_slug, url, domain, link_type)
             VALUES (?1, ?2, ?3, ?4)",
        )?;
        for r in rows {
            stmt.execute(rusqlite::params![r.company_slug, r.url, r.domain, r.link_type])?;
        }
    }
    tx.commit()?;
    Ok(())
}

// ── Partners ──

pub struct PartnerRow {
    pub slug: String,
    pub url: String,
    pub name: String,
    pub title: Option<String>,
    pub bio: Option<String>,
}

pub struct CompanyPartnerRow {
    pub company_slug: String,
    pub partner_slug: String,
    pub match_method: String, // "url" or "name"
}

pub fn save_partners(conn: &Connection, rows: &[PartnerRow]) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;
    let mut count = 0;
    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO partners (slug, url, name, title, bio)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for r in rows {
            count += stmt.execute(rusqlite::params![r.slug, r.url, r.name, r.title, r.bio])?;
        }
    }
    tx.commit()?;
    Ok(count)
}

pub fn fetch_partners(conn: &Connection) -> Result<Vec<PartnerRow>> {
    let mut stmt = conn.prepare("SELECT slug, url, name, title, bio FROM partners")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(PartnerRow {
                slug: row.get(0)?,
                url: row.get(1)?,
                name: row.get(2)?,
                title: row.get(3)?,
                bio: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

pub fn save_company_partners(conn: &Connection, rows: &[CompanyPartnerRow]) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;
    let mut count = 0;
    {
        let mut stmt = tx.prepare(
            "INSERT OR IGNORE INTO company_partners (company_slug, partner_slug, match_method)
             VALUES (?1, ?2, ?3)",
        )?;
        for r in rows {
            count += stmt.execute(rusqlite::params![
                r.company_slug, r.partner_slug, r.match_method
            ])?;
        }
    }
    tx.commit()?;
    Ok(count)
}

/// Fetch company slugs + their raw markdown for partner URL matching.
pub fn fetch_scraped_markdown(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT pd.slug, pd.markdown
         FROM page_data pd
         WHERE pd.markdown IS NOT NULL",
    )?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Fetch companies with primary_partner set but no entry in company_partners yet.
pub fn fetch_unmatched_partners(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT c.slug, c.primary_partner
         FROM companies c
         WHERE c.primary_partner IS NOT NULL
           AND c.primary_partner != ''
           AND NOT EXISTS (
               SELECT 1 FROM company_partners cp WHERE cp.company_slug = c.slug
           )",
    )?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

// ── Overview ──

pub struct OverviewRow {
    pub slug: String,
    pub name: String,
    pub batch: String,
    pub status: String,
    pub team_size: Option<i32>,
    pub location: String,
    pub primary_partner: String,
    pub tags: String,
    pub job_count: i32,
}

pub fn fetch_overview(
    conn: &Connection,
    status: Option<&str>,
    batch: Option<&str>,
    limit: usize,
) -> Result<Vec<OverviewRow>> {
    let mut conditions = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(s) = status {
        conditions.push(format!("status = ?{}", params.len() + 1));
        params.push(Box::new(s.to_string()));
    }
    if let Some(b) = batch {
        conditions.push(format!("batch = ?{}", params.len() + 1));
        params.push(Box::new(b.to_string()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let sql = format!(
        "SELECT slug, COALESCE(name,''), COALESCE(batch,''), COALESCE(status,''),
                team_size, COALESCE(location,''), COALESCE(primary_partner,''),
                COALESCE(tags,''), job_count
         FROM companies{}
         ORDER BY batch_year DESC, slug
         LIMIT {}",
        where_clause, limit
    );

    let mut stmt = conn.prepare(&sql)?;
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let rows = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok(OverviewRow {
                slug: row.get(0)?,
                name: row.get(1)?,
                batch: row.get(2)?,
                status: row.get(3)?,
                team_size: row.get(4)?,
                location: row.get(5)?,
                primary_partner: row.get(6)?,
                tags: row.get(7)?,
                job_count: row.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

// ── Stats ──

pub struct Stats {
    pub total: usize,
    pub visited: usize,
    pub unvisited: usize,
    pub scraped: usize,
    pub errors: usize,
    pub processed: usize,
}

pub fn get_stats(conn: &Connection) -> Result<Stats> {
    let total: usize = conn.query_row("SELECT COUNT(*) FROM pages", [], |r| r.get(0))?;
    let visited: usize =
        conn.query_row("SELECT COUNT(*) FROM pages WHERE visited = 1", [], |r| r.get(0))?;
    let scraped: usize = conn.query_row("SELECT COUNT(*) FROM page_data", [], |r| r.get(0))?;
    let errors: usize = conn.query_row(
        "SELECT COUNT(*) FROM page_data WHERE error IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    let processed: usize =
        conn.query_row("SELECT COUNT(*) FROM companies", [], |r| r.get(0))?;
    Ok(Stats {
        total,
        visited,
        unvisited: total - visited,
        scraped,
        errors,
        processed,
    })
}
