mod db;
mod parser;
mod scraper;
mod sitemap;

use std::time::Instant;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "yc_scraper", about = "YC company scraper via spider.cloud")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Fetch sitemap and populate URL queue
    Init,
    /// Scrape unvisited pages via spider.cloud
    Scrape {
        /// Max pages to scrape (default: all unvisited)
        #[arg(short = 'n', long)]
        limit: Option<usize>,
    },
    /// Split scraped markdown into sections
    Process {
        /// Max pages to process (default: all unprocessed)
        #[arg(short = 'n', long)]
        limit: Option<usize>,
    },
    /// Scrape + process in one pipeline (each page processed immediately after scraping)
    Run {
        /// Max pages to scrape+process
        #[arg(short = 'n', long)]
        limit: Option<usize>,
    },
    /// Show scraping statistics
    Stats,
    /// Companies overview table
    Overview {
        /// Filter by status (Active, Public, Acquired, Inactive)
        #[arg(short, long)]
        status: Option<String>,
        /// Filter by batch (e.g. "Winter 2024")
        #[arg(short, long)]
        batch: Option<String>,
        /// Max rows to display
        #[arg(short = 'n', long, default_value = "50")]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let t0 = Instant::now();
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Init => {
            let conn = db::connect()?;
            db::init_schema(&conn)?;
            let pages = sitemap::fetch_company_urls().await?;
            let inserted = db::insert_pages(&conn, &pages)?;
            println!("Inserted {} new company URLs ({} total found)", inserted, pages.len());
            Ok(())
        }
        Commands::Scrape { limit } => {
            let conn = db::connect()?;
            db::init_schema(&conn)?;
            let pages = db::fetch_unvisited(&conn, limit)?;
            if pages.is_empty() {
                println!("No unvisited pages. Run 'init' first or all pages are scraped.");
                return Ok(());
            }
            println!("Scraping {} pages (streaming to DB)...", pages.len());
            let stats = scraper::scrape_pages_streaming(&conn, pages).await?;
            println!(
                "Done: {} scraped ({} ok, {} errors).",
                stats.total, stats.ok, stats.errors
            );
            Ok(())
        }
        Commands::Process { limit } => {
            let conn = db::connect()?;
            db::init_schema(&conn)?;
            let pages = db::fetch_unprocessed(&conn, limit)?;
            if pages.is_empty() {
                println!("No unprocessed pages. Run 'scrape' first.");
                return Ok(());
            }
            println!("Processing {} pages...", pages.len());
            let counts = process_pages(&conn, &pages)?;
            counts.print();
            Ok(())
        }
        Commands::Run { limit } => {
            let conn = db::connect()?;
            db::init_schema(&conn)?;
            let pages = db::fetch_unvisited(&conn, limit)?;
            if pages.is_empty() {
                println!("No unvisited pages. Run 'init' first.");
                return Ok(());
            }

            // Phase 1: Scrape (streaming to DB)
            let t_scrape = Instant::now();
            println!("Pipeline: scraping {} pages (streaming to DB)...", pages.len());
            let stats = scraper::scrape_pages_streaming(&conn, pages).await?;
            println!(
                "Scraped {} pages ({} ok, {} errors) in {:.1}s",
                stats.total, stats.ok, stats.errors, t_scrape.elapsed().as_secs_f64()
            );

            // Phase 2: Process
            let t_process = Instant::now();
            let unprocessed = db::fetch_unprocessed(&conn, None)?;
            if unprocessed.is_empty() {
                println!("Nothing to process (all scraped pages had errors).");
                return Ok(());
            }
            println!("Processing {} pages...", unprocessed.len());
            let counts = process_pages(&conn, &unprocessed)?;
            println!(
                "Processed in {:.1}s",
                t_process.elapsed().as_secs_f64()
            );
            counts.print();
            Ok(())
        }
        Commands::Overview { status, batch, limit } => {
            let conn = db::connect()?;
            db::init_schema(&conn)?;
            let rows = db::fetch_overview(
                &conn,
                status.as_deref(),
                batch.as_deref(),
                limit,
            )?;
            if rows.is_empty() {
                println!("No companies found.");
                return Ok(());
            }

            // Compact, readable table
            println!(
                "{:>3} | {:<24} | {:<12} | {:<8} | {:>5} | {:<20} | {:<16} | {:>4}",
                "#", "Company", "Batch", "Status", "Size", "Location", "Partner", "Jobs"
            );
            println!("{}", "-".repeat(105));

            for (i, r) in rows.iter().enumerate() {
                let name = truncate(&r.name, 24);
                let loc = truncate(&r.location, 20);
                let partner = truncate(&r.primary_partner, 16);
                let size = r.team_size.map(|s| s.to_string()).unwrap_or_else(|| "-".into());

                println!(
                    "{:>3} | {:<24} | {:<12} | {:<8} | {:>5} | {:<20} | {:<16} | {:>4}",
                    i + 1, name, r.batch, r.status, size, loc, partner, r.job_count
                );
            }

            // Tags summary (separate section to avoid clutter)
            let with_tags: Vec<_> = rows.iter().filter(|r| !r.tags.is_empty()).collect();
            if !with_tags.is_empty() {
                println!("\n--- Tags ---");
                for r in &with_tags {
                    println!("  {}: {}", truncate(&r.slug, 24), r.tags);
                }
            }

            println!("\n{} companies | slug: /companies/<slug>", rows.len());
            Ok(())
        }
        Commands::Stats => {
            let conn = db::connect()?;
            db::init_schema(&conn)?;
            let s = db::get_stats(&conn)?;
            println!("Total:     {}", s.total);
            println!("Visited:   {}", s.visited);
            println!("Unvisited: {}", s.unvisited);
            println!("Scraped:   {}", s.scraped);
            println!("Errors:    {}", s.errors);
            println!("Processed: {}", s.processed);
            Ok(())
        }
    };

    let elapsed = t0.elapsed();
    if elapsed.as_secs() >= 1 {
        println!("\nDone in {}", format_duration(elapsed));
    }

    result
}

struct ProcessCounts {
    companies: usize,
    founders: usize,
    news: usize,
    jobs: usize,
    links: usize,
}

impl ProcessCounts {
    fn print(&self) {
        println!(
            "Saved {} companies, {} founders, {} news, {} jobs, {} links.",
            self.companies, self.founders, self.news, self.jobs, self.links,
        );
    }
}

fn process_pages(
    conn: &rusqlite::Connection,
    pages: &[db::ScrapedPage],
) -> anyhow::Result<ProcessCounts> {
    use indicatif::{ProgressBar, ProgressStyle};
    use rayon::prelude::*;

    let pb = ProgressBar::new(pages.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let mut counts = ProcessCounts {
        companies: 0,
        founders: 0,
        news: 0,
        jobs: 0,
        links: 0,
    };

    for chunk in pages.chunks(500) {
        let results: Vec<_> = chunk.par_iter().map(parser::process_page).collect();

        let mut sections = Vec::new();
        let mut companies = Vec::new();
        let mut founders = Vec::new();
        let mut news = Vec::new();
        let mut jobs = Vec::new();
        let mut links = Vec::new();
        let mut meeting_links = Vec::new();

        for data in results {
            sections.push(data.sections);
            companies.push(data.company);
            counts.founders += data.founders.len();
            counts.news += data.news.len();
            counts.jobs += data.jobs.len();
            counts.links += data.links.len();
            founders.extend(data.founders);
            news.extend(data.news);
            jobs.extend(data.jobs);
            links.extend(data.links);
            meeting_links.extend(data.meeting_links);
        }

        counts.companies += companies.len();
        db::save_sections(conn, &sections)?;
        db::save_extracted(conn, &companies, &founders, &news, &jobs, &links)?;
        db::save_meeting_links(conn, &meeting_links)?;
        pb.inc(chunk.len() as u64);
    }

    pb.finish_and_clear();
    Ok(counts)
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max).collect();
        format!("{}...", truncated)
    }
}

fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{:.1}s", d.as_secs_f64())
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m {}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    }
}
