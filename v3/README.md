# v3 — YC Scraper & Parser

Pure Rust pipeline. Scrapes every Y Combinator company page via [spider.cloud](https://spider.cloud), parses markdown into structured data, stores it in SQLite.

**5,723 companies. 11 minutes to scrape. 8 seconds to parse. Zero errors.**

## How It Works

```
YC Sitemap ──→ URL Queue ──→ spider.cloud ──→ Markdown ──→ 3-Pass Parser ──→ SQLite
  (XML)         5,723 urls    10 concurrent    streaming     Lexer            9 tables
                              retry+backoff    to DB         Clusterer        68 MB
                              11m 23s                        Extractors
                                                             8.2s / 18 cores
```

### The 3-Pass Parser

The parser turns raw markdown into structured database rows in three stages:

**Pass 1 — Lexer** (`blocks.rs`). Every line becomes a typed block:

```
"### Stripe"                    → Heading { level: 3, text: "Stripe" }
"[Fintech](/companies/industry/Fintech)" → TagLink { tag: "Fintech" }
"Patrick Collison"              → Person { name, title, bio, links }
"Founded:2009"                  → MetaField { key: "Founded", value: "2009" }
"Active"                        → StatusLine("Active")
```

Person detection uses word-count heuristics (<=6 words), bare social link patterns (`[](url)`), and title keywords ("Founder", "CEO", "CTO"). Compiles 8 regex patterns once via `LazyLock` and shares them across threads.

**Pass 2 — Clusterer** (`sections.rs`). Groups blocks into named sections by structural transitions:

```
header → description → founders → news → jobs → footer_meta → launches
```

Transition detection: `###` heading starts description, 3+ consecutive MetaFields start footer_meta, first Person block starts founders, external link followed by a date starts news, `/jobs/` URL starts jobs.

**Pass 3 — Extractors** (`extract/*.rs`). One extractor per section type. Each pulls structured fields into DB rows — company info, founders with LinkedIn/Twitter, news with dates, job listings, meeting/scheduling links across 18 platforms.

### The Scraper

Async tokio runtime with semaphore-bounded concurrency (10 concurrent). Each result streams to SQLite via `mpsc` channel the moment it arrives — no batch buffering. Retry with exponential backoff (2s → 4s → 8s) on 429/5xx errors.

### Processing

Rayon `par_iter` in chunks of 500. On 18 cores, processes 5,583 pages in 8.2 seconds (~681 pages/sec). Wall time 8.2s vs CPU time 1m52s = ~14x parallelism efficiency.

## Commands

```bash
export SPIDER_API_KEY="..."

cargo run -- init              # Fetch YC sitemap → URL queue
cargo run -- scrape            # Scrape all unvisited (streams to DB)
cargo run -- scrape -n 50      # Scrape 50 pages
cargo run -- process           # Parse all unprocessed markdown
cargo run -- run               # Scrape + process in one pipeline
cargo run -- run -n 100        # Pipeline 100 pages
cargo run -- overview          # Company table
cargo run -- overview --status Active --batch "Winter 2024" -n 20
cargo run -- stats             # Pipeline progress counters
```

## Schema

| Table | What | Rows |
|-------|------|------|
| `pages` | URL queue | 5,723 |
| `page_data` | Raw markdown + HTTP status + latency | 5,723 |
| `company_sections` | Parsed sections per company | 5,723 |
| `companies` | Structured company data | 5,723 |
| `founders` | Name, title, bio, LinkedIn, Twitter | 11,286 |
| `news` | Articles with publication dates | 4,241 |
| `company_jobs` | Job listings with location/salary | 3,470 |
| `company_links` | External links with domain classification | 42,779 |
| `meeting_links` | Calendly, Cal.com, Motion, HubSpot links | 441 |

## Dependencies

| Crate | Purpose |
|-------|---------|
| `spider-client` | spider.cloud API client |
| `tokio` | Async runtime for concurrent scraping |
| `rayon` | Data-parallel parsing across cores |
| `clap` | CLI argument parsing with derive macros |
| `rusqlite` | SQLite with bundled `libsqlite3` |
| `reqwest` | HTTP client (sitemap fetch) |
| `quick-xml` | XML parsing for YC sitemap |
| `regex` | Pattern matching in lexer + extractors |
| `serde` / `serde_json` | JSON deserialization of spider.cloud responses |
| `tracing` | Structured logging |
| `indicatif` | Progress bars for scrape/process |
| `chrono` | Date parsing |
| `anyhow` | Error handling |

## Project Structure

```
src/
├── main.rs                 CLI + pipeline orchestration + Rayon processing
├── db.rs                   Schema (9 tables), all queries, transactional writes
├── sitemap.rs              Sitemap fetch + XML parse + URL filtering
├── scraper.rs              spider.cloud client, mpsc streaming, retry/backoff
└── parser/
    ├── blocks.rs           Pass 1: line lexer (8 LazyLock regex, person detection)
    ├── sections.rs         Pass 2: structural section clustering
    └── extract/
        ├── mod.rs          Pass 3: orchestrator
        ├── company.rs      Name, batch, status, homepage, social links
        ├── founders.rs     Name, title, bio, LinkedIn, Twitter
        ├── news.rs         Articles with dates
        ├── jobs.rs         Listings with location/salary
        ├── links.rs        External links + domain classification
        └── meetings.rs     18 scheduling platform domains
```

## Performance

| Phase | Wall Time | CPU Time | Throughput |
|-------|-----------|----------|------------|
| Scrape | 11m 23s | — | 8.2 pages/sec (10 concurrent) |
| Process | 8.2s | 1m 52s | 681 pages/sec (18 cores, 14x parallel) |

See [`stats.md`](stats.md) for a full breakdown of the dataset.
