use serde::Deserialize;
use csv::ReaderBuilder;
use rusqlite::{params, Connection, NO_PARAMS, Transaction};
use std::error::Error;

#[derive(Debug, Deserialize)]
struct Record {
    id: String,
    author: String,
    author_flair_css_class: Option<String>,
    author_flair_text: Option<String>,
    body: String,
    can_gild: i32,
    controversiality: i32,
    created_utc: i64,
    distinguished: Option<String>,
    edited: Option<i32>,
    gilded: Option<i32>,
    is_submitter: i32,
    link_id: Option<String>,
    parent_id: Option<String>,
    permalink: String,
    retrieved_on: Option<i64>,
    score: i32,
    stickied: i32,
    subreddit: String,
    subreddit_id: String,
}

fn main() {
    use csv::ReaderBuilder;
    use rusqlite::{params, Connection, NO_PARAMS};
    use std::error::Error;

    fn main() -> Result<(), Box<dyn Error>> {
        // Connect to SQLite database
        let conn = Connection::open("reddit.sqlite")?;

        // Create your table (if it doesn't exist)
        conn.execute_batch("
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = 0;
            PRAGMA cache_size = 1000000;
            PRAGMA locking_mode = EXCLUSIVE;
            PRAGMA temp_store = MEMORY;
        ");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS comments (
                id TEXT PRIMARY KEY,
                author TEXT NOT NULL,
                author_flair_css_class TEXT,
                author_flair_text TEXT,
                body TEXT NOT NULL,
                can_gild INTEGER NOT NULL,
                controversiality INTEGER NOT NULL,
                created_utc INTEGER NOT NULL,
                distinguished TEXT,
                edited INTEGER,
                gilded INTEGER,
                is_submitter INTEGER NOT NULL,
                link_id TEXT,
                parent_id TEXT,
                permalink TEXT NOT NULL,
                retrieved_on INTEGER,
                score INTEGER NOT NULL,
                stickied INTEGER NOT NULL,
                subreddit TEXT NOT NULL,
                subreddit_id TEXT NOT NULL
            ) STRICT",
            NO_PARAMS,
        )?;

        // Prepare CSV reader
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_path("RC_2015-01.csv")?;

        insert_in_batches(&conn, &mut rdr, 50)?;

        Ok(())
    }
}

// Function to insert records in batches
fn insert_in_batches<T: std::io::Read>(
  conn: &Connection,
  rdr: &mut csv::Reader<T>,
  batch_size: usize,
) -> Result<(), Box<dyn Error>> {
    let mut tx = conn.transaction()?;
    let mut stmt = tx.prepare(
        "INSERT INTO your_table (
            id, author, author_flair_css_class, author_flair_text, body,
            can_gild, controversiality, created_utc, distinguished, edited,
            gilded, is_submitter, link_id, parent_id, permalink,
            retrieved_on, score, stickied, subreddit, subreddit_id
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
    )?;
    let mut count = 0;
    for result in rdr.deserialize() {
        let record: Record = result?;

        stmt.execute(params![
            record.id,
            record.author,
            record.author_flair_css_class,
            record.author_flair_text,
            record.body,
            record.can_gild,
            record.controversiality,
            record.created_utc,
            record.distinguished,
            record.edited,
            record.gilded,
            record.is_submitter,
            record.link_id,
            record.parent_id,
            record.permalink,
            record.retrieved_on,
            record.score,
            record.stickied,
            record.subreddit,
            record.subreddit_id,
        ])?;

        count += 1;
        if count % batch_size == 0 {
            tx.commit()?;
            tx = conn.transaction()?;
        }
    }

    // Commit any remaining records
    if count % batch_size != 0 {
        tx.commit()?;
    }

  Ok(())
}