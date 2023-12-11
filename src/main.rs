use std::error::Error;
use std::fs::File;

use csv::Reader;
use rusqlite::{params, Connection, Result};

fn main() -> Result<(), Box<dyn Error>> {
  std::fs::remove_file("reddit.sqlite").ok();
  let conn = Connection::open("reddit.sqlite")?;

  conn.execute_batch("
    PRAGMA journal_mode = OFF;
    PRAGMA synchronous = 0;
    PRAGMA cache_size = 1000000;
    PRAGMA locking_mode = EXCLUSIVE;
    PRAGMA temp_store = MEMORY;
  ")?;
  conn.execute(
    "CREATE TABLE comments (
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
    (),
  )?;

  // Create a CSV reader from the file
  let file = File::open("RC_2015-01.csv")?;

  let mut rdr = Reader::from_reader(file);

  let start = std::time::Instant::now();
  let mut i = 0;

  let mut insert_statement = conn.prepare(
    "INSERT INTO comments (
      id,
      author,
      author_flair_css_class,
      author_flair_text,
      body,
      can_gild,
      controversiality,
      created_utc,
      distinguished,
      edited,
      gilded,
      is_submitter,
      link_id,
      parent_id,
      permalink,
      retrieved_on,
      score,
      stickied,
      subreddit,
      subreddit_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
  )?;

  conn.execute("BEGIN TRANSACTION", ())?;

  for result in rdr.records() {
    let record = result?;

    insert_statement.execute(params![
      record.get(0),
      record.get(1),
      record.get(2),
      record.get(3),
      record.get(4),
      record.get(5),
      record.get(6),
      record.get(7),
      record.get(8),
      record.get(9),
      record.get(10),
      record.get(11),
      record.get(12),
      record.get(13),
      record.get(14),
      record.get(15),
      record.get(16),
      record.get(17),
      record.get(18),
      record.get(19),
    ])?;

    i += 1;

    if i % 10000 == 0 {
      conn.execute("COMMIT", ())?;
      conn.execute("BEGIN TRANSACTION", ())?;
      let duration = start.elapsed();
      let secs = duration.as_secs_f64();
      if secs > 0.0 {
        println!("{} records inserted per second", i as f64 / secs);
      }
    }

    if i == 50000000 {
      conn.execute("COMMIT", ())?;
      break;
    }
  }

  Ok(())
}