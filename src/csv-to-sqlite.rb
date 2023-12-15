require "bundler/inline"

ROWS_PER_INSERT = 100
EXECUTIONS_PER_TRANSACTION = 1

# NOTES:
# Extralite is about 2x faster than sqlite3 gem
# FastestCSV is about 2x faster than Rcsv but failed to correctly parse the file

gemfile do
  source "https://rubygems.org"
  gem "extralite"
  gem "rcsv"
  gem "sqlite3"
end

File.delete("reddit.sqlite") if File.exist?("reddit.sqlite")

db = Extralite::Database.new("reddit.sqlite")
# db = SQLite3::Database.new("reddit.sqlite")

db.execute "PRAGMA journal_mode = OFF"
db.execute "PRAGMA synchronous = 0"
db.execute "PRAGMA cache_size = 100000"
# db.execute "PRAGMA locking_mode = EXCLUSIVE"
db.execute "PRAGMA temp_store = MEMORY"
# db.execute "PRAGMA count_changes = OFF"

db.execute <<-SQL
CREATE TABLE comments (
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
    permalink TEXT,
    retrieved_on INTEGER,
    score INTEGER NOT NULL,
    stickied INTEGER NOT NULL,
    subreddit TEXT NOT NULL,
    subreddit_id TEXT NOT NULL
) STRICT
SQL

values = ROWS_PER_INSERT.times.map { "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" }.join(", ")

statement = db.prepare <<-SQL
  INSERT INTO comments
  VALUES #{values}
SQL

start = Time.now
values = []
i = 0

db.execute("BEGIN TRANSACTION") if EXECUTIONS_PER_TRANSACTION > 1

some_csv_file = File.open('RC_2015-01.csv')
Rcsv.parse(some_csv_file, buffer_size: 108 * 1024 * 1024) do |row|
  i += 1

  values.concat(row)

  if i % ROWS_PER_INSERT == 0
    statement.execute(*values)
    values = []
  end

  if EXECUTIONS_PER_TRANSACTION > 1 && i % EXECUTIONS_PER_TRANSACTION
    db.execute("COMMIT TRANSACTION")
    db.execute("BEGIN TRANSACTION")
  end

  if i % 200_000 == 0
    puts "#{i} rows: #{i / (Time.now - start)} rps"
  end

  if i == 20_000_000
    db.execute("COMMIT TRANSACTION") if EXECUTIONS_PER_TRANSACTION > 1
    break
  end
end
