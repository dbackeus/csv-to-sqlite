require "csv"
require "sqlite3"

File.delete("reddit.sqlite") if File.exists?("reddit.sqlite")

def int(value)
  value == "" ? nil : value.to_i
end

File.open("RC_2015-01.csv") do |file|
  csv = CSV.new(file)

  DB.open "sqlite3://./reddit.sqlite" do |db|
    db.exec "PRAGMA cache_size = 1073741824"
    db.exec "PRAGMA journal_mode = OFF"
    db.exec "PRAGMA locking_mode = EXCLUSIVE"
    db.exec "PRAGMA mmap_size = 2147418112"
    # db.exec "PRAGMA page_size = 16384"
    db.exec "PRAGMA synchronous = OFF"
    db.exec "PRAGMA temp_store = MEMORY"
    db.exec "PRAGMA threads = 8"

    db.exec(
      <<-SQL
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
          permalink TEXT NOT NULL,
          retrieved_on INTEGER,
          score INTEGER NOT NULL,
          stickied INTEGER NOT NULL,
          subreddit TEXT NOT NULL,
          subreddit_id TEXT NOT NULL
        ) STRICT
      SQL
    )

    statement = db.build <<-SQL
      INSERT INTO comments (
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
      ) VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    SQL

    start = Time.monotonic

    executions_per_transaction = 100
    inserts_per_execution = 5
    rows_per_transaction = executions_per_transaction * inserts_per_execution

    50000.times do |i|
      db.exec "BEGIN TRANSACTION"

      executions_per_transaction.times do
        values = Array.new(inserts_per_execution) do
          csv.next
          csv.row.to_a
        end

        # Can this be done without repeating the values?
        statement.exec(
          values[0][0],
          values[0][1],
          values[0][2],
          values[0][3],
          values[0][4],
          values[0][5],
          int(values[0][6]),
          int(values[0][7]),
          values[0][8],
          values[0][9],
          int(values[0][10]),
          int(values[0][11]),
          values[0][12],
          values[0][13],
          values[0][14],
          values[0][15],
          int(values[0][16]),
          int(values[0][17]),
          values[0][18],
          values[0][19],

          values[1][0],
          values[1][1],
          values[1][2],
          values[1][3],
          values[1][4],
          values[1][5],
          int(values[1][6]),
          int(values[1][7]),
          values[1][8],
          values[1][9],
          int(values[1][10]),
          int(values[1][11]),
          values[1][12],
          values[1][13],
          values[1][14],
          values[1][15],
          int(values[1][16]),
          int(values[1][17]),
          values[1][18],
          values[1][19],

          values[2][0],
          values[2][1],
          values[2][2],
          values[2][3],
          values[2][4],
          values[2][5],
          int(values[2][6]),
          int(values[2][7]),
          values[2][8],
          values[2][9],
          int(values[2][10]),
          int(values[2][11]),
          values[2][12],
          values[2][13],
          values[2][14],
          values[2][15],
          int(values[2][16]),
          int(values[2][17]),
          values[2][18],
          values[2][19],

          values[3][0],
          values[3][1],
          values[3][2],
          values[3][3],
          values[3][4],
          values[3][5],
          int(values[3][6]),
          int(values[3][7]),
          values[3][8],
          values[3][9],
          int(values[3][10]),
          int(values[3][11]),
          values[3][12],
          values[3][13],
          values[3][14],
          values[3][15],
          int(values[3][16]),
          int(values[3][17]),
          values[3][18],
          values[3][19],

          values[4][0],
          values[4][1],
          values[4][2],
          values[4][3],
          values[4][4],
          values[4][5],
          int(values[4][6]),
          int(values[4][7]),
          values[4][8],
          values[4][9],
          int(values[4][10]),
          int(values[4][11]),
          values[4][12],
          values[4][13],
          values[4][14],
          values[4][15],
          int(values[4][16]),
          int(values[4][17]),
          values[4][18],
          values[4][19],
        )
      end

      db.exec "COMMIT TRANSACTION"

      if i % 1000 == 0
        inserts = i * rows_per_transaction
        ips = inserts / (Time.monotonic - start).to_f
        puts "IPS: #{ips}"
      end
    end
  end
end