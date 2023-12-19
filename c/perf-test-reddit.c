#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "sqlite-amalgamation-3440200/sqlite3.h"

// gcc -O3 -o perf-test perf-test-reddit.c sqlite-amalgamation-3440200/sqlite3.o && ./perf-test

// Results: ~620k rows + ~850MB / second

int main(int argc, char** argv) {
  const int records_per_statement = 500;

  // Delete reddit.sqlite if exists
  remove("perf-test.sqlite");

  sqlite3 *db;
  int sqlite3_open_result = sqlite3_open("perf-test.sqlite", &db);

  if (sqlite3_open_result != SQLITE_OK) {
    fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }

  // Optimize PRAGMA
  sqlite3_exec(db, "PRAGMA journal_mode = OFF", NULL, NULL, NULL);
  sqlite3_exec(db, "PRAGMA synchronous = 0", NULL, NULL, NULL);
  sqlite3_exec(db, "PRAGMA temp_store = MEMORY", NULL, NULL, NULL);
  sqlite3_exec(db, "PRAGMA cache_size = 100000", NULL, NULL, NULL);

  // Create a minimal table
  char *create_table_sql = "CREATE TABLE comments ("
                            "id INTEGER PRIMARY KEY, "
                            "author TEXT NOT NULL, "
                            "author_flair_css_class TEXT, "
                            "author_flair_text TEXT, "
                            "body TEXT NOT NULL, "
                            "can_gild INTEGER NOT NULL, "
                            "controversiality INTEGER NOT NULL, "
                            "created_utc INTEGER NOT NULL, "
                            "distinguished TEXT, "
                            "edited INTEGER, "
                            "gilded INTEGER, "
                            "is_submitter INTEGER NOT NULL, "
                            "link_id TEXT, "
                            "parent_id TEXT, "
                            "permalink TEXT, "
                            "retrieved_on INTEGER, "
                            "score INTEGER NOT NULL, "
                            "stickied INTEGER NOT NULL, "
                            "subreddit TEXT NOT NULL, "
                            "subreddit_id TEXT NOT NULL"
                            ") STRICT;";
  char *err_msg = 0;
  sqlite3_exec(db, create_table_sql, 0, 0, &err_msg);

  char sql[32768] = "INSERT INTO comments VALUES";

  for(int i = 0; i < records_per_statement - 1; i++) {
    strcat(sql, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),");
  }

  strcat(sql, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);

  time_t start = time(NULL);

  // lorem ipsum body
  char *lorem_ipsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec euismod, "
                      "nunc id aliquam ultricies, diam magna vulputate enim, eget aliquet "
                      "nisl augue quis nunc. Nulla facilisi. Nulla facilisi. Nulla facilisi. "
                      "Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi. Nulla facilisi. Nulla facilisi. Nulla facilisi. Nulla "
                      "facilisi.";

  int i_per_one_hundred_thousand_rows = 100000 / records_per_statement;
  int iterations_for_ten_million_rows = 10000000 / records_per_statement;

  printf("inserting 10m rows...\n");

  for (int i = 0; i < iterations_for_ten_million_rows; i++) {
    for(int j = 0; j < records_per_statement; j++) {
      // bind unique id
      sqlite3_bind_int(stmt, j * 20 + 1, i * records_per_statement + j); // id
      sqlite3_bind_text(stmt, j * 20 + 2, "david", -1, SQLITE_STATIC); // author
      sqlite3_bind_text(stmt, j * 20 + 3, "david", -1, SQLITE_STATIC); // author_flair_css_class
      sqlite3_bind_text(stmt, j * 20 + 4, "hello", -1, SQLITE_STATIC); // author_flair_text
      sqlite3_bind_text(stmt, j * 20 + 5, lorem_ipsum, -1, SQLITE_STATIC); // body
      sqlite3_bind_int(stmt, j * 20 + 6, 1); // can_gild
      sqlite3_bind_int(stmt, j * 20 + 7, 1); // controversiality
      sqlite3_bind_int(stmt, j * 20 + 8, i); // created_utc
      sqlite3_bind_text(stmt, j * 20 + 9, "", -1, SQLITE_STATIC); // distinguished
      sqlite3_bind_int(stmt, j * 20 + 10, 0); // edited
      sqlite3_bind_int(stmt, j * 20 + 11, 0); // gilded
      sqlite3_bind_int(stmt, j * 20 + 12, 1); // is_submitter
      sqlite3_bind_text(stmt, j * 20 + 13, "link_id", -1, SQLITE_STATIC); // link_id
      sqlite3_bind_text(stmt, j * 20 + 14, "parent_id", -1, SQLITE_STATIC); // parent_id
      sqlite3_bind_text(stmt, j * 20 + 15, "permalink", -1, SQLITE_STATIC); // permalink
      sqlite3_bind_int(stmt, j * 20 + 16, i); // retrieved_on
      sqlite3_bind_int(stmt, j * 20 + 17, 10); // score
      sqlite3_bind_int(stmt, j * 20 + 18, 1); // stickied
      sqlite3_bind_text(stmt, j * 20 + 19, "subreddit", -1, SQLITE_STATIC); // subreddit
      sqlite3_bind_text(stmt, j * 20 + 20, "subreddit_id", -1, SQLITE_STATIC); // subreddit_id
    }

    // print SQL statement
    // printf("%s\n", sqlite3_expanded_sql(stmt));

    int result = sqlite3_step(stmt);
    if (result != SQLITE_DONE) {
      fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return 1;
    }
    sqlite3_reset(stmt);

    // report inserts per second every 100,000 rows
    if (i % i_per_one_hundred_thousand_rows == 0) {
      int rows_inserted = (i + 1) * records_per_statement;
      time_t now = time(NULL);
      double elapsed = difftime(now, start);
      if (elapsed > 1) {
          double ips = rows_inserted / elapsed;
          printf("progress: %d (%.2f ips)\n", i, ips);
      }
    }
  }

  sqlite3_finalize(stmt);
  sqlite3_close(db);

  // report total inserts per second
  int rows_inserted = iterations_for_ten_million_rows * records_per_statement;
  time_t now = time(NULL);
  double elapsed = difftime(now, start);
  double ips = rows_inserted / elapsed;
  printf("total: %d (%.2f ips)\n", rows_inserted, ips);

  // size of database
  FILE *fp = fopen("perf-test.sqlite", "r");
  fseek(fp, 0L, SEEK_END);
  long int res = ftell(fp);
  fclose(fp);

  // report total size of DB in gigabytes
  double gb = res / 1000000000.0;
  printf("size: %.2fGB\n", gb);

  // report megabytes written per second
  double mbps = res / elapsed / 1000000;
  printf("mbps: %.2fMB\n", mbps);

  return 0;
}