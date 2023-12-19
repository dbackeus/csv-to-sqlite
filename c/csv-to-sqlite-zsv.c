#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <zsv.h> // make install on the zsv repo
#include "sqlite-amalgamation-3440200/sqlite3.h"
// #include <sqlite3.h> // apt install libsqlite3-dev

// gcc -O3 -o csv-to-sqlite-zsv -l zsv csv-to-sqlite-zsv.c sqlite-amalgamation-3440200/sqlite3.o  && ./csv-to-sqlite-zsv

// Results with 1 record per statement: ~570k rows + ~165MB / second

int main(int argc, char** argv) {
  const int records_per_statement = 1;
  const int statements_per_transaction = 1000;

  // Delete reddit.sqlite if exists
  remove("reddit.sqlite");

  // Initialize SQLite
  sqlite3 *db;
  int sqlite3_open_result = sqlite3_open("reddit.sqlite", &db);

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

  char *create_table_sql = "CREATE TABLE comments ("
                            "id TEXT PRIMARY KEY, "
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

  FILE *file = fopen("/root/csv-to-sqlite/RC_2015-01.csv", "rb");
  if(!file) { printf("error: could not open file\n"); return 1; }

  struct zsv_opts opts = { 0 };
  opts.stream = file;

  zsv_parser parser = zsv_new(&opts);
  if (parser == NULL) { printf("error: could not create parser\n"); return 1; }

  // sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);

  char *int_conversion_buffer = malloc(256);

  printf("Parsing CSV file...\n");

  time_t start = time(NULL);

  // Start transaction
  if(statements_per_transaction > 1) sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);

  int row_num = 0;
  size_t record_num = 0;
  while(1) {
    size_t bind_num = 1;

    for(int i = 0; i < records_per_statement; i++) {
      if(!zsv_next_row(parser) == zsv_status_row) {
        printf("error: could not get next row\n");
        return 1;
      }

      // id
      struct zsv_cell cell = zsv_get_cell(parser, 0);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // if(row_num == 1000) {
      //   printf("Got cell: %.*s\n", cell.len, cell.len ? cell.str : "");

      //   printf("%s\n", sqlite3_expanded_sql(stmt));
      //   return 1;
      // }

      // author
      cell = zsv_get_cell(parser, 1);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // author_flair_css_class
      cell = zsv_get_cell(parser, 2);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // author_flair_text
      cell = zsv_get_cell(parser, 3);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // body
      cell = zsv_get_cell(parser, 4);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // can_gild
      cell = zsv_get_cell(parser, 5);
      memcpy(int_conversion_buffer, cell.str, cell.len);
      int_conversion_buffer[cell.len] = '\0';
      sqlite3_bind_int(stmt, bind_num++, atoi(int_conversion_buffer));

      // controversiality
      cell = zsv_get_cell(parser, 6);
      memcpy(int_conversion_buffer, cell.str, cell.len);
      int_conversion_buffer[cell.len] = '\0';
      sqlite3_bind_int(stmt, bind_num++, atoi(int_conversion_buffer));

      // created_utc
      cell = zsv_get_cell(parser, 7);
      memcpy(int_conversion_buffer, cell.str, cell.len);
      int_conversion_buffer[cell.len] = '\0';
      sqlite3_bind_int(stmt, bind_num++, atoi(int_conversion_buffer));

      // distinguished
      cell = zsv_get_cell(parser, 8);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // edited
      cell = zsv_get_cell(parser, 9);
      memcpy(int_conversion_buffer, cell.str, cell.len);
      int_conversion_buffer[cell.len] = '\0';
      sqlite3_bind_int(stmt, bind_num++, atoi(int_conversion_buffer));

      // gilded
      cell = zsv_get_cell(parser, 10);
      memcpy(int_conversion_buffer, cell.str, cell.len);
      int_conversion_buffer[cell.len] = '\0';
      sqlite3_bind_int(stmt, bind_num++, atoi(int_conversion_buffer));

      // is_submitter
      cell = zsv_get_cell(parser, 11);
      memcpy(int_conversion_buffer, cell.str, cell.len);
      int_conversion_buffer[cell.len] = '\0';
      sqlite3_bind_int(stmt, bind_num++, atoi(int_conversion_buffer));

      // link_id
      cell = zsv_get_cell(parser, 12);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // parent_id
      cell = zsv_get_cell(parser, 13);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // permalink
      cell = zsv_get_cell(parser, 14);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // retrieved_on
      cell = zsv_get_cell(parser, 15);
      memcpy(int_conversion_buffer, cell.str, cell.len);
      int_conversion_buffer[cell.len] = '\0';
      sqlite3_bind_int(stmt, bind_num++, atoi(int_conversion_buffer));

      // score
      cell = zsv_get_cell(parser, 16);
      memcpy(int_conversion_buffer, cell.str, cell.len);
      int_conversion_buffer[cell.len] = '\0';
      sqlite3_bind_int(stmt, bind_num++, atoi(int_conversion_buffer));

      // stickied
      cell = zsv_get_cell(parser, 17);
      memcpy(int_conversion_buffer, cell.str, cell.len);
      int_conversion_buffer[cell.len] = '\0';
      sqlite3_bind_int(stmt, bind_num++, atoi(int_conversion_buffer));

      // subreddit
      cell = zsv_get_cell(parser, 18);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      // subreddit_id
      cell = zsv_get_cell(parser, 19);
      sqlite3_bind_text(stmt, bind_num++, cell.str, cell.len, SQLITE_STATIC);

      row_num++;
    }

    int result = sqlite3_step(stmt);
    if(result != SQLITE_DONE) {
      printf("row: %d\n", row_num);
      // print SQL statement
      printf("%s\n", sqlite3_expanded_sql(stmt));

      fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return 1;
    }

    sqlite3_reset(stmt);
    // sqlite3_clear_bindings(stmt);

    // commit every 1000 rows
    if (statements_per_transaction > 1 && row_num % statements_per_transaction == 0) {
      sqlite3_exec(db, "COMMIT TRANSACTION", NULL, NULL, NULL);
      sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    }

     // report inserts per second every 100,000 rows
    if (row_num % 100000 == 0) {
      time_t now = time(NULL);
      double elapsed = difftime(now, start);
      if (elapsed > 1) {
          double ips = row_num / elapsed;
          printf("progress: %d (%.2f ips)\n", row_num, ips);
      }
    }
  }

  if(statements_per_transaction > 1) sqlite3_exec(db, "COMMIT TRANSACTION", NULL, NULL, NULL);

  sqlite3_close(db);

  zsv_delete(parser);

  fclose(file);

  return 0;
}
