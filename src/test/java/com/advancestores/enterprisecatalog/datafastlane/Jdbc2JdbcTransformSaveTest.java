package com.advancestores.enterprisecatalog.datafastlane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.advancestores.enterprisecatalog.datafastlane.context.ConnectionContext;

/**
 * Uses a recipe file that executes a JDBC load and a JDBC save.
 */
class Jdbc2JdbcTransformSaveTest {
  private static final String srcBookCountQuery =
      "SELECT count(*) FROM books";
  private static final String destBookCountQuery =
      "SELECT count(*) FROM new_books";
  private static final String destAuthorCountQuery =
      "SELECT count(*) FROM new_authors";

  private static final String h2SourceUrl =
      "jdbc:h2:mem:srcdb;DB_CLOSE_DELAY=-1;IGNORECASE=TRUE";
  private static final String h2DestinationUrl =
      "jdbc:h2:mem:destdb;DB_CLOSE_DELAY=-1;IGNORECASE=TRUE";

  private static final long EXPECTED_BOOK_COUNT = 8;
  private static final long EXPECTED_AUTHOR_COUNT = 5;

  private static int bookCount;

  @BeforeAll
  public static void setup() throws Exception {
    Connection srcConn =
        DriverManager.getConnection(h2SourceUrl, "test", null);
    Connection destConn =
        DriverManager.getConnection(h2DestinationUrl, "dest", null);
    srcConn.setAutoCommit(true);
    destConn.setAutoCommit(true);

    Statement stmt = srcConn.createStatement();

    stmt.execute(
        "create table books (id int not null, authorId int not null, title varchar(100) not null)");

    stmt.execute(
        "insert into books (id, authorId, title) values (1, 100, 'Atlas Shrugged')");
    stmt.execute(
        "insert into books (id, authorId, title) values (2, 200, '1984')");
    stmt.execute(
        "insert into books (id, authorId, title) values (3, 200, 'Animal Farm')");
    stmt.execute(
        "insert into books (id, authorId, title) values (4, 300, 'The Overton Window')");
    stmt.execute(
        "insert into books (id, authorId, title) values (5, 300, 'The Eye of Moloch')");
    stmt.execute(
        "insert into books (id, authorId, title) values (6, 400, 'One Second After')");
    stmt.execute(
        "insert into books (id, authorId, title) values (7, 300, 'The Immortal Nicholas')");
    stmt.execute(
        "insert into books (id, authorId, title) values (8, 500, 'Spark In Action')");

    ResultSet rs = stmt.executeQuery(srcBookCountQuery);
    assertTrue(rs.next());
    bookCount = rs.getInt(1);
    assertEquals(EXPECTED_BOOK_COUNT, bookCount);
    System.out.println("There are " + bookCount + " books in the table.");

    srcConn.close();
  }

  @AfterAll
  public static void tearDown() throws Exception {
  }

  @Test
  public void srcDbTest() throws Exception {
    Connection conn =
        DriverManager.getConnection(h2SourceUrl, "test", null);
    conn.setAutoCommit(true);

    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery(srcBookCountQuery);
    assertTrue(rs.next());
    assertEquals(bookCount, rs.getInt(1));

    conn.close();
  }

  @Test
  void test() throws Exception {
    DataStore store =
        new DataStore("src/test/resources/recipe-jdbc2jdbc-save.yaml");

    DataTransformer.transform(store);

    ConnectionContext srcCtx =
        store.getConnectionInstance("jdbc", "h2-source");
    assertNotNull(srcCtx);
    assertEquals(h2SourceUrl, srcCtx.getUrl());

    ConnectionContext destCtx =
        store.getConnectionInstance("jdbc", "h2-destination");
    assertNotNull(destCtx);
    assertEquals(h2DestinationUrl, destCtx.getUrl());

    assertEquals(2, store.getDataframeCount());

    Dataset<Row> books = store.get("books");
    assertNotNull(books);
    assertEquals(EXPECTED_BOOK_COUNT, books.count());

    Dataset<Row> authors = store.get("authors");
    assertNotNull(authors);
    assertEquals(EXPECTED_AUTHOR_COUNT, authors.count());

    // check destination database for new tables
    Connection conn =
        DriverManager.getConnection(h2DestinationUrl, "dest", null);
    conn.setAutoCommit(true);

    Statement stmt = conn.createStatement();

    ResultSet rs = stmt.executeQuery(destBookCountQuery);
    assertTrue(rs.next());
    assertEquals(EXPECTED_BOOK_COUNT, rs.getInt(1));

    rs = stmt.executeQuery(destAuthorCountQuery);
    assertTrue(rs.next());
    assertEquals(EXPECTED_AUTHOR_COUNT, rs.getInt(1));

    conn.close();
  }

}
