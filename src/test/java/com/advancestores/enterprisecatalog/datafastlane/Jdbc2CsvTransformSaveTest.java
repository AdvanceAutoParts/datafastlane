package com.advancestores.enterprisecatalog.datafastlane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
 * Uses a recipe file that executes a JDBC load and a CSV save.
 */
class Jdbc2CsvTransformSaveTest {
    private static final String bookCountQuery = "SELECT count(*) FROM books";

    private static final String h2Url = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;IGNORECASE=TRUE";

    private static final long EXPECTED_BOOK_COUNT = 8;

    private static int bookCount;

    @BeforeAll
    public static void setup() throws Exception {
        Connection conn = DriverManager.getConnection(h2Url, "test", null);
        conn.setAutoCommit(true);

        Statement stmt = conn.createStatement();

        stmt.execute("create table books (id int not null, authorId int not null, title varchar(100) not null)");

        stmt.execute("insert into books (id, authorId, title) values (1, 100, 'Atlas Shrugged')");
        stmt.execute("insert into books (id, authorId, title) values (2, 200, '1984')");
        stmt.execute("insert into books (id, authorId, title) values (3, 200, 'Animal Farm')");
        stmt.execute("insert into books (id, authorId, title) values (4, 300, 'The Overton Window')");
        stmt.execute("insert into books (id, authorId, title) values (5, 300, 'The Eye of Moloch')");
        stmt.execute("insert into books (id, authorId, title) values (6, 400, 'One Second After')");
        stmt.execute("insert into books (id, authorId, title) values (7, 300, 'The Immortal Nicholas')");
        stmt.execute("insert into books (id, authorId, title) values (8, 500, 'Spark In Action')");

        ResultSet rs = stmt.executeQuery(bookCountQuery);
        assertTrue(rs.next());
        bookCount = rs.getInt(1);
        assertEquals(EXPECTED_BOOK_COUNT, bookCount);
        System.out.println("There are " + bookCount + " books in the table.");

        conn.close();
    }

    @AfterAll
    public static void tearDown() throws Exception {
    }

    @Test
    public void dbTest() throws Exception {
        Connection conn = DriverManager.getConnection(h2Url, "test", null);
        conn.setAutoCommit(true);

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery(bookCountQuery);
        assertTrue(rs.next());
        assertEquals(bookCount, rs.getInt(1));

        conn.close();
    }

    @Test
    void test() throws Exception {
        DataStore store = new DataStore("src/test/resources/recipe-jdbc2csv-save.yaml");

        DataTransformer.transform(store);

        ConnectionContext ctx = store.getConnectionInstance("jdbc", "h2");
        assertNotNull(ctx);
        assertEquals(h2Url, ctx.getUrl());

        assertEquals(1, store.getDataframeCount());

        Dataset<Row> books = store.get("books");
        assertNotNull(books);
        assertEquals(EXPECTED_BOOK_COUNT, books.count());

        // make sure new CSV file was created
        String filename = "src/test/resources/JUNIT_BOOKS.csv";
        File file = new File(filename);

        long fileRowCount = 0;

        try {
            Path path = Paths.get(filename);
            fileRowCount = Files.lines(path).count();
        }
        catch (Exception e) {
            if (file.exists()) {
                assertTrue(file.delete());
            }

            fail("Unable to count lines in file '" + filename + "'.\n" + e.getMessage());
        }

        // subtract 1 for header row
        fileRowCount--;

        assertTrue(file.delete());

        assertEquals(EXPECTED_BOOK_COUNT, fileRowCount);
    }

    @Test
    void testWithFragment() throws Exception {
        DataStore store = new DataStore("src/test/resources/recipe-jdbc2csv-fragment-save.yaml");

        DataTransformer.transform(store);

        ConnectionContext ctx = store.getConnectionInstance("jdbc", "h2");
        assertNotNull(ctx);
        assertEquals(h2Url, ctx.getUrl());

        assertEquals(1, store.getDataframeCount());

        Dataset<Row> books = store.get("books");
        assertNotNull(books);
        assertEquals(EXPECTED_BOOK_COUNT, books.count());

        // make sure new CSV file was created
        String filename = "src/test/resources/JUNIT_BOOKS.csv";
        File file = new File(filename);

        long fileRowCount = 0;

        try {
            Path path = Paths.get(filename);
            fileRowCount = Files.lines(path).count();
        }
        catch (Exception e) {
            if (file.exists()) {
                assertTrue(file.delete());
            }

            fail("Unable to count lines in file '" + filename + "'.\n" + e.getMessage());
        }

        // subtract 1 for header row
        fileRowCount--;

        assertTrue(file.delete());

        assertEquals(EXPECTED_BOOK_COUNT, fileRowCount);
    }

}
