package com.advancestores.enterprisecatalog.datafastlane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Uses a recipe file that executes a CSV load and a CSV save. This confirms
 * that a recipe with two $store tags works as expected.
 */
class Csv2CsvMultiStoreTransformSaveTest {
  private static final long EXPECTED_AUTHORS_COUNT = 15; // do not include
                                                         // header
  private static final long EXPECTED_AUTHORS2_COUNT = 5; // do not include
                                                         // header

  @BeforeAll
  public static void setup() throws Exception {
  }

  @AfterAll
  public static void tearDown() throws Exception {
  }

  @Test
  void test() throws Exception {
    DataStore store = new DataStore(
        "src/test/resources/recipe-csv2csv-save-multiple-store-tags.yaml");

    DataTransformer.transform(store);

    assertEquals(2, store.getDataframeCount());

    Dataset<Row> authors = store.get("authors");
    assertNotNull(authors);
    assertEquals(EXPECTED_AUTHORS_COUNT, authors.count());

    Dataset<Row> authors2 = store.get("authors2");
    assertNotNull(authors2);
    assertEquals(EXPECTED_AUTHORS2_COUNT, authors2.count());

    // make sure new CSV file was created
    String filename = "src/test/resources/JUNIT_AUTHORS.csv";
    File file = new File(filename);
    long fileRowCount = 0;

    try {
      Path path = Paths.get(filename);
      fileRowCount = Files.lines(path).count();
    } catch (Exception e) {
      if (file.exists()) {
        assertTrue(file.delete());
      }

      fail("Unable to count lines in file '" + filename + "'.\n"
          + e.getMessage());
    }

    boolean authorsDeleted = file.delete();

    // include header
    assertEquals(EXPECTED_AUTHORS_COUNT + 1, fileRowCount);

    // make sure new CSV file was created
    filename = "src/test/resources/JUNIT_AUTHORS2.csv";
    file = new File(filename);
    fileRowCount = 0;

    try {
      Path path = Paths.get(filename);
      fileRowCount = Files.lines(path).count();
    } catch (Exception e) {
      if (file.exists()) {
        assertTrue(file.delete());
      }

      fail("Unable to count lines in file '" + filename + "'.\n"
          + e.getMessage());
    }

    boolean authors2Deleted = file.delete();

    // include header
    assertEquals(EXPECTED_AUTHORS2_COUNT + 1, fileRowCount);

    assertTrue(authorsDeleted);
    assertTrue(authors2Deleted);
  }

}
