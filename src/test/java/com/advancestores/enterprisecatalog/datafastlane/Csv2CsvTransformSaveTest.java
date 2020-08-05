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
 * Uses a recipe file that executes a CSV load and a CSV save.
 */
class Csv2CsvTransformSaveTest {
    private static final long EXPECTED_AUTHORS_COUNT = 15;  // do not include header

    @BeforeAll
    public static void setup() throws Exception {
    }

    @AfterAll
    public static void tearDown() throws Exception {
    }

    @Test
    void test() throws Exception {
        DataStore store = new DataStore("src/test/resources/recipe-csv2csv-save.yaml");

        DataTransformer.transform(store);

        assertEquals(1, store.getDataframeCount());

        Dataset<Row> authors = store.get("authors");
        assertNotNull(authors);
        assertEquals(EXPECTED_AUTHORS_COUNT, authors.count());

        // make sure new CSV file was created
        String filename = "src/test/resources/JUNIT_AUTHORS.csv";
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

        assertTrue(file.delete());

        // include header
        assertEquals(EXPECTED_AUTHORS_COUNT + 1, fileRowCount);
    }

}
