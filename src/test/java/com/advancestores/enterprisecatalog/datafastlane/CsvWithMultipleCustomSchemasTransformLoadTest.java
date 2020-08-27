package com.advancestores.enterprisecatalog.datafastlane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/**
 * Uses a recipe file that executes a CSV loads using multiple custom schema, i.e. each load has a schema.
 */
class CsvWithMultipleCustomSchemasTransformLoadTest {

    @Test
    void test() {
        DataStore store = new DataStore("src/test/resources/recipe-csv-custom-schema-multiples.yaml");

        assertTrue(DataTransformer.transform(store));

        assertNotNull(store.getSchema("buyersguide"));
        assertNotNull(store.getSchema("authors"));

        Dataset<Row> buyersGuideDs = store.get("buyersguide");
        assertNotNull(buyersGuideDs);

        long buyersGuideDsRowCount = buyersGuideDs.count();
        assertTrue(buyersGuideDsRowCount > 0);

        Dataset<Row> authorsDs = store.get("authors");
        assertNotNull(authorsDs);

        long authorsDsRowCount = authorsDs.count();
        assertTrue(authorsDsRowCount > 0);

        String filename = "data/buyersguide/PROD_BUYERS_GUIDE_SAMPLE.csv";
        long buyersGuideFileRowCount = 0;

        try {
            Path path = Paths.get(filename);
            buyersGuideFileRowCount = Files.lines(path).count();
        }
        catch (Exception e) {
            fail("Unable to count lines in file '" + filename + "'.\n" + e.getMessage());
        }

        // subtract 1 for header row
        buyersGuideFileRowCount--;

        assertEquals(buyersGuideFileRowCount, buyersGuideDsRowCount);

        filename = "data/books/authors2.csv";
        long authorsFileRowCount = 0;

        try {
            Path path = Paths.get(filename);
            authorsFileRowCount = Files.lines(path).count();
        }
        catch (Exception e) {
            fail("Unable to count lines in file '" + filename + "'.\n" + e.getMessage());
        }

        // subtract 1 for header row
        authorsFileRowCount--;

        assertEquals(authorsFileRowCount, authorsDsRowCount);
    }

}
