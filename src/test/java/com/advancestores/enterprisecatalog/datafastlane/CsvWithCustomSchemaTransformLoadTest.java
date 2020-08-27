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

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.DataTransformer;

/**
 * Uses a recipe file that executes a CSV load using a custom schema.
 */
class CsvWithCustomSchemaTransformLoadTest {

    @Test
    void test() {
        DataStore store = new DataStore("src/test/resources/recipe-csv-custom-schema.yaml");

        assertTrue(DataTransformer.transform(store));

        Dataset<Row> ds = store.get("buyersguide");

        assertNotNull(ds);
        long dsRowCount = ds.count();

        assertTrue(dsRowCount > 0);

        String filename = "data/buyersguide/PROD_BUYERS_GUIDE_SAMPLE.csv";
        long fileRowCount = 0;

        try {
            Path path = Paths.get(filename);
            fileRowCount = Files.lines(path).count();
        }
        catch (Exception e) {
            fail("Unable to count lines in file '" + filename + "'.\n" + e.getMessage());
        }

        // subtract 1 for header row
        fileRowCount--;

        assertEquals(fileRowCount, dsRowCount);
    }

}
