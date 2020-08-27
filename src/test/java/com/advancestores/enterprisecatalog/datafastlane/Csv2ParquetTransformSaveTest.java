package com.advancestores.enterprisecatalog.datafastlane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Uses a recipe file that executes a CSV load and a parquet save.
 */
class Csv2ParquetTransformSaveTest {
    private static final long EXPECTED_BG_COUNT = 206263;  // do not include header

    @BeforeAll
    public static void setup() throws Exception {
    }

    @AfterAll
    public static void tearDown() throws Exception {
    }

    // parquet files should generate multiple part-* files using the buyersguide data
    @Test
    void test() throws Exception {
        DataStore store = new DataStore("src/test/resources/recipe-csv2parquet-save.yaml");

        DataTransformer.transform(store);

        assertEquals(1, store.getDataframeCount());

        // make sure new delta lake directory was created
        String parentFolderName = "data/parquet";
        String folderName = parentFolderName + "/buyersguide";
        File parentFolder = new File(parentFolderName);
        File authorsFolder = new File(folderName);
        Iterator<File> files = FileUtils.iterateFiles(authorsFolder, new WildcardFileFilter("part-*"), null);
        long fileCount = 0;

        while (files.hasNext()) {
            files.next();
            fileCount++;
        }

        System.out.println("Parquet Files Generated: " + fileCount);

        if (fileCount == 0) {
            FileUtils.deleteDirectory(parentFolder);
            assertTrue(fileCount > 0);
        }
        else {
            SparkSession session = SparkSession.builder()
                                               .appName("junit test")
                                               .config("spark.executor.memory", "8g")
                                               .config("spark.memory.offHeap.enabled", "true")
                                               .config("spark.memory.offHeap.size", "32g")
                                               .master("local[*]")
                                               .getOrCreate();

            try {
                Dataset<Row> dataframe = session.read().parquet(folderName);
                assertEquals(EXPECTED_BG_COUNT, dataframe.count());
            }
            catch (Exception e) {
                fail("An error occurred while reading " + folderName + " parquet file.\n" + e.getMessage());
            }
            finally {
                FileUtils.deleteDirectory(parentFolder);
            }
        }
    }
}
