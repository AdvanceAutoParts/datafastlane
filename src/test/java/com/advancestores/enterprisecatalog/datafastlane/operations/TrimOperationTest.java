package com.advancestores.enterprisecatalog.datafastlane.operations;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.DataTransformer;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Attribute;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Container;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;
import com.advancestores.enterprisecatalog.datafastlane.util.SparkUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
/**
 * Test Fixture for the TrimOperation class
 *
 *
 * @author gabriel.priester
 * @version 1.0.0
 */
class TrimOperationTest {

    @ParameterizedTest()
    @CsvSource({ "ALL, expectedResultTrim, recipe-trim-string.yaml",
                 "LEFT, expectedResultLTrim, recipe-ltrim-string.yaml",
                 "RIGHT, expectedResultRTrim, recipe-rtrim-string.yaml"})
    void testTrim(String trimType, String expectedResultsColumn, String recipeFileName) {
        String filename = "data/test/TrimTest.csv";
        long rowCount = 0;

        try {
            Path path = Paths.get(filename);
            rowCount = Files.lines(path).count();
        }
        catch (Exception e) {
            fail("Unable to count lines in file '" + filename + "'.\n" + e.getMessage());
        }

        // subtract 1 for header row
        rowCount--;

        DataStore store = new DataStore(String.format("src/test/resources/%s",recipeFileName));
        TrimOperation trimOp = new TrimOperation();

        trimOp.setStore(store);

        DataTransformer.transform(store);

        // store container plus more
        assertTrue(store.getRecipe().getContainers().size() > 1);

        Container container = store.getRecipe()
                                   .getContainers()
                                   .stream()
                                   .filter(c -> !c.getContainerName().startsWith("$"))
                                   .findFirst()
                                   .orElse(null);

        assertNotNull(container);

        trimOp.setContainerName(container.getContainerName());

        assertTrue(container.getAttributes().size() > 0);

        Attribute attribute = container.getAttributes().get(0);
        trimOp.setAttributeName(attribute.getName());

        assertTrue(attribute.getOperations().size() > 0);

        Operation operation = attribute.getOperation("trim");
        assertNotNull(operation);

        trimOp.setOperationDefinition(operation);
        trimOp.run();

        DataStore ds = trimOp.getStore();
        Dataset<Row> df = ds.getFirstDataframe();

        df.foreach((Row r) ->
            {
                String expectedValue = r.getString(r.fieldIndex(expectedResultsColumn));
                String actualValue = r.getString(r.fieldIndex("subjectTextToTrim"));
                Integer rowId = r.getInt(r.fieldIndex("id"));

                String testFailureMessage = String.format("Test failed where row id = %d: ", rowId);
                assertEquals(expectedValue,actualValue,testFailureMessage);
            }
        );

    }
}
