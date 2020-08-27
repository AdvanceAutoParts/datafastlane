package com.advancestores.enterprisecatalog.datafastlane.operations;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.DataTransformer;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Attribute;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Container;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test Fixture for the LowerCaseOperation, UpperCaseOperation, and InitialCapOperation classes
 *
 * @author gabriel.priester
 * @version 1.0.0
 */
class LetterCaseOperationTest {

    @ParameterizedTest
    @CsvSource({ "upperCase, expectedResultUpperCase, recipe-uppercase.yaml",
                 "lowerCase, expectedResultLowerCase, recipe-lowercase.yaml",
                 "initialCap, expectedResultInitialCap, recipe-initial-cap.yaml"})
    void test(String operationName, String expectedResultColumn, String recipeFileName) {
        String filename = "data/test/LetterCaseTest.csv";
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

        Operation operation = attribute.getOperation(operationName);
        assertNotNull(operation);

        trimOp.setOperationDefinition(operation);
        trimOp.run();

        DataStore ds = trimOp.getStore();
        Dataset<Row> df = ds.getFirstDataframe();

        df.foreach((Row r) ->
            {
                String expectedValue = r.getString(r.fieldIndex(expectedResultColumn));
                String actualValue = r.getString(r.fieldIndex("subjectTextForLetterCase"));
                Integer rowId = r.getInt(r.fieldIndex("id"));

                String testFailureMessage = String.format("Test failed where row id = %d: ", rowId);
                assertEquals(expectedValue,actualValue,testFailureMessage);
            }
        );

    }
}
