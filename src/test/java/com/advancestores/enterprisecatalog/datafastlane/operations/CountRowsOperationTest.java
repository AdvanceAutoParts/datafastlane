package com.advancestores.enterprisecatalog.datafastlane.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.DataTransformer;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Attribute;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Container;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

class CountRowsOperationTest {

    @Test
    void test() {
        String filename = "data/books/books.csv";
        long lineCount = 0;

        try {
            Path path = Paths.get(filename);
            lineCount = Files.lines(path).count();
        }
        catch (Exception e) {
            fail("Unable to count lines in file '" + filename + "'.\n" + e.getMessage());
        }

        // subtract 1 for header row
        lineCount--;

        DataStore store = new DataStore("src/test/resources/recipe-count-books.yaml");
        CountRowsOperation crOp = new CountRowsOperation();

        crOp.setStore(store);

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

        crOp.setContainerName(container.getContainerName());

        assertTrue(container.getAttributes().size() > 0);

        Attribute attribute = container.getAttributes().get(0);
        crOp.setAttributeName(attribute.getName());

        assertTrue(attribute.getOperations().size() > 0);

        Operation operation = attribute.getOperation("countRows");
        assertNotNull(operation);

        crOp.setOperationDefinition(operation);
        crOp.run();

        assertEquals(lineCount, crOp.getRowCount());
    }

}
