package com.advancestores.enterprisecatalog.datafastlane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import com.advancestores.enterprisecatalog.datafastlane.util.Utils;

/**
 * Tests the $import tag. The $import tag is handled by a preprocessor. It will scan the recipe and
 * replace any $import tag with the contents referenced by the import file path. The replacement is
 * recursive, i.e. fragments may also contain $import tags.
 */
class RecipeImportFragmentTest {

    // Recipe with no $import tag. This should result in no replacement and no temporary file.
    @Test
    void testNoImport() {
        assertFalse(Utils.existsInFile("src/test/resources/recipe-csv2json-save.yaml", "$import"));
        DataStore store = new DataStore("src/test/resources/recipe-csv2json-save.yaml");
        assertEquals("src/test/resources/recipe-csv2json-save.yaml", store.getRawRecipePath());
        assertEquals("src/test/resources/recipe-csv2json-save.yaml", store.getRecipePath());
        assertFalse(Utils.existsInFile(store.getRawRecipePath(), "$import"));
        assertFalse(Utils.existsInFile(store.getRecipePath(), "$import"));
    }

    // Recipe with a single $import tag. The import will be replaced and a temporary file will be created.
    @Test
    void testSingleImport() {
        assertTrue(Utils.existsInFile("src/test/resources/recipe-single-import.yaml", "$import"));
        DataStore store = new DataStore("src/test/resources/recipe-single-import.yaml");
        assertEquals("src/test/resources/recipe-single-import.yaml", store.getRawRecipePath());
        assertTrue(store.getRecipePath().contains("__dfl__pp__"));
        assertTrue(Utils.existsInFile(store.getRawRecipePath(), "$import"));
        assertFalse(Utils.existsInFile(store.getRecipePath(), "$import"));

        // there is a deleteOnExit for the temp file. make sure we can still get to the file before exit
        try {
            String s = FileUtils.readFileToString(new File(store.getRecipePath()), "UTF-8");
            assertTrue(!s.isEmpty());
        }
        catch (Exception e) {
            fail("Should not have thrown an exception reading the temp file.");
        }
    }

    // Recipe with multiple $import tags. All imports will be replaced and a temp file will be created.
    @Test
    void testMultipleImports() {
        assertTrue(Utils.existsInFile("src/test/resources/recipe-multiple-imports.yaml", "$import"));
        DataStore store = new DataStore("src/test/resources/recipe-multiple-imports.yaml");
        assertEquals("src/test/resources/recipe-multiple-imports.yaml", store.getRawRecipePath());
        assertTrue(store.getRecipePath().contains("__dfl__pp__"));
        assertTrue(Utils.existsInFile(store.getRawRecipePath(), "$import"));
        assertFalse(Utils.existsInFile(store.getRecipePath(), "$import"));
    }

    // Recipe with a $import tag and the imported fragment also contains a $import tag. All imports
    // will be recursively replaced. Temp files will be created.
    @Test
    void testSingleImportWithEmbeddedImport() {
        assertTrue(Utils.existsInFile("src/test/resources/recipe-single-import-embedded.yaml", "$import"));
        DataStore store = new DataStore("src/test/resources/recipe-single-import-embedded.yaml");
        assertEquals("src/test/resources/recipe-single-import-embedded.yaml", store.getRawRecipePath());
        assertTrue(store.getRecipePath().contains("__dfl__pp__"));
        assertTrue(Utils.existsInFile(store.getRawRecipePath(), "$import"));
        assertFalse(Utils.existsInFile(store.getRecipePath(), "$import"));

        // there is a deleteOnExit for the temp file. make sure we can still get to the file before exit
        try {
            String s = FileUtils.readFileToString(new File(store.getRecipePath()), "UTF-8");
            assertTrue(!s.isEmpty());
        }
        catch (Exception e) {
            fail("Should not have thrown an exception reading the temp file.");
        }
    }

}
