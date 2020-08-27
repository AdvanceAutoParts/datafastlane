package com.advancestores.enterprisecatalog.datafastlane.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.K;

/**
 * Test the SparkContext class.
 */
class S3PathTest {

    @Test
    void testConstructorInvalidFormat() {
        try {
            new S3Path("/tmp/myfile");
            fail("Should have failed with invalid format.");
        }
        catch (FastLaneException e) {
            assertTrue(e.getMessage().contains("is malformed"));
        }

        try {
            new S3Path("");
            fail("Should have failed with invalid format.");
        }
        catch (FastLaneException e) {
            assertTrue(e.getMessage().contains("is malformed"));
        }

        try {
            new S3Path(null);
            fail("Should have failed with invalid format.");
        }
        catch (FastLaneException e) {
            assertTrue(e.getMessage().contains("is malformed"));
        }

        try {
            new S3Path("s3://");
            fail("Should have failed with invalid format.");
        }
        catch (FastLaneException e) {
            assertTrue(e.getMessage().contains("is malformed"));
        }

        try {
            new S3Path("s3://bucket");
            fail("Should have failed with invalid format.");
        }
        catch (FastLaneException e) {
            assertTrue(e.getMessage().contains("is malformed"));
        }

        try {
            new S3Path("s3://bucket/");
            fail("Should have failed with invalid format.");
        }
        catch (FastLaneException e) {
            assertTrue(e.getMessage().contains("is malformed"));
        }

        try {
            new S3Path("s3:///bucket/filename.txt");
            fail("Should have failed with invalid format.");
        }
        catch (FastLaneException e) {
            assertTrue(e.getMessage().contains("is malformed"));
        }

        try {
            new S3Path("s3://bucket//filename.txt");
            fail("Should have failed with invalid format.");
        }
        catch (FastLaneException e) {
            assertTrue(e.getMessage().contains("is malformed"));
        }

        try {
            new S3Path("s3://my_bucket/filename.txt");
            fail("Should have failed with invalid format.");
        }
        catch (FastLaneException e) {
            assertTrue(e.getMessage().contains("S3 bucket name is not valid"));
        }

    }

    @Test
    void testValidConstructor() {
        S3Path s3path = new S3Path("s3://henry-test-data/recipes/recipe.yaml");
        assertNotNull(s3path);
        assertEquals(K.S3_PREFIX, s3path.getPrefix());
        assertEquals("henry-test-data", s3path.getBucketName());
        assertEquals("recipes/recipe.yaml", s3path.getPath());
        assertEquals("recipe.yaml", s3path.getFileName());
        System.out.println(s3path.toString());
    }
}
