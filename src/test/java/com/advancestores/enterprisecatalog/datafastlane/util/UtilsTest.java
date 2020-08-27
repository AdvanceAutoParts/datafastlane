package com.advancestores.enterprisecatalog.datafastlane.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import com.advancestores.enterprisecatalog.datafastlane.K;
import com.advancestores.enterprisecatalog.datafastlane.context.AwsCredentialsContext;

/**
 * Test the Utils class.
 */
class UtilsTest {

    @Test
    void testGetGson() {
        assertNotNull(Utils.getGson());
    }

    @Test
    void testGetGsonExpose() {
        assertNotNull(Utils.getGsonExpose());
    }

    @Test
    void testGetEnv() {
        assertEquals("BOGUS", Utils.getEnv("bogus", "BOGUS"));
        assertNull(Utils.getEnv("bogus"));
        assertNull(Utils.getEnv(null));
        assertEquals("NULL", Utils.getEnv(null, "NULL"));

        Map<String, String> env = System.getenv();
        assertFalse(env.isEmpty());
        String[] keys = new String[env.size()];
        env.keySet().toArray(keys);

        String key = keys[0];
        String val = env.get(key);
        assertEquals(val, Utils.getEnv(key));
    }

    @Test
    void testGetSparkDataType() {
        assertNull(Utils.getSparkDataType(""));
        assertNull(Utils.getSparkDataType(null));

        assertEquals(DataTypes.StringType, Utils.getSparkDataType("string"));
        assertEquals(DataTypes.IntegerType, Utils.getSparkDataType("integer"));
        assertEquals(DataTypes.BooleanType, Utils.getSparkDataType("boolean"));
        assertEquals(DataTypes.ByteType, Utils.getSparkDataType("byte"));
        assertEquals(DataTypes.FloatType, Utils.getSparkDataType("float"));
        assertEquals(DataTypes.DoubleType, Utils.getSparkDataType("double"));
        assertEquals(DataTypes.DateType, Utils.getSparkDataType("date"));
        assertEquals(DataTypes.TimestampType, Utils.getSparkDataType("timestamp"));
        assertEquals(DataTypes.LongType, Utils.getSparkDataType("long"));
        assertEquals(DataTypes.ShortType, Utils.getSparkDataType("short"));
        assertEquals(DataTypes.BinaryType, Utils.getSparkDataType("binary"));
    }

    @Test
    void testGetSparkStructField() {
        assertNull(Utils.getSparkStructField(null, null, "false"));
        assertNull(Utils.getSparkStructField("", "", "true"));

        StructField field = Utils.getSparkStructField("stringfield", "string", null);
        assertNotNull(field);
        assertFalse(field.nullable());
        assertEquals(DataTypes.StringType, field.dataType());
        assertEquals("stringfield", field.name());

        field = Utils.getSparkStructField("integerfield", "integer", "false");
        assertNotNull(field);
        assertFalse(field.nullable());
        assertEquals(DataTypes.IntegerType, field.dataType());
        assertEquals("integerfield", field.name());

        field = Utils.getSparkStructField("booleanfield", "boolean", "true");
        assertNotNull(field);
        assertTrue(field.nullable());
        assertEquals(DataTypes.BooleanType, field.dataType());
        assertEquals("booleanfield", field.name());

        field = Utils.getSparkStructField("bytefield", "byte", "true");
        assertNotNull(field);
        assertTrue(field.nullable());
        assertEquals(DataTypes.ByteType, field.dataType());
        assertEquals("bytefield", field.name());

        field = Utils.getSparkStructField("floatfield", "float", "true");
        assertNotNull(field);
        assertTrue(field.nullable());
        assertEquals(DataTypes.FloatType, field.dataType());
        assertEquals("floatfield", field.name());

        field = Utils.getSparkStructField("doublefield", "double", "true");
        assertNotNull(field);
        assertTrue(field.nullable());
        assertEquals(DataTypes.DoubleType, field.dataType());
        assertEquals("doublefield", field.name());

        field = Utils.getSparkStructField("datefield", "date", "true");
        assertNotNull(field);
        assertTrue(field.nullable());
        assertEquals(DataTypes.DateType, field.dataType());
        assertEquals("datefield", field.name());

        field = Utils.getSparkStructField("timestampfield", "timestamp", "true");
        assertNotNull(field);
        assertTrue(field.nullable());
        assertEquals(DataTypes.TimestampType, field.dataType());
        assertEquals("timestampfield", field.name());

        field = Utils.getSparkStructField("longfield", "long", "true");
        assertNotNull(field);
        assertTrue(field.nullable());
        assertEquals(DataTypes.LongType, field.dataType());
        assertEquals("longfield", field.name());

        field = Utils.getSparkStructField("shortfield", "short", "true");
        assertNotNull(field);
        assertTrue(field.nullable());
        assertEquals(DataTypes.ShortType, field.dataType());
        assertEquals("shortfield", field.name());

        field = Utils.getSparkStructField("binaryfield", "binary", "true");
        assertNotNull(field);
        assertTrue(field.nullable());
        assertEquals(DataTypes.BinaryType, field.dataType());
        assertEquals("binaryfield", field.name());
    }

    /**
     * This is an integration test. It will only attempt to run if the AWS environment variables or
     * system properties are set.
     * 
     * This will test the following S3 actions:
     * 1. upload
     * 2. download
     * 3. delete file
     * 4. delete bucket
     */
    @Test
    void testS3FileActions() {
        assertEquals("/data/myfile.txt", Utils.downloadS3File("/data/myfile.txt", null, new AwsCredentialsContext()));

        String accessKeyId = Utils.getEnv(K.KEY_AWS_ACCESS_KEY_ID, System.getProperty(K.KEY_AWS_ACCESS_KEY_ID));
        String secretAccessKey = Utils.getEnv(K.KEY_AWS_SECRET_ACCESS_KEY,
                                              System.getProperty(K.KEY_AWS_SECRET_ACCESS_KEY));
        String sessionToken = Utils.getEnv(K.KEY_AWS_SESSION_TOKEN, System.getProperty(K.KEY_AWS_SESSION_TOKEN));
        String role = Utils.getEnv(K.KEY_AWS_ROLE_ARN, System.getProperty(K.KEY_AWS_ROLE_ARN));
        String regionStr = Utils.getEnv(K.KEY_AWS_REGION, System.getProperty(K.KEY_AWS_REGION));

        if (StringUtils.isNotEmpty(accessKeyId) && StringUtils.isNotEmpty(secretAccessKey) &&
            StringUtils.isNotEmpty(regionStr)) {
            String bucketName = UUID.randomUUID().toString();
            String filename = "recipe-connections.yaml";
            String s3PathStr = "s3://" + bucketName + "/" + filename;
            AwsCredentialsContext ctx = new AwsCredentialsContext(accessKeyId, secretAccessKey, sessionToken, regionStr,
                                                                  role);

            // need to create an S3 file first
            boolean uploadStatus = Utils.uploadS3File("src/test/resources/" + filename, s3PathStr, ctx);

            if (uploadStatus) {
                boolean localFileExists = false;

                // try to download the new file
                String localFilePath = Utils.downloadS3Bucket(s3PathStr, ctx);

                if (StringUtils.isNotEmpty(localFilePath)) {
                    localFileExists = (Utils.checkFile(localFilePath) == null);
                }

                // clean up
                boolean deleteStatus = Utils.deleteS3File(s3PathStr, ctx);
                assertTrue(deleteStatus, "***** The S3 file " + s3PathStr +
                                         " was not removed after unit test completed. Manual clean up is required.");
                assertTrue(Utils.deleteS3Bucket(bucketName, ctx),
                           "***** The S3 bucket " + bucketName + " was not removed after unit test completed. Manual clean up is required.");

                // check this last to be sure all created items had a chance to be cleaned up
                assertTrue(localFileExists);
            }
            else {
                fail("Expected a file to be uploaded to S3.");
            }
        }
        else {
            System.out.println("***** The AWS role, AWS region and/or <userhome>/.aws credentials directory are not set. The S3 actions test will be skipped.");
        }
    }

    @Test
    void testExistsInFile() {
        assertTrue(Utils.existsInFile("src/test/resources/recipe-single-import.yaml", "$import"));
        assertFalse(Utils.existsInFile("src/test/resources/recipe-single-import.yaml", "$importttt"));
        assertFalse(Utils.existsInFile("src/test/resources/bogus.yaml", "$import"));
    }

    @Test
    void testCheckFile() {
        assertNull(Utils.checkFile("src/test/resources/recipe-single-import.yaml"));
        assertNotNull(Utils.checkFile("src/test/resources/"));
        assertNotNull(Utils.checkFile("bogus.yaml"));
    }

    @Test
    void testCheckDirectory() {
        assertNull(Utils.checkDirectory("src/test/resources"));
        assertNotNull(Utils.checkFile("src/test/resources/bogus"));
    }

    @Test
    void testGetInterpolator() {
        StringSubstitutor s1 = Utils.getInterpolatorInstance();
        StringSubstitutor s2 = Utils.getInterpolatorInstance();

        assertNotNull(s1);
        assertNotNull(s2);

        // should generate a new object each time
        assertNotEquals(s1, s2);
    }

    @Test
    void testAwsMaskRoleArnValue() {
        assertEquals("null", Utils.maskAwsRoleArnValue(null));
        assertEquals("null", Utils.maskAwsRoleArnValue(""));
        assertEquals("null", Utils.maskAwsRoleArnValue("    "));
        assertEquals("null", Utils.maskAwsRoleArnValue("  23  "));
        assertEquals("*****:aws*****", Utils.maskAwsRoleArnValue("arn:aws:iam::111222333444:role/bossman"));
        assertEquals("null", Utils.maskAwsRoleArnValue("012345"));
        assertEquals("*****3456*****", Utils.maskAwsRoleArnValue("0123456"));
        assertEquals("*****3456*****", Utils.maskAwsRoleArnValue("012345678"));
    }

    @Test
    void testBuildLocalTempPath() {
        String tempDirName = FileUtils.getTempDirectoryPath();
        String path = Utils.buildLocalTempPath("myfile.txt");

        assertTrue(path.startsWith(tempDirName));
        assertTrue(path.endsWith("_myfile.txt"));

        path = Utils.buildLocalTempPath(null);
        assertEquals(tempDirName, path);

        path = Utils.buildLocalTempPath("");
        assertEquals(tempDirName, path);
    }
}
