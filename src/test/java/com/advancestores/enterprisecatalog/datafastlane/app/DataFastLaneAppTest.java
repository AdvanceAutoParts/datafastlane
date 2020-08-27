package com.advancestores.enterprisecatalog.datafastlane.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import com.advancestores.enterprisecatalog.datafastlane.K;
import com.advancestores.enterprisecatalog.datafastlane.context.AwsCredentialsContext;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;

/**
 * Test execution the DataFastLaneApp application.
 */
class DataFastLaneAppTest {

    @Test
    void testDryRun() {
        try {
            // should run the recipe file as a dryrun
            DataFastLaneApp.main(new String[] {"src/test/resources/recipe1.yaml", "--dryrun"});
        }
        catch (Exception e) {
            fail("Should not have received an exception.");
        }

        try {
            // should run the recipe file as a dryrun
            DataFastLaneApp.main(new String[] {"--DRyrUN", "src/test/resources/recipe1.yaml"});
        }
        catch (Exception e) {
            fail("Should not have received an exception.");
        }

        try {
            // should run the recipe file as a dryrun
            DataFastLaneApp.main(new String[] {"--DRyrUN", "--DRYRUN", "src/test/resources/recipe1.yaml",
                                               "somebogusfile.txt", "anotherbogusfile.txt"});
        }
        catch (Exception e) {
            fail("Should not have received an exception.");
        }

    }

    @Test
    void test() throws Exception {
        try {
            // should print usage statement
            DataFastLaneApp.main(new String[] {""});
        }
        catch (Exception e) {
            fail("Should not have received an exception.");
        }

        try {
            // should print usage statement
            DataFastLaneApp.main(new String[] {});
        }
        catch (Exception e) {
            fail("Should not have received an exception.");
        }

        try {
            // should run the recipe file
            DataFastLaneApp.main(new String[] {"src/test/resources/recipe1.yaml"});
        }
        catch (Exception e) {
            fail("Should not have received an exception.");
        }

        // test variable interpolation
        System.getProperties().put("resourceDir", "src/test/resources/");

        try {
            // should run the recipe file
            DataFastLaneApp.main(new String[] {"${sys:resourceDir}recipe1.yaml"});
        }
        catch (Exception e) {
            fail("Should not have received an exception.");
        }
    }

    /**
     * Tests S3 access for recipes and data files. This will only run if the S3-required
     * variables are set as environment or system properties. Without a valid AWS role and region,
     * access to S3 will not work. This will obviously need network access as well.
     * 
     * The test will create an S3 bucket, upload recipe and data files. The recipe references the S3 data
     * file via a system property which needs to be set once the bucket is created. The DFL app will run
     * and attempt to process the recipe. The test will attempt to clean up behind itself.
     * 
     * Required Environment Variables/System Properties:
     * - aws_access_key_id: AWS account access key ID
     * - aws_secret_access_key: AWS account secrete access key
     * - aws_session_token: AWS temporary session token (optional - set when using temporary session creds)
     * - aws_role_arn: AWS role ARN with permission to read/write to S3
     * - aws_region: AWS region used for S3 access
     * - s3data: the root prefix where the recipe and data files can be found
     * 
     * Do not fail the test if the env vars are not set.
     */
    @Test
    void testS3Integration() throws Exception {
        String accessKeyId = Utils.getEnv(K.KEY_AWS_ACCESS_KEY_ID, System.getProperty(K.KEY_AWS_ACCESS_KEY_ID));
        String secretAccessKey = Utils.getEnv(K.KEY_AWS_SECRET_ACCESS_KEY,
                                              System.getProperty(K.KEY_AWS_SECRET_ACCESS_KEY));
        String sessionToken = Utils.getEnv(K.KEY_AWS_SESSION_TOKEN, System.getProperty(K.KEY_AWS_SESSION_TOKEN));
        String role = Utils.getEnv(K.KEY_AWS_ROLE_ARN, System.getProperty(K.KEY_AWS_ROLE_ARN));
        String regionStr = Utils.getEnv(K.KEY_AWS_REGION, System.getProperty(K.KEY_AWS_REGION));
        StringBuilder buf = new StringBuilder();

        buf.append("\nRunning S3 Integration Test\n");
        buf.append("    Access Key ID: " + accessKeyId + "\n");
        buf.append("    Secret Access Key: " + secretAccessKey + "\n");
        buf.append("    Session Token: " + sessionToken + "\n");
        buf.append("    Role: " + Utils.maskAwsRoleArnValue(role) + "\n");
        buf.append("    Region: " + regionStr + "\n");
        System.out.println(buf.toString());

        // run the test only if the required env vars are set
        if (StringUtils.isNotEmpty(accessKeyId) && StringUtils.isNotEmpty(secretAccessKey) &&
            StringUtils.isNotEmpty(regionStr)) {
            long testStart = System.currentTimeMillis();
            String bucketName = UUID.randomUUID().toString();
            String s3data = K.S3_PREFIX + bucketName + "/";
            String dataFilename = "books.csv";
            String recipeFilename = "recipe-sample-books-interpolation.yaml";
            String s3DataPath = s3data + dataFilename;
            String s3RecipePath = s3data + recipeFilename;
            AwsCredentialsContext ctx = new AwsCredentialsContext(accessKeyId, secretAccessKey, sessionToken, regionStr,
                                                                  role);

            System.out.println("S3 configuration is correct. Running test...");

            // set the system properties since the recipe will look there
            System.setProperty(K.KEY_AWS_ACCESS_KEY_ID, ctx.getAccessKeyId());
            System.setProperty(K.KEY_AWS_SECRET_ACCESS_KEY, ctx.getSecretAccessKey());
            System.setProperty(K.KEY_AWS_SESSION_TOKEN, ctx.getSessionToken());
            System.setProperty(K.KEY_AWS_ROLE_ARN, ctx.getRoleArn());
            System.setProperty(K.KEY_AWS_REGION, ctx.getRegion());
            System.setProperty("s3data", s3data);

            System.out.println("S3 Bucket: " + bucketName);
            System.out.println("S3 data location: " + s3data);
            System.out.println("S3 Recipe Path: " + s3RecipePath);
            System.out.println("S3 Data path: " + s3DataPath);

            // copy recipe and data file to S3
            boolean recipeUploaded = Utils.uploadS3File("src/test/resources/integration/recipes/" + recipeFilename,
                                                        s3RecipePath, ctx);

            if (!recipeUploaded) {
                // make sure the bucket is not hanging around
                boolean bucketDeleted = Utils.deleteS3Bucket(bucketName, ctx);

                System.out.println("Recipe Uploaded: " + recipeUploaded);
                System.out.println("Bucket Deleted: " + bucketDeleted);

                assertTrue((!recipeUploaded && bucketDeleted),
                           "The recipe file failed to be uploaded to S3. The bucket should have been deleted.");
            }

            boolean dataUploaded = Utils.uploadS3File("src/test/resources/integration/data/" + dataFilename, s3DataPath,
                                                      ctx);

            if (!dataUploaded) {
                // make sure the recipe file and bucket are not hanging around
                boolean recipeDeleted = Utils.deleteS3File(s3RecipePath, ctx);
                boolean bucketDeleted = Utils.deleteS3Bucket(bucketName, ctx);

                System.out.println("Data Uploaded: " + dataUploaded);
                System.out.println("Recipe Deleted: " + recipeDeleted);
                System.out.println("Bucket Deleted: " + bucketDeleted);

                assertTrue((!dataUploaded && recipeDeleted && bucketDeleted),
                           "The data file failed to be uploaded to S3. The recipe file and the bucket should have been deleted.");
            }

            String completionStatus = K.FAILED;

            // at this point, the recipe and the data have successfully been uploaded to S3 - now use them
            try {
                DataFastLaneApp.main(new String[] {s3RecipePath});
                completionStatus = K.SUCCESS;
            }
            catch (Exception e) {
                System.out.println("The S3 Integration test failed.\n" + e.getMessage());
            }

            // make sure the recipe file and bucket are not hanging around
            boolean recipeDeleted = Utils.deleteS3File(s3RecipePath, ctx);
            boolean dataDeleted = Utils.deleteS3File(s3DataPath, ctx);
            boolean bucketDeleted = Utils.deleteS3Bucket(bucketName, ctx);

            System.out.println("Recipe Deleted: " + recipeDeleted);
            System.out.println("Data Deleted: " + dataDeleted);
            System.out.println("Bucket Deleted: " + bucketDeleted);

            System.out.println("S3 Integration Test completed in " + ((System.currentTimeMillis() - testStart) / 1000) +
                               " seconds with a status of " + completionStatus + ".");

            assertTrue(recipeDeleted && dataDeleted && bucketDeleted,
                       "The recipe, data, and bucket should have been deleted.");

            assertEquals(K.SUCCESS, completionStatus);
        }
        else {
            System.out.println("***** The AWS environment variables are not set. S3 integration test will not run. *****");
        }
    }
}
