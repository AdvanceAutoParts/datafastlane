package com.advancestores.enterprisecatalog.datafastlane.util;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.K;
import com.advancestores.enterprisecatalog.datafastlane.context.AwsCredentialsContext;
import com.advancestores.enterprisecatalog.datafastlane.context.S3Path;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public abstract class Utils {
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  private static final Gson GSON =
      new GsonBuilder().setPrettyPrinting().create();
  private static final Gson GSON_EXPOSE =
      new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

  private static final String CLIENT_ERR_MSG =
      "Unable to create a client object to access S3 files. Check the user credentials.";

  /**
   * This Gson will not ignore annotated properties that should not be
   * exposed, e.g. passwords
   * 
   * @return Gson - singleton object used to easily convert Java objects to
   *         JSON strings - use for logging/toString
   */
  public static Gson getGson() {
    return GSON;
  }

  /**
   * Use this Gson when non-annotated fields are to be excluded from the
   * JSON string. Used to protect sensitive information such as passwords.
   * 
   * @return Gson - singleton object used to easily convert Java objects to
   *         JSON strings - use for logging/toString
   */
  public static Gson getGsonExpose() {
    return GSON_EXPOSE;
  }

  /**
   * Extract variable value from System environment. If not found, null will
   * be returned.
   * 
   * @param String
   *          varName - variable name/key from Environment map.
   * @return String - found value or null
   */
  public static String getEnv(String varName) {
    return getEnv(varName, null);
  }

  /**
   * Extract variable value from System environment. If not found, the value
   * contained in the defaultValue parameter will be returned.
   * 
   * @param String
   *          varName - variable name/key from Environment map.
   * @param String
   *          defaultValue - value to return if the variable is not found in
   *          the map
   * @return String - found value or value in defaultValue parameter
   */
  public static String getEnv(String varName, String defaultValue) {
    String varValue = System.getenv((varName == null ? "" : varName));

    return (varValue != null ? varValue : defaultValue);
  }

  /**
   * S3 client for S3 file access. AWS user credentials are required. If a
   * role ARN is provided, credentials of the role will be used.
   * 
   * @param AwsCredentialsContext
   *          ctx - AWS credential parameter values.
   * 
   * @return AmazonS3 - client object used to access S3
   */
  public static AmazonS3 getS3Client(AwsCredentialsContext ctx) {
    AmazonS3 s3client = null;

    try {
      Regions region = Regions.fromName(ctx.getRegion());
      AWSCredentials activeCreds =
          (StringUtils.isNotEmpty(ctx.getSessionToken())
              ? new BasicSessionCredentials(ctx.getAccessKeyId(),
                  ctx.getSecretAccessKey(),
                  ctx.getSessionToken())
              : new BasicAWSCredentials(ctx.getAccessKeyId(),
                  ctx.getSecretAccessKey()));

      if (StringUtils.isNotEmpty(ctx.getRoleArn())) {
        log.debug(
            "An AWS role ARN was provided. Switching to role credentials.");
        AssumeRoleRequest assumeRole =
            new AssumeRoleRequest().withRoleArn(ctx.getRoleArn())
                .withRoleSessionName(UUID.randomUUID()
                    .toString());

        // credentials for user requesting to assume role
        AWSSecurityTokenService sts =
            AWSSecurityTokenServiceClientBuilder.standard()
                .withRegion(region)
                .withCredentials(
                    new AWSStaticCredentialsProvider(activeCreds))
                .build();
        Credentials creds = sts.assumeRole(assumeRole).getCredentials();
        activeCreds = new BasicSessionCredentials(creds.getAccessKeyId(),
            creds.getSecretAccessKey(),
            creds.getSessionToken());
      }

      s3client = AmazonS3ClientBuilder.standard()
          .withRegion(region)
          .withCredentials(new AWSStaticCredentialsProvider(activeCreds))
          .build();
    } catch (Exception e) {
      String errMsg =
          "An error occurred while creating an Amazon S3 client.";
      log.error(errMsg, e);
      throw new FastLaneException(errMsg);
    }

    return s3client;
  }

  /**
   * Deletes an S3 file. It will not check to see if the file exists before
   * delete.
   * 
   * @param String
   *          s3PathStr - fully qualified S3 file name
   * @param AwsCredentialsContext
   *          ctx - AWS credential parameter values.
   * 
   * @return boolean - true if the file does not exist after the delete
   */
  public static boolean deleteS3File(String s3PathStr,
      AwsCredentialsContext ctx) {
    AmazonS3 s3client = null;
    boolean retval = false;

    log.debug("Attempting to delete S3 file {} using credentials {}",
        s3PathStr, ctx);

    // this will throw an exception if the S3 path is malformed
    S3Path s3path = new S3Path(s3PathStr);

    long deleteStart = System.currentTimeMillis();
    String status = K.FAILED;

    try {
      s3client = Utils.getS3Client(ctx);

      if (s3client != null) {
        log.debug("\nBucket Name: {}\nBucket FilePath: {}\n",
            s3path.getBucketName(), s3path.getPath());

        s3client.deleteObject(s3path.getBucketName(), s3path.getPath());
        retval = !s3client.doesObjectExist(s3path.getBucketName(),
            s3path.getPath());
        status = (retval ? K.SUCCESS : K.FAILED);
      } else {
        throw new FastLaneException(CLIENT_ERR_MSG);
      }
    } catch (Exception e) {
      String errMsg = "An error occurred while deleting S3 file "
          + s3PathStr + "\n" + e.getMessage();
      log.error(errMsg);
      throw new FastLaneException(errMsg);
    } finally {
      if (s3client != null) {
        s3client.shutdown();
      }

      log.debug(
          "Deletion of file {} completed in {} ms with a status of {}.",
          s3PathStr,
          (System.currentTimeMillis() - deleteStart), status);
    }

    return retval;
  }

  /**
   * Deletes an S3 bucket. It will not check to see if the bucket exists
   * before delete. The bucket must be empty and only the owner can delete.
   * 
   * @param String
   *          bucketName - name of bucket to delete
   * @param AwsCredentialsContext
   *          ctx - AWS credential parameter values.
   * 
   * @return boolean - true if the bucket does not exist after the delete
   */
  public static boolean deleteS3Bucket(String bucketName,
      AwsCredentialsContext ctx) {
    AmazonS3 s3client = null;
    boolean retval = false;

    log.debug("Attempting to delete S3 bucket {} using credentials {}",
        bucketName, ctx);

    long deleteStart = System.currentTimeMillis();
    String status = K.FAILED;

    try {
      s3client = Utils.getS3Client(ctx);

      if (s3client != null) {
        log.debug("Bucket Name: {}", bucketName);

        s3client.deleteBucket(bucketName);
        retval = !s3client.doesBucketExistV2(bucketName);
        status = (retval ? K.SUCCESS : K.FAILED);
      } else {
        throw new FastLaneException(CLIENT_ERR_MSG);
      }
    } catch (Exception e) {
      String errMsg = "An error occurred while deleting S3 bucket "
          + bucketName + "\n" + e.getMessage();
      log.error(errMsg);
      throw new FastLaneException(errMsg);
    } finally {
      if (s3client != null) {
        s3client.shutdown();
      }

      log.debug(
          "Deletion of bucket {} completed in {} ms with a status of {}.",
          bucketName,
          (System.currentTimeMillis() - deleteStart), status);
    }

    return retval;
  }

  /**
   * Upload a local file to an S3 bucket. If the bucket does not exist, it
   * will be created.
   * 
   * @param String
   *          localFilePath - fully qualified path of file to be uploaded
   * @param String
   *          s3PathStr - fully qualified S3 file name
   * @param AwsCredentialsContext
   *          ctx - AWS credential parameter values.
   * 
   * @return boolean - true if the file exists in the S3 bucket after upload
   */
  public static boolean uploadS3File(String localFilePath, String s3PathStr,
      AwsCredentialsContext ctx) {
    AmazonS3 s3client = null;
    boolean retval = false;

    log.debug(
        "Attempting to upload local file {} to {} using credentials {}",
        localFilePath, s3PathStr, ctx);

    // this will throw an exception if the S3 path is malformed
    S3Path s3path = new S3Path(s3PathStr);

    // make sure all the required params are valid - no need to do
    // unnecessary work
    FastLaneException fle = Utils.checkFile(localFilePath);

    if (fle != null) {
      log.error("The local file {} is not valid.\n{}", localFilePath, fle);
      throw fle;
    }

    long copyStart = System.currentTimeMillis();
    String status = K.FAILED;

    try {
      s3client = Utils.getS3Client(ctx);

      if (s3client != null) {
        // check to see if the bucket already exists
        if (!s3client.doesBucketExistV2(s3path.getBucketName())) {
          log.debug(
              "S3 bucket with name {} does not exist. It will be created.",
              s3path.getBucketName());

          s3client.createBucket(s3path.getBucketName());
        }

        log.debug(
            "\nLocal File Path: {}\nBucket Name: {}\nBucket FilePath: {}\n",
            localFilePath,
            s3path.getBucketName(), s3path.getPath());

        s3client.putObject(s3path.getBucketName(), s3path.getPath(),
            new File(localFilePath));
        retval = s3client.doesObjectExist(s3path.getBucketName(),
            s3path.getPath());
        status = (retval ? K.SUCCESS : K.FAILED);
      } else {
        throw new FastLaneException(CLIENT_ERR_MSG);
      }
    } catch (Exception e) {
      String errMsg = "An error occurred while uploading file "
          + localFilePath + " to S3 file " + s3PathStr +
          "\n" + e.getMessage();
      log.error(errMsg);
      throw new FastLaneException(errMsg);
    } finally {
      if (s3client != null) {
        s3client.shutdown();
      }

      log.debug("Upload of file {} completed in {} ms with status {}.",
          s3PathStr,
          (System.currentTimeMillis() - copyStart), status);
    }

    return retval;
  }

  /**
   * Builds a file path using the local temp directory and provided
   * filename. If no file is specified, the temp dir name will be returned.
   * No checks are performed for validity of filename.
   * 
   * @param String
   *          filename - name of file to use in path specification
   * 
   * @return String - full path name of local temp directory file
   */
  public static String buildLocalTempPath(String filename) {
    return (StringUtils.isNotEmpty(filename)
        ? FileUtils.getTempDirectoryPath() + UUID.randomUUID() + "_"
            + filename
        : FileUtils.getTempDirectoryPath());
  }

  // convenience call to ignore destination path and use local temp
  // directory
  public static String downloadS3Bucket(String s3Path,
      AwsCredentialsContext ctx) {
    return downloadS3File(s3Path, null, ctx);
  }

  /**
   * If the path represented by s3Path is an S3 bucket reference, copies the
   * file stored in the bucket to a local directory for processing. If
   * provided, access to the file is presumed by AWS role ARN, i.e. the file
   * should be accessible by the role specified by the ARN. The appropriate
   * AWS region must be specified as well.
   * 
   * @param String
   *          s3Path - path definition of file to be processed. If it begins
   *          with s3://, its an S3 bucket
   * @param String
   *          destinationPath - local path to which the S3 file will be
   *          copied. If null, the temp dir will be used along with the
   *          filename part of the S3 file, i.e. value after last '/'
   * @param AwsCredentialsContext
   *          ctx - AWS credential parameter values.
   * 
   * @return String - path definition of local file; if not an S3 bucket,
   *         the same value that was passed in
   */
  public static String downloadS3File(String s3PathStr,
      String destinationPath, AwsCredentialsContext ctx) {
    String filePath = s3PathStr;

    log.debug("\ns3Path: {}\ndestinationPath: {}\ncredentials: {}\n",
        s3PathStr, destinationPath, ctx);

    if (s3PathStr.startsWith(K.S3_PREFIX)) {
      long copyStart = System.currentTimeMillis();
      String status = K.FAILED;
      AmazonS3 s3client = null;

      try {
        s3client = Utils.getS3Client(ctx);

        if (s3client != null) {
          S3Path s3path = new S3Path(s3PathStr);

          if (StringUtils.isEmpty(destinationPath)) {
            // if no dest path was specified, use the local temp directory
            // and the filename part of S3
            filePath = Utils.buildLocalTempPath(s3path.getFileName());
          } else {
            filePath = destinationPath;
          }

          log.debug(
              "\nLocal File Path: {}\nBucket Name: {}\nBucket FilePath: {}\n",
              filePath,
              s3path.getBucketName(), s3path.getPath());

          S3Object s3object =
              s3client.getObject(s3path.getBucketName(), s3path.getPath());
          S3ObjectInputStream inputStream = s3object.getObjectContent();
          FileUtils.copyInputStreamToFile(inputStream, new File(filePath));
          status = K.SUCCESS;
        } else {
          throw new FastLaneException(CLIENT_ERR_MSG);
        }
      } catch (Exception e) {
        String errMsg = "An error occurred while copying S3 file "
            + s3PathStr + " to local storage " +
            filePath + ".\n" + e.getMessage();
        log.error(errMsg);
        throw new FastLaneException(errMsg);
      } finally {
        if (s3client != null) {
          s3client.shutdown();
        }

        log.debug("Download of file {} completed in {} ms with status {}.",
            s3PathStr,
            (System.currentTimeMillis() - copyStart), status);
      }
    }

    return filePath;
  }

  /**
   * In the recipe file, a simple value is used for Spark data types. In
   * this method, those keys are mapped to Spark data type objects.
   * 
   * @param String
   *          dataType - simple key value used to map to a Spark object
   * @return DataType - Spark data type object
   */
  public static final DataType getSparkDataType(String dataType) {
    DataType dt = null;

    if (StringUtils.isNotEmpty(dataType)) {
      dataType = dataType.trim().toLowerCase();

      switch (dataType) {
        case "string":
          dt = DataTypes.StringType;
          break;
        case "integer":
          dt = DataTypes.IntegerType;
          break;
        case "boolean":
          dt = DataTypes.BooleanType;
          break;
        case "byte":
          dt = DataTypes.ByteType;
          break;
        case "float":
          dt = DataTypes.FloatType;
          break;
        case "double":
          dt = DataTypes.DoubleType;
          break;
        case "date":
          dt = DataTypes.DateType;
          break;
        case "timestamp":
          dt = DataTypes.TimestampType;
          break;
        case "long":
          dt = DataTypes.LongType;
          break;
        case "short":
          dt = DataTypes.ShortType;
          break;
        case "binary":
          dt = DataTypes.BinaryType;
          break;
        default:
          dt = null;
      }
    }

    return dt;
  }

  /**
   * Convenience method to convert the data from the recipe file ($schema
   * section) to a Spark object.
   * 
   * @param String
   *          fieldName - name to be used by the Spark field object
   * @param String
   *          dataTypeStr - key used to map to a Spark data type (string,
   *          integer, etc)
   * @param String
   *          nullableStr - "true" if field should be considered nullable.
   *          Anything other than "true" is false
   * 
   * @return StructField - Spark field object
   */
  public static StructField getSparkStructField(String fieldName,
      String dataTypeStr, String nullableStr) {
    StructField sf = null;

    if (StringUtils.isNotEmpty(fieldName)
        && StringUtils.isNotEmpty(dataTypeStr)) {
      DataType dt = getSparkDataType(dataTypeStr);

      if (dt != null) {
        boolean nullable = "true".equals(nullableStr);
        sf = DataTypes.createStructField(fieldName, dt, nullable);
      }
    }

    return sf;
  }

  /**
   * Convert the recipe mode string to a proper Spark mode.
   * 
   * @param String
   *          modeStr - recipe value representing a Spark mode
   * @return SaveMode - Spark mode from recipe string or ErrorIfExists when
   *         no mode or invalid mode specified
   */
  public static SaveMode getSparkSaveMode(String modeStr) {
    SaveMode saveMode = null;

    if (StringUtils.isNotEmpty(modeStr)) {
      saveMode = SaveMode.valueOf(modeStr);
    }

    return (saveMode == null ? SaveMode.ErrorIfExists : saveMode);
  }

  /**
   * Check the access of a given file name. Returns null if all is ok or an
   * Exception object.
   */
  public static FastLaneException checkFile(String filename) {
    FastLaneException e = null;
    File rawFile = new File(filename);

    if (!rawFile.exists() || !rawFile.isFile()) {
      String errMsg = "The file with name " + filename
          + " does not exist or is not a regular file.";
      e = new FastLaneException(errMsg);
    }

    if (!rawFile.canRead()) {
      String errMsg = "The file with name " + filename
          + " is not readable.  Check permissions.";
      log.error(errMsg);
      e = new FastLaneException(errMsg);
    }

    return e;
  }

  /**
   * Check the access of a given directory name. Returns null if all is ok
   * or an Exception object.
   */
  public static FastLaneException checkDirectory(String dirName) {
    FastLaneException e = null;
    File dirFile = new File(dirName);

    if (!dirFile.exists() || dirFile.isFile()) {
      String errMsg = "The directory with name " + dirName
          + " does not exist or is not a directory.";
      e = new FastLaneException(errMsg);
    }

    if (!dirFile.canRead()) {
      String errMsg = "The directory with name " + dirName
          + " is not readable.  Check permissions.";
      log.error(errMsg);
      e = new FastLaneException(errMsg);
    }

    return e;
  }

  /**
   * Search for a string value within a file.
   * 
   * @param Stirng
   *          filepath - full path of file
   * @param String
   *          value - value for which to search
   * @return boolean - true if the value was found; otherwise, false
   */
  public static boolean existsInFile(String filepath, String value) {
    boolean retval = false;

    try (Stream<String> stream = Files.lines(Paths.get(filepath))) {
      retval = stream.anyMatch(line -> line.contains(value));
    } catch (Exception e) {
      String errMsg = "An error occurred while checking file " + filepath
          + " for value " + value;
      log.error(errMsg);
      retval = false;
    }

    return retval;
  }

  /**
   * The StringSubstitutor class is not thread safe. Use this to get a
   * consistent instance of the interpolator. It's used to do variable
   * interpolation.
   */
  public static StringSubstitutor getInterpolatorInstance() {
    return StringSubstitutor.createInterpolator()
        .setEnableSubstitutionInVariables(true)
        .setValueDelimiter("|")
        .setEnableUndefinedVariableException(true);
  }

  // when logging the role ARN, mask it. proves there is some data in the
  // string. expects a properly formatted ARN.
  public static String maskAwsRoleArnValue(String roleArn) {
    roleArn = (roleArn == null ? roleArn : roleArn.trim());

    return (StringUtils.isEmpty(roleArn) || roleArn.length() < 7 ? "null"
        : ("*****" + roleArn.substring(3, 7) + "*****"));
  }
}
