package com.advancestores.enterprisecatalog.datafastlane.context;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.K;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;
import com.amazonaws.services.s3.internal.BucketNameUtils;

/**
 * Takes an S3 path and breaks it down into components.
 * 
 * Example:
 * 
 * input: s3://test-data/recipes/fragments/recipe.yaml output: prefix =
 * s3:// bucketName = test-data path = /recipes/fragments/recipe.yaml
 * fileName = recipe.yaml
 *
 */
public class S3Path {
  private static final Logger log = LoggerFactory.getLogger(S3Path.class);

  private String prefix;
  private String bucketName;
  private String path;
  private String fileName;

  public S3Path(String s3Path) {
    if (isValidFormat(s3Path)) {
      int prefixIndex = s3Path.indexOf(K.S3_PREFIX) + K.S3_PREFIX.length();
      int bucketIndex = s3Path.indexOf('/', prefixIndex);
      int lastSlashIndex = s3Path.lastIndexOf('/');

      prefix = s3Path.substring(0, prefixIndex);
      bucketName = s3Path.substring(prefixIndex, bucketIndex);
      path = s3Path.substring(bucketIndex + 1);
      fileName = s3Path.substring(lastSlashIndex + 1);

      // make sure the bucket has a valid format
      if (!BucketNameUtils.isValidV2BucketName(bucketName)) {
        String errMsg = "The S3 bucket name is not valid.";

        log.error(errMsg);
        throw new FastLaneException(errMsg);
      }
    } else {
      String errMsg = "S3 path with value '" + s3Path + "' is malformed.";

      log.error(errMsg);
      throw new FastLaneException(errMsg);
    }
  }

  /**
   * At a minimum, expects the format to be: s3://<bucket name>/<file name>
   */
  private static boolean isValidFormat(String s3Path) {
    boolean retval = false;

    if (StringUtils.isEmpty(s3Path)) {
      log.error("S3 path string is empty.");
    } else if (!s3Path.startsWith(K.S3_PREFIX)) {
      log.error(
          "S3 path string does not begin with '" + K.S3_PREFIX + "'.");
    } else if (StringUtils.countMatches(s3Path, "/") < 3) {
      log.error(
          "S3 path string does not contain enough path separators, i.e. missing path components.");
    } else if (StringUtils.endsWith(s3Path, "/")) {
      log.error("S3 path string should not end with a path separator.");
    } else if (StringUtils.countMatches(s3Path, "///") > 0) {
      log.error(
          "S3 path string should not use triple consecutive path separators.");
    } else if (StringUtils.countMatches(s3Path, "//") > 1) {
      log.error(
          "S3 path string should not use multiple consecutive path separators.");
    } else {
      log.debug("S3path string has a valid format.");
      retval = true;
    }

    return retval;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getPath() {
    return path;
  }

  public String getFileName() {
    return fileName;
  }

  public String toString() {
    return Utils.getGson().toJson(this);
  }
}
