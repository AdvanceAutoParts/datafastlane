package com.advancestores.enterprisecatalog.datafastlane;

public abstract class K {

  public static final String SUCCESS = "SUCCESS";
  public static final String FAILED = "FAILED";

  public static final String STORE_NAME = "$store";
  protected static final String SPARK = "$spark";
  protected static final String SCHEMA = "$schema";
  protected static final String CONNECTION = "$connection";
  protected static final String IMPORT = "$import";

  // some predefined keys used by SparkContext
  public static final String KEY_SPARK_EXECUTOR_MEMORY =
      "spark.executor.memory";
  public static final String KEY_SPARK_MEMORY_OFFHEAP_ENABLED =
      "spark.memory.offHeap.enabled";
  public static final String KEY_SPARK_MEMORY_OFFHEAP_SIZE =
      "spark.memory.offHeap.size";

  protected static final String APP_NAME_DEFAULT = "Data in the Fast Lane";
  protected static final String EXECUTOR_MEMORY_DEFAULT = "8g";
  protected static final String MEMORY_OFF_HEAP_ENABLED_DEFAULT = "true";
  protected static final String MEMORY_OFF_HEAP_SIZE_DEFAULT = "32g";
  protected static final String MASTER_DEFAULT = "local[*]";

  public static final String S3_PREFIX = "s3://";

  // AWS option keys
  public static final String KEY_AWS_ROLE_ARN = "aws_role_arn";
  public static final String KEY_AWS_REGION = "aws_region";
  public static final String KEY_AWS_ACCESS_KEY_ID = "aws_access_key_id";
  public static final String KEY_AWS_SECRET_ACCESS_KEY =
      "aws_secret_access_key";
  public static final String KEY_AWS_SESSION_TOKEN = "aws_session_token";

  // general option keys
  public static final String KEY_SOURCE = "source";
  public static final String KEY_DESTINATION = "destination";
  public static final String DISTINCT = "distinct";
  public static final String NESTING_COLUMN_SUFFIX = "Details";
  public static final String STRUCT_NAME = "structName";
  public static final String LIST_NAME_SUFFIX = "s";
  public static final String LIST_NAME = "listName";

}
