package com.advancestores.enterprisecatalog.datafastlane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.context.ConnectionContext;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Attribute;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Container;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;

/**
 * Executes a variety of DataTransformer.transform tests.
 *
 */
class DataTransformerTest {
  private static final Logger log =
      LoggerFactory.getLogger(DataTransformerTest.class);

  /**
   * Couldn't get PowerMockito to properly mock the System class. I found
   * the following method which modifies the class using reflection.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected static void setEnv(Map<String, String> newenv)
      throws Exception {
    try {
      Class<?> processEnvironmentClass =
          Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField =
          processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);

      Map<String, String> env =
          (Map<String, String>) theEnvironmentField.get(null);

      env.putAll(newenv);

      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
          .getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);

      Map<String, String> cienv = (Map<String,
          String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();

      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.clear();
          map.putAll(newenv);
        }
      }
    }
  }

  @Test
  void testLoadDataframe1() {
    DataStore store = new DataStore("src/test/resources/recipe1.yaml");
    DataTransformer.transform(store);
    Dataset<Row> dfRes = store.getFirstDataframe();
    assertNotNull(dfRes, "Dataframe should not be null");
    assertTrue(dfRes.columns().length == 5,
        "Result DF should have 8 columns");
  }

  @Test
  void testLoadDataframe2() {
    DataStore store = new DataStore("src/test/resources/recipe2.yaml");
    DataTransformer.transform(store);
    Dataset<Row> dfRes = store.getFirstDataframe();
    assertTrue(dfRes.columns().length == 8,
        "Result DF should have 8 columns");
  }

  @Test
  void testLoadDataframe3() {
    DataStore store = new DataStore("src/test/resources/recipe3.yaml");
    DataTransformer.transform(store);
    Dataset<Row> dfRes = store.getFirstDataframe();
    assertTrue(dfRes.columns().length == 8,
        "Result DF should have 8 columns");
  }

  @Test
  void testRenameColumnInDataframe() {
    DataStore store =
        new DataStore("src/test/resources/recipe-rename-column.yaml");
    DataTransformer.transform(store);
    Dataset<Row> dfRes = store.getFirstDataframe();
    assertNotNull(dfRes, "Dataframe should not be null");
    // assertTrue(dfRes.columns().length == 8, "Result DF should have 8
    // columns");
  }

  @Test
  void testSampleDataframe() {
    DataStore store =
        new DataStore("src/test/resources/recipe-sample.yaml");
    DataTransformer.transform(store);
    Dataset<Row> dfRes = store.getFirstDataframe();
    assertNotNull(dfRes, "Dataframe should not be null");
    // assertTrue(dfRes.columns().length == 8, "Result DF should have 8
    // columns");
  }

  @Test
  void testSampleSaveDataframe() {
    DataStore store =
        new DataStore("src/test/resources/recipe-sample-save.yaml");
    DataTransformer.transform(store);
    Dataset<Row> dfRes = store.getFirstDataframe();
    assertNotNull(dfRes, "Dataframe should not be null");
    // assertTrue(dfRes.columns().length == 8, "Result DF should have 8
    // columns");
  }

  @Test
  void testSchemaContainer() {
    DataStore store =
        new DataStore("src/test/resources/recipe-custom-schema.yaml");
    DataTransformer.transform(store);
    StructType schemaDef = store.getSchema("buyersguide");

    assertEquals(11, schemaDef.fields().length);

    for (StructField f : schemaDef.fields()) {
      String[] fieldElements = f.name().split("_");
      assertEquals(3, fieldElements.length);

      DataType expectedDataType = Utils.getSparkDataType(fieldElements[0]);
      assertNotNull(expectedDataType);

      boolean expectedNullable =
          (fieldElements[2].contentEquals("t") ? true : false);

      assertTrue(f.dataType().sameType(expectedDataType));
      assertEquals(expectedNullable, f.nullable(), "Field: " + f.name());
    }
  }

  @Test
  void testConnectionContainerNoVariablesSet() {
    try {
      Map<String, String> testEnv = new HashMap<>();
      setEnv(testEnv);

      // creating the store will do variable interpolation
      DataStore store =
          new DataStore("src/test/resources/recipe-connections.yaml");
      DataTransformer.transform(store);
      fail("Should not have successfully performed transformation.");
    } catch (FastLaneException fle) {
      assertTrue(fle.getMessage().contains("Cannot resolve variable"));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  void testEnv() throws Exception {
    Map<String, String> testEnv = new HashMap<>();

    testEnv.put("postgres_host", "postgres_host_value");
    testEnv.put("postgres_port", "postgres_port_value");
    testEnv.put("postgres_user", "postgres_user_value");
    testEnv.put("postgres_pw", "postgres_pw_value");
    testEnv.put("mongo_user", "mongo_user_value");
    testEnv.put("mongo_pw", "mongo_pw_value");
    testEnv.put("custom_pw", "custom_pw_value");
    testEnv.put("aws_node",
        "https://vpc-my-es-lcuqqa6oa5ujhpjcy6efhgxlfgr.us-east-1.es.amazonaws.com");
    setEnv(testEnv);

    System.getProperties().put("mysql_database", "mysql_database_value");

    assertEquals("postgres_host_value", System.getenv("postgres_host"));
    assertEquals("postgres_port_value", System.getenv("postgres_port"));
    assertEquals("postgres_user_value", System.getenv("postgres_user"));
    assertEquals("postgres_pw_value", System.getenv("postgres_pw"));
    assertEquals("mongo_user_value", System.getenv("mongo_user"));
    assertEquals("mongo_pw_value", System.getenv("mongo_pw"));
    assertEquals("custom_pw_value", System.getenv("custom_pw"));
    assertEquals(
        "https://vpc-my-es-lcuqqa6oa5ujhpjcy6efhgxlfgr.us-east-1.es.amazonaws.com",
        System.getenv("aws_node"));
    assertEquals("mysql_database_value",
        System.getProperty("mysql_database"));

    testEnv.remove("postgres_user");
    setEnv(testEnv);

    assertNull(System.getenv("postgres_user"));
  }

  @Test
  void testConnectionContainerWithVariablesSet() throws Exception {
    Map<String, String> testEnv = new HashMap<>();

    testEnv.put("postgres_host", "POSTGRES");
    testEnv.put("postgres_port", "5407");
    testEnv.put("postgres_user", "root");
    testEnv.put("postgres_pw", "rootPW");
    testEnv.put("mongo_user", "mongo-root");
    testEnv.put("mongo_pw", "mongo-rootPW");
    testEnv.put("custom_pw", "customPW");
    testEnv.put("aws_node",
        "https://vpc-my-es-lcuqqa6oa5ujhpjcy6efhgxlfgr.us-east-1.es.amazonaws.com");

    setEnv(testEnv);

    System.getProperties().put("mysql_database", "VCDB");

    assertEquals("POSTGRES", System.getenv("postgres_host"));
    assertEquals("5407", System.getenv("postgres_port"));
    assertEquals("root", System.getenv("postgres_user"));
    assertEquals("rootPW", System.getenv("postgres_pw"));
    assertEquals("mongo-root", System.getenv("mongo_user"));
    assertEquals("mongo-rootPW", System.getenv("mongo_pw"));
    assertEquals("customPW", System.getenv("custom_pw"));
    assertEquals(
        "https://vpc-my-es-lcuqqa6oa5ujhpjcy6efhgxlfgr.us-east-1.es.amazonaws.com",
        System.getenv("aws_node"));
    assertEquals("VCDB", System.getProperty("mysql_database"));

    // creating the store will do variable interpolation
    DataStore store =
        new DataStore("src/test/resources/recipe-connections.yaml");

    DataTransformer.transform(store);
    ConnectionContext jdbc_mysql =
        store.getConnectionInstance("jdbc", "mysql-connection");
    ConnectionContext jdbc_postgres =
        store.getConnectionInstance("jdbc", "postgres-connection");
    ConnectionContext jdbc_mongo =
        store.getConnectionInstance("jdbc", "mongodb-connection");
    ConnectionContext custom_custom = store.getConnectionInstance(
        "custom-connection-type", "custom-connection");
    ConnectionContext aws_elasticsearch =
        store.getConnectionInstance("elasticsearch", "aws");

    assertNotNull(jdbc_mysql);
    assertNotNull(jdbc_postgres);
    assertNotNull(jdbc_mongo);
    assertNotNull(custom_custom);
    assertNotNull(aws_elasticsearch);

    assertEquals("com.mysql.cj.jdbc.Driver", jdbc_mysql.getDriver());
    assertEquals("jdbc:mysql://mysql_host:3306/VCDB", jdbc_mysql.getUrl());
    assertNull(jdbc_mysql.getHost());
    assertNull(jdbc_mysql.getPort());
    assertEquals("mysql_admin", jdbc_mysql.getUsername());
    assertEquals("mysql_pw", jdbc_mysql.getPassword());
    assertNull(jdbc_mysql.getDatabase());

    assertEquals("org.postgresql.Driver", jdbc_postgres.getDriver());
    assertEquals("jdbc:postgres://POSTGRES:5407/postgres_database",
        jdbc_postgres.getUrl());
    assertNull(jdbc_postgres.getHost());
    assertNull(jdbc_postgres.getPort());
    assertEquals("root", jdbc_postgres.getUsername());
    assertEquals("rootPW", jdbc_postgres.getPassword());
    assertNull(jdbc_postgres.getDatabase());

    assertNull(jdbc_mongo.getDriver());
    assertEquals(
        "mongodb://mongo-root:mongo-rootPW@10.17.100.100:2501/mongo_database",
        jdbc_mongo.getUrl());

    assertNull(jdbc_mongo.getHost());
    assertNull(jdbc_mongo.getPort());
    assertNull(jdbc_mongo.getUsername());
    assertNull(jdbc_mongo.getPassword());
    assertNull(jdbc_mongo.getDatabase());

    assertEquals("com.custom.driver", custom_custom.getDriver());
    assertEquals("custom_host", custom_custom.getHost());
    assertEquals("12345", custom_custom.getPort());
    assertEquals("custom_admin", custom_custom.getUsername());
    assertEquals("customPW", custom_custom.getPassword());
    assertEquals("custom_database", custom_custom.getDatabase());

    testEnv.remove("postgres_user");
    setEnv(testEnv);

    assertNull(System.getenv("postgres_user"));

    // creating the store will do variable interpolation
    store = new DataStore("src/test/resources/recipe-connections.yaml");

    DataTransformer.transform(store);
    jdbc_mysql = store.getConnectionInstance("jdbc", "mysql-connection");
    jdbc_postgres =
        store.getConnectionInstance("jdbc", "postgres-connection");
    jdbc_mongo = store.getConnectionInstance("jdbc", "mongodb-connection");
    custom_custom = store.getConnectionInstance("custom-connection-type",
        "custom-connection");
    aws_elasticsearch = store.getConnectionInstance("elasticsearch", "aws");

    assertNotNull(jdbc_mysql);
    assertNotNull(jdbc_postgres);
    assertNotNull(jdbc_mongo);
    assertNotNull(custom_custom);
    assertNotNull(aws_elasticsearch);

    assertEquals("com.mysql.cj.jdbc.Driver", jdbc_mysql.getDriver());
    assertEquals("jdbc:mysql://mysql_host:3306/VCDB", jdbc_mysql.getUrl());
    assertNull(jdbc_mysql.getHost());
    assertNull(jdbc_mysql.getPort());
    assertEquals("mysql_admin", jdbc_mysql.getUsername());
    assertEquals("mysql_pw", jdbc_mysql.getPassword());
    assertNull(jdbc_mysql.getDatabase());

    assertEquals("org.postgresql.Driver", jdbc_postgres.getDriver());
    assertEquals("jdbc:postgres://POSTGRES:5407/postgres_database",
        jdbc_postgres.getUrl());
    assertNull(jdbc_postgres.getHost());
    assertNull(jdbc_postgres.getPort());
    assertEquals("admin", jdbc_postgres.getUsername());
    assertEquals("rootPW", jdbc_postgres.getPassword());
    assertNull(jdbc_postgres.getDatabase());

    assertNull(jdbc_mongo.getDriver());
    assertEquals(
        "mongodb://mongo-root:mongo-rootPW@10.17.100.100:2501/mongo_database",
        jdbc_mongo.getUrl());

    assertNull(jdbc_mongo.getHost());
    assertNull(jdbc_mongo.getPort());
    assertNull(jdbc_mongo.getUsername());
    assertNull(jdbc_mongo.getPassword());
    assertNull(jdbc_mongo.getDatabase());

    assertEquals("com.custom.driver", custom_custom.getDriver());
    assertEquals("custom_host", custom_custom.getHost());
    assertEquals("12345", custom_custom.getPort());
    assertEquals("custom_admin", custom_custom.getUsername());
    assertEquals("customPW", custom_custom.getPassword());
    assertEquals("custom_database", custom_custom.getDatabase());

    assertEquals(
        "https://vpc-my-es-lcuqqa6oa5ujhpjcy6efhgxlfgr.us-east-1.es.amazonaws.com",
        aws_elasticsearch.getHost());
    assertEquals("true", aws_elasticsearch.getEnableWanOnly());
    assertEquals("false", aws_elasticsearch.getEnableNetSsl());
  }

  @Test
  void testContainerWithOptionsVariablesSet() throws Exception {
    Map<String, String> testEnv = new HashMap<>();

    testEnv.put(K.KEY_AWS_REGION, "us-east-1");
    testEnv.put(K.KEY_AWS_ROLE_ARN,
        "arn:aws:iam::111222333444:role/theman");

    setEnv(testEnv);

    System.getProperties().put(K.KEY_DESTINATION,
        "/data/localfiles/myfile.txt");

    assertEquals("us-east-1", System.getenv(K.KEY_AWS_REGION));
    assertEquals("arn:aws:iam::111222333444:role/theman",
        System.getenv(K.KEY_AWS_ROLE_ARN));
    assertEquals("/data/localfiles/myfile.txt",
        System.getProperty(K.KEY_DESTINATION));

    // creating the store will do variable interpolation - should set the
    // option values
    DataStore store = new DataStore(
        "src/test/resources/recipe-with-parameterized-options.yaml");

    DataTransformer.transform(store);

    Container storeContainer = store.getRecipe().getContainer("$store");
    assertNotNull(storeContainer);

    Attribute attribute = storeContainer.getAttribute("books");
    assertNotNull(attribute);

    Operation operation = attribute.getOperation("load");
    assertNotNull(operation);

    assertEquals("us-east-1", operation.getOptionValue(K.KEY_AWS_REGION));
    assertEquals("arn:aws:iam::111222333444:role/theman",
        operation.getOptionValue(K.KEY_AWS_ROLE_ARN));
    assertEquals("/data/localfiles/myfile.txt",
        operation.getOptionValue(K.KEY_DESTINATION));

  }

  /**
   * test the generation of UUID in a recipe
   */
  @Test
  void testGenUuid() {
    DataStore store =
        new DataStore("src/test/resources/recipe-uuid-gen.yaml");
    DataTransformer.transform(store);
    Dataset<Row> dfRes = store.getFirstDataframe();
    long rowCount = dfRes.count();
    long uniqueUuidCount = dfRes.select("uuid").distinct().count();
    assertTrue(rowCount == uniqueUuidCount,
        "Not every row has a unique UUID");
  }

  @Test
  void testDropDataframeFromStoreInDataframe() {
    DataStore store = new DataStore(
        "src/test/resources/recipe-drop-dataframe-from-store.yaml");
    DataTransformer.transform(store);
    int count = store.getDataframeCount();
    log.info("Store contains: {}", store.getPrettyDataframeNames());
    assertTrue(count == 0, "Store should not contain any dataset");
  }

  @Test
  void testDropColumnFromDataframe() {
    DataStore store = new DataStore(
        "src/test/resources/recipe-drop-column-from-dataframe.yaml");
    DataTransformer.transform(store);
    Dataset<Row> dfRes = store.getFirstDataframe();
    assertNotNull(dfRes, "Dataframe should not be null");
    assertTrue(dfRes.columns().length == 4,
        "Result DF should have 4 columns");
  }
}
