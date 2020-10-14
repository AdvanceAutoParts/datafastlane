package com.advancestores.enterprisecatalog.datafastlane;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.context.ConnectionContext;
import com.advancestores.enterprisecatalog.datafastlane.context.SparkContext;
import com.advancestores.enterprisecatalog.datafastlane.operations.CoreOperation;
import com.advancestores.enterprisecatalog.datafastlane.operations.OperationBuilder;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Attribute;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Container;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Option;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Recipe;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;

public class DataTransformer {
  private static final Logger log =
      LoggerFactory.getLogger(DataTransformer.class);

  /**
   * Processes a recipe directly on the store, like loading/dropping
   * dataframes.
   * 
   * @param store
   * @param recipe
   * @return
   */
  private static boolean processRecipeOnStore(DataStore store,
      Container recipe) {
    List<Attribute> attributes = recipe.getAttributes();

    // Apply all transformations, step by step
    for (Attribute attribute : attributes) {
      String attributeName = attribute.getName();
      List<Operation> operations = attribute.getOperations();

      for (Operation operation : operations) {
        CoreOperation op =
            OperationBuilder.build(store, attributeName, operation);
        boolean status = op.run();
        if (!status) {
          log.error(
              "An error occured in the previous operation, stopping the recipe.");
          return false;
        }

      }
    }
    return true;
  }

  private static boolean processRecipeOnTable(DataStore store,
      String tableName, Container recipe) {
    List<Attribute> transformations = recipe.getAttributes();

    // Apply all transformations, step by step
    for (Attribute transformation : transformations) {
      String columnName = transformation.getName();
      List<Operation> operations = transformation.getOperations();

      for (Operation operation : operations) {
        CoreOperation op =
            OperationBuilder.build(store, tableName, columnName, operation);
        boolean status = op.run();
        if (!status) {
          log.error(
              "An error occured in the previous operation, stopping the recipe.");
          return false;
        }
      } // end of applying transformations
    }

    return true;
  }

  protected static SparkContext configureSpark(DataStore store,
      Container container) {
    SparkContext ctx = new SparkContext();

    // make sure some defaults are set in case there is no $spark section
    ctx.setAppName(K.APP_NAME_DEFAULT);
    ctx.setMaster(K.MASTER_DEFAULT);
    ctx.addProperty(K.KEY_SPARK_EXECUTOR_MEMORY, K.EXECUTOR_MEMORY_DEFAULT);
    ctx.addProperty(K.KEY_SPARK_MEMORY_OFFHEAP_ENABLED,
        K.MEMORY_OFF_HEAP_ENABLED_DEFAULT);
    ctx.addProperty(K.KEY_SPARK_MEMORY_OFFHEAP_SIZE,
        K.MEMORY_OFF_HEAP_SIZE_DEFAULT);

    if (container != null) {
      Attribute session = container.getAttribute("session");

      if (session != null) {
        Operation operation = session.getFirstOperation();

        if (operation != null) {
          ctx.setAppName(operation.getAppName() != null
              ? operation.getAppName() : K.APP_NAME_DEFAULT);
          ctx.setMaster(operation.getMaster() != null
              ? operation.getMaster() : K.MASTER_DEFAULT);

          for (Option option : operation.getOptions()) {
            ctx.addProperty(option);
          }
        } else {
          log.info("No operations defined for the Spark container.");
        }
      } else {
        log.info("No session attribute defined for the Spark container.");
      }
    } else {
      log.info("Spark container is null.");
    }

    return ctx;
  }

  /**
   * A custom schema consists of one "attribute" and multiple "operations"
   * where each operation will have on required and one optional "option".
   * The "attribute" identifies the name of the custom schema. Each
   * "operation" identifies the name of a field. A field will have a
   * required "option" for datatype and an optional "option" for whether the
   * field is nullable (true or false). Nullable will be true when set to
   * "true" and false in all other cases.
   * 
   * Multiple custom schemas may be defined. They are used by the load
   * operation.
   * 
   * @param DataStore
   *          store - the object that stores information and datasets loaded
   *          by the Spark process
   * @param Container
   *          container - "container" section of a recipe file
   * 
   */
  protected static void processCustomSchema(DataStore store,
      Container container) {
    // TODO we need to unify our error processing, either exceptions or
    // status or something else
    StructType customSchema = null;
    String schemaName = null;

    if (store != null && container != null) {
      String containerName = container.getContainerName();

      log.trace("\nProcessing Container:\n{}", container);

      if (!container.getAttributes().isEmpty()) {
        for (Attribute schemaAttribute : container.getAttributes()) {
          List<StructField> structFieldList = new ArrayList<>();

          schemaName = schemaAttribute.getName();

          log.debug("Container info for {}", containerName);
          log.debug("    Schema Name: {}", schemaName);
          log.debug("    Field Count: {}",
              schemaAttribute.getOperations().size());

          // an operation === field; each field should have a datatype
          // option and maybe a
          // nullable option
          for (Operation operation : schemaAttribute.getOperations()) {
            String fieldName = operation.getName();
            String dataType = operation.getOptionValue("datatype");
            String nullable = operation.getOptionValue("nullable");

            if (StringUtils.isNotEmpty(fieldName)
                && StringUtils.isNotEmpty(dataType)) {
              nullable =
                  (StringUtils.isEmpty(nullable) ? "false" : nullable);

              log.debug("        Field Name: " + fieldName + "\tData Type: "
                  + dataType + "\tNullable: " +
                  nullable);

              structFieldList.add(
                  Utils.getSparkStructField(fieldName, dataType, nullable));
            } else {
              String errMsg =
                  "Malformed schema field. The name and data type are required for schema fields. Field Name: "
                      +
                      fieldName + "  Data Type: " + dataType;
              log.error(errMsg);
              throw new FastLaneException(errMsg);
            }
          }

          log.debug("Creating Spark structure with data for {} fields.",
              structFieldList.size());

          customSchema = DataTypes.createStructType(structFieldList
              .toArray(new StructField[structFieldList.size()]));

          // save the custom schema to the DataStore object
          store.addSchema(schemaName, customSchema);
        }
      } else {
        String errMsg =
            "Malformed custom schema. There is a 'schema' section, but the 'attribute' property is missing.";
        log.error(errMsg);
        throw new FastLaneException(errMsg);
      }
    } else {
      String errMsg =
          "Missing parameters. The 'store' and 'container' objects cannot be null.  Store null: "
              +
              (store == null) + "  Container null: " + (container == null);

      log.error(errMsg);
      throw new FastLaneException(errMsg);
    }
  }

  /**
   * A connection container may contain multiple attribute tags. Each
   * attribute represents a "type" of connection, e.g. "jdbc". An attribute
   * may have multiple operations where an operation represents an instance
   * of that type of connection, e.g. for a jdbc type, there could be
   * multiple mysql instances or a mysql, postgres, etc. For each operation,
   * the connection options are provided, e.g. driver, host, port, etc.
   * 
   * Variable interpolation is supported for connection properties. The
   * values can be resolved from constants, system properties, environment
   * variables, dns, script, etc. Defaults may be added using the pipe
   * character followed by a value to used.
   * 
   * For more details of valid prefixes, see
   * https://commons.apache.org/proper/commons-text/apidocs/org/apache/commons/text/lookup/StringLookupFactory.html#dnsStringLookup
   * 
   * Examples: ${sys:var_name} --> reads from system properties
   * ${env:var_name} --> reads from environment variables
   * ${env:var_name|DEFAULT_VALUE} --> if 'var_name' is not found, use
   * 'DEFAULT_VALUE' ${java:var_name} --> reads from Java platform variables
   * 
   * The default value suffix is optional and can be added in case the
   * environment variable is not set.
   * 
   * @param DataStore
   *          store - the object that stores information and datasets loaded
   *          by the Spark process
   * @param Container
   *          container - "container" section of a recipe file
   * 
   */
  protected static void processConnection(DataStore store,
      Container container) {
    // TODO we need to unify our error processing, either exceptions or
    // status or something else
    if (store != null && container != null) {
      String containerName = container.getContainerName();

      log.trace("\nProcessing Container:\n{}", container);

      if (!container.getAttributes().isEmpty()) {
        // each "attribute" represents a connection type, e.g. jdbc
        for (Attribute connectionAttribute : container.getAttributes()) {
          String connectionType = connectionAttribute.getName();

          log.debug("Container info for {}", containerName);
          log.debug("    Connection Type: {}", connectionType);
          log.debug("    Instance Count: {}",
              connectionAttribute.getOperations().size());

          // an operation === connection instance; each instance will have
          // properties
          // defined by "option" tags
          for (Operation operation : connectionAttribute.getOperations()) {
            String instanceName = operation.getName();
            ConnectionContext ctx = new ConnectionContext();

            for (Option option : operation.getOptions()) {
              ctx.addProperty(option);
            }

            log.debug("\nConnection Context:\n{}", ctx);

            store.addConnection(connectionType, instanceName, ctx);
          }
        }
      } else {
        String errMsg =
            "Malformed connection. There is a 'connection' section, but the 'attribute' property is missing.";
        log.error(errMsg);
        throw new FastLaneException(errMsg);
      }
    } else {
      String errMsg =
          "Missing parameters. The 'store' and 'container' objects cannot be null.  Store null: "
              +
              (store == null) + "  Container null: " + (container == null);

      log.error(errMsg);
      throw new FastLaneException(errMsg);
    }
  }

  /**
   * The recipe file is processed from top to bottom. Containers that are
   * used as references by other containers should be listed near the top of
   * the file. This includes $spark, $connection, and $schema. The $import
   * can be included multiple times and in any location. The $store
   * container may be included multiple times; however, it should appear
   * after any of the reference containers.
   */
  public static boolean transform(DataStore store) {
    log.debug("-> transform()");
    boolean status;

    Recipe recipe = store.getRecipe();
    int i = 0;
    for (Container container : recipe.getContainers()) {
      String containerName = container.getContainerName();

      log.debug("Processing rule #{}/{} on '{}'", i++, recipe.getCount(),
          containerName);

      switch (containerName) {
        case K.STORE_NAME:
          status = processRecipeOnStore(store, container);
          if (!status) {
            log.error(
                "An error occured on a store operation, stopping further processing of the recipe.");
            return false;
          }
          break;
        case K.SCHEMA:
          processCustomSchema(store, container);
          break;
        case K.CONNECTION:
          processConnection(store, container);
          break;
        case K.SPARK:
          // this will already have been processed when the Store object was
          // created. No need to
          // process again just for logging purposes.
          log.debug("Skipping {} section - already loaded.", K.SPARK);
          break;
        case K.IMPORT:
          // the preprocessor should have already replaced all imports;
          // however, just in case, flag it
          log.warn(
              "An unprocessed $import tag was found. This is unexpected. {}",
              containerName);
          break;
        default:
          status = processRecipeOnTable(store, containerName, container);
          if (!status) {
            log.error(
                "An error occured on a dataframe operation, stopping further processing of the recipe.");
            return false;
          }
      }
    }

    return true;
  }

}
