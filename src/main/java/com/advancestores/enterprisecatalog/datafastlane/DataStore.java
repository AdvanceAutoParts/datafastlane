package com.advancestores.enterprisecatalog.datafastlane;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.context.ConnectionContext;
import com.advancestores.enterprisecatalog.datafastlane.context.SparkContext;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Recipe;
import com.advancestores.enterprisecatalog.datafastlane.recipe.RecipeReader;
import com.advancestores.enterprisecatalog.datafastlane.spark_udf.UuidGeneratorUdf;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;

public class DataStore {
  private static final Logger log =
      LoggerFactory.getLogger(DataStore.class);

  private static final String TEMP_FILE_PREFIX = "__dfl__pp__";

  private Map<String, Dataset<Row>> store = null;
  private BigDecimal datapoints = new BigDecimal(1);
  private SparkSession spark = null;
  private String firstDataframeName = null;
  private Map<String, StructType> schemaStore;
  private Map<String, Map<String, ConnectionContext>> connectionStore;
  private Recipe recipe = null;
  private String rawRecipePath = null; // recipe file which may contain
                                       // $import references
  private String recipePath = null; // preprocessed version of recipe
                                    // where $import refs are replaced

  public DataStore(String rawRecipePath) {
    log.info("Processing recipe file: {}", rawRecipePath);

    // Basic init
    store = new HashMap<>();
    schemaStore = new HashMap<>();
    connectionStore = new HashMap<>();
    this.rawRecipePath = rawRecipePath;

    // this may be a recursive process. The final call will set the
    // final recipe path value
    preprocessRecipe(this.rawRecipePath);

    recipe = RecipeReader.read(recipePath);

    log.debug("\nRECIPE: {}\n", recipe);

    int recipeCount = recipe.getContainers().size();
    log.debug("{} rules to apply:\n{}", recipeCount, recipe.debug());

    // we can remove temp recipe files
    try {
      if (recipePath.contains(TEMP_FILE_PREFIX)) {
        log.debug("Scheduling file {} for removal.", recipePath);
        FileUtils.forceDeleteOnExit(new File(recipePath));
      }
    } catch (Exception e) {
      log.error("The temporary file {} was not deleted.", recipePath);
    }

    // if there is a '$spark' container, see if any interesting session
    // options were provided
    SparkContext sparkCtx =
        DataTransformer.configureSpark(null, recipe.getContainer(K.SPARK));

    log.debug("\nSPARK CONTEXT: \n{}", sparkCtx);

    this.spark = SparkSession.builder()
        .appName(sparkCtx.getAppName())
        .config("spark.executor.memory", sparkCtx.getExecutorMemory())
        .config("spark.memory.offHeap.enabled",
            sparkCtx.getMemoryOffHeapEnabled())
        .config("spark.memory.offHeap.size",
            sparkCtx.getMemoryOffHeapSize())
        .master(sparkCtx.getMaster())
        .getOrCreate();

    // Register UDFs here
    this.spark.udf().register("genUuid", new UuidGeneratorUdf(),
        DataTypes.StringType);

    log.debug("DataStore is ready");
  }

  /**
   * Read the raw recipe file and apply preprocessing rules. This will be
   * called recursively so all fragments, from original or other fragment,
   * are replaced. The following rules are in force:
   * 
   * 1. replace $import tags with the contents of the referenced file
   * 
   */
  private void preprocessRecipe(String activeRecipePath) {
    log.debug("Preprocessing recipe file: {}", activeRecipePath);

    FastLaneException fle = Utils.checkFile(activeRecipePath);
    if (fle != null) {
      log.error(fle.getMessage());
      throw fle;
    }

    if (Utils.existsInFile(activeRecipePath, K.IMPORT)) {
      // if successful, this will be set to the processed path value
      recipePath = null;

      // get the name of the raw recipe file path, use the filename
      // part, create a new file in the
      // temp directory that will be used for substitution and DFL
      // processing, i.e. do not change
      // the original recipe file. Remove the temp file when finished.
      String origFilename =
          Paths.get(rawRecipePath).getFileName().toString();
      String wipRecipePath = FileUtils.getTempDirectoryPath()
          + TEMP_FILE_PREFIX + UUID.randomUUID() + "_" +
          origFilename;

      log.debug(
          "Original Recipe Filename: {}\nFile Path Created by Preprocessor: {}",
          origFilename,
          wipRecipePath);

      StringSubstitutor interpolator = Utils.getInterpolatorInstance();
      String prefix = K.IMPORT + ":";
      int embeddedFragmentCnt = 0;

      try (
          BufferedWriter writer =
              new BufferedWriter(new FileWriter(wipRecipePath));
          BufferedReader reader =
              new BufferedReader(new FileReader(activeRecipePath))) {

        String line;
        int fragmentCnt = 0;

        while ((line = reader.readLine()) != null) {
          if (line.trim().startsWith(prefix)) {
            String[] importParts = line.split(":");

            // should have the format of "$import: <some file path>". the
            // path may have variables
            if (importParts.length != 2) {
              String errMsg =
                  "The import tag has an invalid format: {}" + line;
              log.error(errMsg);
              throw new FastLaneException(errMsg);
            }

            // support variable interpolation for path names, e.g.
            // $import: ${env:fragments_dir}/fragment1.yaml
            String fragmentPath =
                interpolator.replace(importParts[1].trim());

            fle = Utils.checkFile(fragmentPath);
            if (fle != null) {
              log.error(fle.getMessage());
              throw fle;
            }

            try (BufferedReader fragReader =
                new BufferedReader(new FileReader(fragmentPath))) {
              String fragLine = null;

              while ((fragLine = fragReader.readLine()) != null) {
                String trimmedFragLine = fragLine.trim();

                if (!trimmedFragLine.isEmpty()) {
                  embeddedFragmentCnt +=
                      (trimmedFragLine.startsWith(K.IMPORT) ? 1 : 0);
                  writer.write(fragLine + System.lineSeparator());
                }
              }

              fragmentCnt++;
            } catch (Exception fe) {
              String errMsg =
                  "An error occurred while writing fragment lines to recipe file";
              log.error(errMsg);
              throw new FastLaneException(errMsg);
            }
          } else {
            writer.write(line + System.lineSeparator());
          }
        }

        log.debug("Fragments Processed: {}", fragmentCnt);
      } catch (FileNotFoundException fnfe) {
        throw new FastLaneException(fnfe.getMessage());
      } catch (FastLaneException fle2) {
        throw fle2;
      } catch (Exception e2) {
        throw new FastLaneException(e2.getMessage());
      }

      // if the last file was a temp file, it can removed, i.e.
      // intermediate temp files are not needed
      if (activeRecipePath.contains(TEMP_FILE_PREFIX)) {
        log.debug("Deleting temp file: {}", activeRecipePath);

        try {
          FileUtils.forceDelete(new File(activeRecipePath));
        } catch (Exception e) {
          log.error(
              "An error occurred while deleting temp file {}.\nCause: {}",
              activeRecipePath,
              e.getMessage());
        }
      }

      if (embeddedFragmentCnt > 0) {
        log.debug(
            "Embedded Fragments: {}\nRecursively calling preprocess for: {}",
            embeddedFragmentCnt,
            wipRecipePath);

        preprocessRecipe(wipRecipePath);
      } else {
        recipePath = wipRecipePath;
      }
    } else {
      // if no $import statement, use the input file
      recipePath = activeRecipePath;

      log.trace("No import statement in file {}", recipePath);
    }
  }

  public DataStore add(String tableName, Dataset<Row> df) {
    // Quick check if this is a new dataframe, if so, we keep the
    // metrics.
    Dataset<Row> df0 = this.store.get(tableName);

    if (df0 == null) {
      BigDecimal newDatapoints = new BigDecimal(df.columns().length)
          .multiply(new BigDecimal(df.count()));
      this.datapoints = this.datapoints.add(newDatapoints);
    }

    this.store.put(tableName, df);

    if (firstDataframeName == null) {
      firstDataframeName = tableName;
    }

    return this;
  }

  public Dataset<Row> get(String tableName) {
    return this.store.get(tableName);
  }

  public int getDataframeCount() {
    return store.size();
  }

  public BigDecimal getDatapoints() {
    return this.datapoints;
  }

  public String getHumanDatapoints() {
    BigDecimal l = this.datapoints;
    BigDecimal t = new BigDecimal(1000);
    int exp = 0;

    while (l.compareTo(t) > 0) {
      l = l.divide(t);
      exp += 3;
    }

    float i = l.multiply(t).intValue();

    return (i / 1000) + " * 10^" + exp;
  }

  public String getPrettyDataframeNames() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;

    for (String name : store.keySet()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      sb.append(name);
    }

    return sb.toString();
  }

  public void loadFromDeltaLake(String dataframeName, String location) {
    Dataset<Row> df = spark.read().format("delta").load("/tmp/" + location);
    add(dataframeName, df);
  }

  public void remove(String dataframeName) {
    this.store.remove(dataframeName);
  }

  public void writeToDeltaLake(String dataframeName, String location) {
    Dataset<Row> df = get(dataframeName);
    df.write().format("delta").option("overwriteSchema", true)
        .mode("overwrite").save("/tmp/" + location);
  }

  public Dataset<Row> getFirstDataframe() {
    return get(this.firstDataframeName);
  }

  public void addSchemaStore(Map<String, StructType> schemaStore) {
    // if the schemaStore is null, assume it is an inferred schema
    if (schemaStore != null) {
      this.schemaStore.putAll(schemaStore);
    }
  }

  public void addSchema(String schemaName, StructType schemaDef) {
    if (StringUtils.isNotEmpty(schemaName) && schemaDef != null) {
      schemaStore.put(schemaName, schemaDef);
    }
  }

  /**
   * Store data by connection type, e.g. jdbc, and instances of that type,
   * e.g. mysql:driver, host, port
   * 
   * @param String
   *          connectionType - type of connection, e.g. jdbc
   * @param String
   *          instanceName - name of connection instance, e.g. mysql
   * @param ConnectionContext
   *          instanceContext - properties of the connection instance;
   *          includes driver, port, etc.
   */
  public void addConnection(String connectionType, String instanceName,
      ConnectionContext instanceContext) {
    if (StringUtils.isNotEmpty(connectionType)
        && StringUtils.isNotEmpty(instanceName)
        && instanceContext != null) {
      Map<String, ConnectionContext> instances =
          connectionStore.get(connectionType);

      if (instances == null) {
        instances = new HashMap<>();
      }

      instances.put(instanceName, instanceContext);
      connectionStore.put(connectionType, instances);
    }
  }

  public String getRawRecipePath() {
    return rawRecipePath;
  }

  public String getRecipePath() {
    return recipePath;
  }

  public Recipe getRecipe() {
    return this.recipe;
  }

  public SparkSession getSparkSession() {
    return this.spark;
  }

  public StructType getSchema(String schemaName) {
    return (StringUtils.isNotEmpty(schemaName) ? schemaStore.get(schemaName)
        : null);
  }

  /**
   * Grab a specific connection instance for a given connection type, e.g.
   * jdbc:mysql --> context object.
   * 
   * @param String
   *          connectionType - type of connection e.g. jdbc (represented by
   *          attribute tag)
   * @param String
   *          instanceName - specific instance of a connection type e.g.
   *          mysql (represented by operation tag)
   * 
   * @return ConnectionContext - properties of a specific connection
   *         instance (driver, host, etc)
   */
  public ConnectionContext getConnectionInstance(String connectionType,
      String instanceName) {
    ConnectionContext connectionInstance = null;

    if (StringUtils.isNotEmpty(connectionType)
        && StringUtils.isNotEmpty(instanceName)) {
      Map<String, ConnectionContext> instances =
          connectionStore.get(connectionType);

      if (instances != null) {
        connectionInstance = instances.get(instanceName);
      }
    }

    return connectionInstance;
  }

  public void drop(String dataframeName) {
    if (dataframeName == null) {
      return;
    }

    // Free the cache (just in case)
    Dataset<Row> df = get(dataframeName);
    if (df != null) {
      df.unpersist();
    }

    // remove from map
    this.store.remove(dataframeName);
  }


  public String expandProperties(String valueToExpand) {
    Pattern p = Pattern.compile("\\$\\{sys:(.+?)\\}");
    Matcher m = p.matcher(valueToExpand);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String key = m.group(1);
      if (key != null) {
                String value = System.getProperty(key);
        if (value != null) {
          m.appendReplacement(sb, value);
        }
      }
    }
    m.appendTail(sb);
    return sb.toString();
  }

}
