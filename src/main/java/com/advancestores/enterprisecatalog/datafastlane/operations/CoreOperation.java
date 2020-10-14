package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.K;
import com.advancestores.enterprisecatalog.datafastlane.context.AwsCredentialsContext;
import com.advancestores.enterprisecatalog.datafastlane.context.ConnectionContext;
import com.advancestores.enterprisecatalog.datafastlane.context.ReaderContext;
import com.advancestores.enterprisecatalog.datafastlane.context.WriterContext;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Option;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Options;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;

public abstract class CoreOperation implements Operationable {
  private static final Logger log =
      LoggerFactory.getLogger(CoreOperation.class);

  private Operation operationDefinition;
  private DataStore store;
  private String containerName;
  private String attributeName;

  public abstract boolean run();

  public void setStore(DataStore store) {
    this.store = store;
  }

  protected ReaderContext buildReader(SparkSession session) {
    // example formats support by Spark: csv, jdbc, text, orc, parquet, json
    String format = operationDefinition.getFormat();
    ReaderContext readerCtx = new ReaderContext();

    if (StringUtils.isEmpty(format)) {
      String errMsg = "The data format was not specified. Cannot continue.";
      log.error(errMsg);
      throw new FastLaneException(errMsg);
    } else {
      log.debug("Processing data format: {}", format);

      DataFrameReader reader = session.read().format(format);
      Options options = operationDefinition.getOptions();

      String dataframeName = getAttributeName();
      String connectionName = options.getOptionAsString("connection");
      String filePath = null;

      // if it uses a connection reference, look it up; if not a connection,
      // assume file
      if (StringUtils.isNotEmpty(connectionName)) {
        ConnectionContext ctx =
            store.getConnectionInstance(format, connectionName);

        if (ctx != null) {
          // map DFL properties to Spark-named properties - by default, the
          // table to load
          // is the same name as the dataframe; however, that can be
          // overridden using the
          // source option
          String source = options.getOptionAsString("source");
          reader = reader.option("dbtable",
              (StringUtils.isNotEmpty(source) ? source : dataframeName));

          // copy context properties to reader options
          for (String key : ctx.getPropertyKeys()) {
            reader.option(key, ctx.getProperty(key));
          }

          log.debug("\nReader context for table " + dataframeName
              + " using connection reference " + format +
              ":" + connectionName);
        } else {
          throw new FastLaneException(
              "Unable to find Connection reference '" + format + ":" +
                  connectionName + "'.");
        }
      } else {
        // if the path is an s3 file, make a local copy before processing.
        // the roleArn and region should
        // be defined as options. the destination path may also be specified
        // as an option
        filePath = Utils.downloadS3File(operationDefinition.getPath(),
            options.getOptionAsString(K.KEY_DESTINATION),
            new AwsCredentialsContext(
                options.getOptionAsString(K.KEY_AWS_ACCESS_KEY_ID),
                options.getOptionAsString(K.KEY_AWS_SECRET_ACCESS_KEY),
                options.getOptionAsString(K.KEY_AWS_SESSION_TOKEN),
                options.getOptionAsString(K.KEY_AWS_REGION),
                options.getOptionAsString(K.KEY_AWS_ROLE_ARN)));

        log.debug("\nLoading file {} using path: {}\n", dataframeName,
            filePath);
      }

      // if a custom schema is required, pull it from the schema data store
      StructType schema =
          store.getSchema(operationDefinition.getSchemaName());

      if (schema != null) {
        reader = reader.schema(schema);
      } else {
        log.debug("Unable to find a schema object with name '"
            + operationDefinition.getSchemaName() +
            "'. No custom schema will be used.");
      }

      // add any other options that may have been defined
      for (Option option : options) {
        reader = reader.option(option.getKey(), option.getValue());
      }

      readerCtx = readerCtx.setReader(reader)
          .setFilePath(store.expandProperties(filePath))
          .setDataframeName(dataframeName)
          .setConnectionName(connectionName);
    }

    return readerCtx;
  }

  protected WriterContext buildWriter(Dataset<Row> dataframe) {
    // example formats supported by Spark:
    // csv, jdbc, text, orc, parquet, json, org.elasticsearch.spark.sql
    String format = operationDefinition.getFormat();

    if (StringUtils.isEmpty(format)) {
      String errMsg = "The data format was not specified. Cannot continue.";
      log.error(errMsg);
      throw new FastLaneException(errMsg);
    }

    Options options = operationDefinition.getOptions();
    String destination = operationDefinition.getPath();

    // The path property identifies the destination of the save. if format
    // is a file-type, such as csv,
    // it will be the actual path of the file to be created. If jdbc, it
    // will be a table name. If Elasticsearch
    // it will be an index name
    if (StringUtils.isEmpty(destination)) {
      String errMsg =
          "The destination path was not specified. Cannot continue.";
      log.error(errMsg);
      throw new FastLaneException(errMsg);
    }

    log.debug("Processing data format: {}\nDestination: {}", format,
        destination);

    DataFrameWriter<Row> writer;
    String dataframeName = getAttributeName();
    String connectionName = options.getOptionAsString("connection");
    String headerStr = options.getOptionAsString("header");
    boolean hasHeader = (StringUtils.isEmpty(headerStr) ? false
        : ("true".equals(headerStr.trim().toLowerCase())));

    // if it uses a connection reference, look it up; if not a connection,
    // assume file
    if (StringUtils.isNotEmpty(connectionName)) {
      ConnectionContext ctx =
          store.getConnectionInstance(format, connectionName);

      if (ctx != null) {
        log.debug("\nWriter context for table " + dataframeName
            + " using connection reference " + format +
            ":" + connectionName);

        writer = dataframe.write().format(format);

        // jdbc wants dbtable for the table name - the value is stored in
        // the destination option
        writer = writer.option("dbtable", destination);

        // copy context properties to reader options
        for (String key : ctx.getPropertyKeys()) {
          writer.option(key, ctx.getProperty(key));
        }
      } else {
        throw new FastLaneException("Unable to find Connection reference '"
            + format + ":" + connectionName +
            "'.");
      }
    } else {
      log.debug(
          "\nWriter context for file {} of format {} will be coalesced into a single file.",
          dataframeName,
          format);

      // this will be a file write...use coalesce to force a single file for
      // mergable types
      if (WriterContext.isMergable(format)) {
        log.debug("Writing to a mergable file type: {}", format);
        writer = dataframe.coalesce(1).write().format(format);
      } else {
        writer = dataframe.write().format(format);
      }
    }

    // add any other options that may have been defined
    for (Option option : options) {
      writer = writer.option(option.getKey(), option.getValue());
    }

    writer.mode(Utils.getSparkSaveMode(operationDefinition.getMode()));

    // the dataframe is passed in because "save to text" requires some data
    // manipulation
    return new WriterContext().setWriter(writer)
        .setDestination(destination)
        .setDataframeName(dataframeName)
        .setConnectionName(connectionName)
        .setHasHeader(hasHeader)
        .setFormat(format)
        .setDataframe(dataframe);
  }

  public Operation getOperationDefinition() {
    return operationDefinition;
  }

  public DataStore getStore() {
    return store;
  }

  public String getContainerName() {
    return containerName;
  }

  public String getAttributeName() {
    return attributeName;
  }

  public void setContainerName(String containerName) {
    this.containerName = containerName;
  }

  public void setAttributeName(String attributeName) {
    this.attributeName = attributeName;
  }

  public void setOperationDefinition(Operation operationDefinition) {
    this.operationDefinition = operationDefinition;
  }

  /**
   * Returns true if working at the store level ($store).
   * 
   * @return
   */
  public boolean isStore() {
    return (StringUtils.isEmpty(containerName) ? true
        : containerName.trim().toLowerCase().equals(K.STORE_NAME));
  }

}
