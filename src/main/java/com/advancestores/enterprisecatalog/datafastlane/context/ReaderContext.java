package com.advancestores.enterprisecatalog.datafastlane.context;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;
import com.google.gson.annotations.Expose;

/**
 * Stores information surrounding a DataFrameReader. The goal was to use the
 * DataFrameReader options map to store extra things; however, the property
 * is private. Things can be put in, but not read out.
 */
public class ReaderContext {
  private static final Logger log =
      LoggerFactory.getLogger(ReaderContext.class);

  private DataFrameReader reader;
  @Expose()
  private String filePath;
  @Expose()
  private String dataframeName;
  @Expose()
  private String connectionName;

  public ReaderContext() {
  }

  public DataFrameReader getReader() {
    return reader;
  }

  public Dataset<Row> load() {
    Dataset<Row> dataframe = null;
    long loadStart = System.currentTimeMillis();

    try {
      if (filePath == null) {
        log.debug(">>>> Loading {} using connection reference {}",
            dataframeName, connectionName);
        dataframe = reader.load();
      } else {
        log.debug(">>>> Loading {} from file {}", dataframeName, filePath);
        dataframe = reader.load(filePath);
      }

      log.debug(">>>> Load completed in {} seconds",
          ((System.currentTimeMillis() - loadStart) / 1000));
    } catch (Exception e) {
      String errMsg = "An error occurred while loading:\n" + toString()
          + "\nError: " + e.getMessage();
      log.error(errMsg);
      throw new FastLaneException(errMsg);
    }

    return dataframe;
  }

  public ReaderContext setReader(DataFrameReader reader) {
    this.reader = reader;
    return this;
  }

  public String getFilePath() {
    return filePath;
  }

  public ReaderContext setFilePath(String filePath) {
    this.filePath = filePath;
    return this;
  }

  public String getDataframeName() {
    return dataframeName;
  }

  public ReaderContext setDataframeName(String dataframeName) {
    this.dataframeName = dataframeName;
    return this;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public ReaderContext setConnectionName(String connectionName) {
    this.connectionName = connectionName;
    return this;
  }

  public String toString() {
    return Utils.getGsonExpose().toJson(this);
  }
}
