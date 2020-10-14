package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.context.ReaderContext;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

public class LoadOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(LoadOperation.class);

  // TODO remove
  private Long rowCount;

  @Override
  public boolean run() {
    log.debug("-> LoadOperation:run()");

    boolean retval = false;
    long runStart = System.currentTimeMillis();
    Operation operationDefinition = getOperationDefinition();
    DataStore store = getStore();

    if (operationDefinition != null && store != null) {
      SparkSession spark = store.getSparkSession();
      String format = operationDefinition.getFormat();

      if (StringUtils.isEmpty(format)) {
        String errMsg =
            "The data format was not specified. Cannot continue.";
        log.error(errMsg);
        throw new FastLaneException(errMsg);
      } else {
        log.debug("Processing data format: {}", format);
      }

      ReaderContext readerCtx = buildReader(spark);
      Dataset<Row> df = readerCtx.load();

      store.add(readerCtx.getDataframeName(), df);

      // monitor this in case row counting is taking an unexpected large
      // amount of time
      long countStart = System.currentTimeMillis();
      rowCount = df.count();

      log.debug(
          "Row count call completed in {} seconds. Loaded {} records.",
          ((System.currentTimeMillis() - countStart) / 1000), rowCount);

      retval = true;
    } else {
      if (operationDefinition == null) {
        log.error("No operation associated to this load.");
      }

      if (store == null) {
        log.error("No data store associated to this load.");
      }
    }

    log.debug("Run of {} completed in {} seconds with a status of {}",
        this.getClass().getName(),
        ((System.currentTimeMillis() - runStart) / 1000), retval);

    return retval;
  }

  /**
   * 
   * @return
   * @deprecated This is an expensive operation that should not be done
   *             here.
   */
  public Long getRowCount() {
    return rowCount;
  }

}
