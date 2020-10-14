package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

/**
 * The Spark "sample" method does not take "percent" of the rows. Instead,
 * the fraction value represents a "probability" applied to each row. It
 * uses Bernoulli sampling and the resulting row count will be different on
 * each call.
 */
public class SampleOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(SampleOperation.class);

  Long rowCount;

  @Override
  public boolean run() {
    long runStart = System.currentTimeMillis();
    DataStore store = super.getStore();
    String dataframeName = super.getContainerName();

    if (dataframeName == null) {
      // We are working with the store as a container
      dataframeName = super.getAttributeName();
    }

    Operation operation = super.getOperationDefinition();
    if (operation == null) {
      log.error("There is no linked operation");
      return false;
    }

    Dataset<Row> df = store.get(dataframeName);
    if (df == null) {
      log.error("Unknown dataframe '{}' in store", dataframeName);
      return false;
    }

    double fraction = operation.getFractionAsPercent();
    df = df.sample(false, fraction);
    store.add(dataframeName, df);

    // monitor this in case row counting is taking an unexpected large
    // amount of time
    long countStart = System.currentTimeMillis();

    rowCount = df.count();

    log.debug("Row count call completed in "
        + ((System.currentTimeMillis() - countStart) / 1000) +
        " seconds.  Loaded " + rowCount + " records.");

    log.debug(this.getClass().getName() + " completed in "
        + (System.currentTimeMillis() - runStart) / 1000 +
        " seconds.");

    return true;
  }

  public Long getRowCount() {
    return rowCount;
  }

}
