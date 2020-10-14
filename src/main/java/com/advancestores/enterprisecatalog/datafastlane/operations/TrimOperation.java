package com.advancestores.enterprisecatalog.datafastlane.operations;

import static org.apache.spark.sql.functions.ltrim;
import static org.apache.spark.sql.functions.rtrim;
import static org.apache.spark.sql.functions.trim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;
import com.esotericsoftware.minlog.Log;

/**
 * Operation to trim whitespace preceding or succeeding a string
 *
 * <pre>
 * trimTest:
 * - attribute: subjectTextToTrim
 *   operations:
 *   - operation: trim
 #     options:
 #       left: true
 #       right: true
 * </pre>
 *
 */
public class TrimOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(TrimOperation.class);

  private enum TrimType {
    ALL,
    LEFT,
    RIGHT
  }

  @Override
  public boolean run() {
    long runStart = System.currentTimeMillis();

    log.debug("-> {}:run()", this.getClass().getSimpleName());

    // initialization
    Operation operationDefinition = getOperationDefinition();
    if (operationDefinition == null) {
      log.error("No operation associated to this load.");
      return false;
    }

    DataStore store = getStore();
    if (store == null) {
      log.error("No data store associated to this load.");
      return false;
    }

    String dataframeName = super.getContainerName();
    Dataset<Row> df = store.get(dataframeName);
    if (df == null) {
      log.error("Could not find a dataframe called '{}' in the store",
          dataframeName);
      return false;
    }

    // Processing
    String operationColumnName = super.getAttributeName();

    String explicitTrimLeft =
        operationDefinition.getOptionValue("left", "invalid").toLowerCase();
    String explicitTrimRight = operationDefinition
        .getOptionValue("right", "invalid").toLowerCase();

    TrimType trimType = TrimType.ALL;

    if (explicitTrimLeft.substring(0, 1).charAt(0) == 't'
        && explicitTrimRight.substring(0, 1).charAt(0) != 't') {
      trimType = TrimType.LEFT;
    } else if (explicitTrimLeft.substring(0, 1).charAt(0) != 't'
        && explicitTrimRight.substring(0, 1).charAt(0) == 't') {
      trimType = TrimType.RIGHT;
    } else if (explicitTrimLeft.substring(0, 1).charAt(0) == 'f'
        && explicitTrimRight.substring(0, 1).charAt(0) == 'f') {
      Log.error(
          "Cannot specify false for both left and right options for Trim operation",
          dataframeName);
      return false;
    }

    log.debug("Trim operation applied to {}, Trim Type: {}",
        operationColumnName, trimType.toString());
    switch (trimType) {
      case ALL:
        df = df.withColumn(operationColumnName,
            trim(df.col(operationColumnName)));
        break;
      case LEFT:
        df = df.withColumn(operationColumnName,
            ltrim(df.col(operationColumnName)));
        break;
      case RIGHT:
        df = df.withColumn(operationColumnName,
            rtrim(df.col(operationColumnName)));
        break;
    }

    // Save result
    store.add(dataframeName, df);

    log.debug("{} completed in {} seconds.",
        this.getClass().getSimpleName(),
        (System.currentTimeMillis() - runStart) / 1000.0);

    return true;
  }

}
