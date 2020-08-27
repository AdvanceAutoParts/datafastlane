package com.advancestores.enterprisecatalog.datafastlane.operations;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.callUDF;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

/**
 * Operation to drop a list of columns. The syntax is:
 * 
 * <pre>
 * - operation: dropColumns
 *   with: ' Roll (deg)| Pitch (deg)| Magnetometer X (µT)| Magnetometer Y (µT)| Magnetometer Z (µT)'
 *   options:
 *     separator: '|'
 * </pre>
 *
 */
public class DropColumnsOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(DropColumnsOperation.class);

  private static final String SEPARATOR = "separator";
  private static final String DEFAULT_SEPARATOR = ",";

  private Operation operationDefinition;

  @Override
  public boolean run() {
    long runStart = System.currentTimeMillis();

    log.debug("-> run()");

    // initialization & validation
    operationDefinition = getOperationDefinition();
    if (operationDefinition == null) {
      log.error("No definition associated to this operation.");
      return false;
    }

    String dataframeName = super.getContainerName();
    if (dataframeName != null) {
      // We are working with the store as a container
      log.error("This operation only works at the store level");
      return false;
    }
    dataframeName = getAttributeName();

    DataStore store = getStore();
    if (store == null) {
      log.error("No data store associated to this load.");
      return false;
    }

    Dataset<Row> df = store.get(dataframeName);
    if (df == null) {
      log.error("Unknown dataframe '{}' in store", dataframeName);
      return false;
    }

    String columnAsOneString = operationDefinition.getWith();
    if (columnAsOneString == null || columnAsOneString.isEmpty()) {
      log.error("No columns to drop");
      return false;
    }

    // real run!
    String[] cols = columnAsOneString.split(getOptionSeparator());
    List<String> colArray = Arrays.asList(cols);
    log.info(
        "Will remove {} columns [{}] from raw columns [{}] using separator [{}]",
        colArray.size(), colArray, columnAsOneString, getOptionSeparator());
    for (int i = 0; i < cols.length; i++) {
      df = df.drop(cols[i]);
    }
    store.add(dataframeName, df);

    log.debug("{} completed in {}s", this.getClass().getName(),
        (System.currentTimeMillis() - runStart) / 1000.0);

    return true;
  }

  private String getOptionSeparator() {
    if (operationDefinition.getOptions() == null) {
      log.error("No options specified");
      return DEFAULT_SEPARATOR;
    }
    if (!operationDefinition.getOptions().hasOption(SEPARATOR)) {
      return DEFAULT_SEPARATOR;
    }

    return operationDefinition.getOptions().getOptionAsString(SEPARATOR);
  }

}
