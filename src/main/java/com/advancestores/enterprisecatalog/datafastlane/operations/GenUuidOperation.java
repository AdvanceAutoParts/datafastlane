package com.advancestores.enterprisecatalog.datafastlane.operations;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

/**
 * Operation to generate a UUID from a list of column. The syntax is:
 * 
 * <pre>
 * books:
 * - attribute: uuid
 *   operations:
 *   - operation: genUuid
 *     with: name, title, id
 * </pre>
 *
 */
public class GenUuidOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(GenUuidOperation.class);

  @Override
  public boolean run() {
    long runStart = System.currentTimeMillis();

    log.debug("-> run()");

    // initialization & validation
    Operation operationDefinition = getOperationDefinition();
    if (operationDefinition == null) {
      log.error("No definition associated to this operation.");
      return false;
    }

    String dataframeName = super.getContainerName();

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

    String newColumnName = super.getAttributeName();

    String columns = operationDefinition.getWith();
    if (columns == null || columns.isEmpty()) {
      log.error("No columns to work with as seed for generating a UUID");
      return false;
    }

    // real run!
    String[] colsAsString = columns.split(",");
    Column[] cols = new Column[colsAsString.length];
    for (int i = 0; i < colsAsString.length; i++) {
      cols[i] = df.col(colsAsString[i].trim());
    }

    Column col = array(cols);

    df = df.withColumn(newColumnName, callUDF("genUuid", col));
    store.add(dataframeName, df);

    log.debug("{} completed in {}s", this.getClass().getName(),
        (System.currentTimeMillis() - runStart) / 1000.0);

    return true;
  }

}
