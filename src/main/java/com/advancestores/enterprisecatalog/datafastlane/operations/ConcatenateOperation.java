package com.advancestores.enterprisecatalog.datafastlane.operations;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

public class ConcatenateOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(ConcatenateOperation.class);

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

    String columnName = getAttributeName();
    if (columnName == null) {
      log.error("No column name found");
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
    List<String> columnNames = operationDefinition.getWithAsList();
    log.debug("List of columns: {}", columnNames);
    List<Column> columns = new ArrayList<>();
    for (String colName : columnNames) {
      Column col;
      if (colName.startsWith("'")) {
        // literal column
        col = lit(colName.substring(1, colName.length() - 1));
      } else {
        // named column
        col = col(colName);
      }
      log.debug("Column {} added as {}", colName, col);
      columns.add(col);
    }
    df = df.withColumn(columnName, concat(columns.toArray(new Column[0])));

    // Save result
    store.add(dataframeName, df);

    log.debug("{} completed in {} seconds.",
        this.getClass().getSimpleName(),
        (System.currentTimeMillis() - runStart) / 1000.0);

    return true;
  }

}
