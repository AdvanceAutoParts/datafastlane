package com.advancestores.enterprisecatalog.datafastlane.operations;

import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.min;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

/**
 * Performs a "pivot" group by on a column,
 * 
 * @author jgp
 */
public class PivotGroupByOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(PivotGroupByOperation.class);

  @Override
  public boolean run() {
    log.debug("-> {}:run()", this.getClass().getSimpleName());
    boolean status = true;

    long t0 = System.currentTimeMillis();

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

    List<String> excludedColumnsFromRecipe =
        operationDefinition.getExcludingAsList();
    List<String> excludedColumns = new ArrayList<>();
    List<String> excludedColumnsStartsWith = new ArrayList<>();
    List<String> excludedColumnsEndsWith = new ArrayList<>();
    for (String colName : excludedColumnsFromRecipe) {
      if (colName.startsWith("*")) {
        excludedColumnsEndsWith.add(colName.substring(1));
      } else {
        if (colName.endsWith("*")) {
          excludedColumnsStartsWith
              .add(colName.substring(0, colName.length() - 1));
        } else {
          excludedColumns.add(colName);
        }
      }
    }

    String pivotColumnName = super.getAttributeName();
    log.debug("Pivot column: {}", pivotColumnName);
    log.debug("Excluded column: {}", excludedColumns);
    log.debug("Excluded column ending with: {}", excludedColumnsEndsWith);
    log.debug("Excluded column starting with: {}",
        excludedColumnsStartsWith);
    List<Column> groupByColumns = new ArrayList<>();
    List<Column> firstOnlyColumns = new ArrayList<>();
    for (String colName : df.columns()) {
      boolean includeColumn = true;
      if (colName.compareTo(pivotColumnName) == 0) {
        includeColumn = false;
      }
      if (includeColumn && isExcluded(excludedColumns, colName)) {
        includeColumn = false;
        firstOnlyColumns.add(first(df.col(colName)).as(colName));
      }
      if (includeColumn
          && isExcludedEndsWith(excludedColumnsEndsWith, colName)) {
        includeColumn = false;
        firstOnlyColumns.add(first(df.col(colName)).as(colName));
      }
      if (includeColumn
          && isExcludedStartsWith(excludedColumnsStartsWith, colName)) {
        includeColumn = false;
        firstOnlyColumns.add(first(df.col(colName)).as(colName));
      }
      if (includeColumn) {
        groupByColumns.add(df.col(colName));
      }
    }

    // Ready for processing
    log.debug("Group by on: {}", groupByColumns);
    log.debug("First only: {}", firstOnlyColumns);
    df = df.orderBy(pivotColumnName)
        .groupBy(groupByColumns.toArray(new Column[0]))
        .agg(min(pivotColumnName).as(pivotColumnName),
            firstOnlyColumns.toArray(new Column[0]));
    store.add(dataframeName, df);

    log.debug("{} completed in {}s, status: {}",
        this.getClass().getSimpleName(),
        (System.currentTimeMillis() - t0) / 1000.0, status);
    return status;
  }

  private boolean isExcluded(List<String> list, String value) {
    for (String element : list) {
      if (value.compareTo(element) == 0) {
        return true;
      }
    }
    return false;
  }

  private boolean isExcludedStartsWith(List<String> list, String value) {
    for (String element : list) {
      if (value.startsWith(element)) {
        return true;
      }
    }
    return false;
  }

  private boolean isExcludedEndsWith(List<String> list, String value) {
    for (String element : list) {
      if (value.endsWith(element)) {
        return true;
      }
    }
    return false;
  }

}
