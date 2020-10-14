package com.advancestores.enterprisecatalog.datafastlane.operations;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.struct;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.K;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

/**
 * Performs a "pivot" group by on a column,
 * 
 * @author jgp
 */
public class NestedGroupByOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(NestedGroupByOperation.class);

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

    String targetDataframeName = super.getAttributeName();

    DataStore store = getStore();
    if (store == null) {
      log.error("No data store associated to this load.");
      return false;
    }

    Dataset<Row> df = store.get(operationDefinition.getFrom());
    if (df == null) {
      log.error("Unknown dataframe '{}' in store",
          operationDefinition.getFrom());
      return false;
    }

    List<Column> selectColumns = new ArrayList<>();
    selectColumns
        .addAll(Arrays.asList(operationDefinition.getOnAsColumnArray()));
    selectColumns.addAll(
        Arrays.asList(operationDefinition.getNestingAsColumnArray()));
    String nestingColumn = operationDefinition.getNestingAsArray()[0];
    String structName = operationDefinition.getOptionValue(K.STRUCT_NAME,
        nestingColumn + K.NESTING_COLUMN_SUFFIX);
    String listName = operationDefinition.getOptionValue(K.LIST_NAME,
        nestingColumn + K.LIST_NAME_SUFFIX);
    Dataset<Row> resDf = df
        .select(selectColumns.toArray(new Column[0])).distinct()
        .withColumn(structName,
            struct(operationDefinition.getNestingAsColumnArray()))
        .drop(operationDefinition.getNestingAsArray())
        .groupBy(operationDefinition.getOnAsColumnArray())
        .agg(collect_list(structName).as(listName));

    // Save results
    store.add(targetDataframeName, resDf);

    // Status update
    log.debug("{} completed in {}s, status: {}",
        this.getClass().getSimpleName(),
        (System.currentTimeMillis() - t0) / 1000.0, status);
    return status;
  }
}
