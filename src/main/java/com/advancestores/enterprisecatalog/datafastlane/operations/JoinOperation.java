package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;
import com.advancestores.enterprisecatalog.datafastlane.util.SparkUtils;

public class JoinOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(JoinOperation.class);

  @Override
  public boolean run() {
    log.debug("-> {}:run()", this.getClass().getSimpleName());
    boolean status = true;
    long t0 = System.currentTimeMillis();

    // init
    DataStore store = super.getStore();
    Operation operation = super.getOperationDefinition();

    // get the left stuff of the join
    String tableName = super.getContainerName();
    String columnName = super.getAttributeName();

    // get the right stuff from the operationDefinition
    String rightTableName = operation.getWith();
    String rightColumnName = operation.getOn();
    String joinType = operation.getType();

    if (rightTableName == null) {
      // one line syntax
      rightTableName = operation.getArgumentAt(0);
      rightColumnName = operation.getArgumentAt(1);
      joinType = operation.getArgumentAt(2);
    }
    if (rightTableName == null) {
      log.error(
          "You need to specify the right part of the join and the join column, skipping this step.");
      return false;
    }
    if (rightColumnName == null) {
      log.error("You need to specify the join column, skipping this step.");
      return false;
    }
    if (joinType == null) {
      joinType = "left";
    }
    log.debug("About to join '{}'.'{}' with '{}'.'{}' using a '{}' join",
        tableName, columnName, rightTableName,
        rightColumnName, joinType);
    Dataset<Row> rightDf = store.get(rightTableName);
    if (rightDf == null) {
      log.error(
          "Could not find a dataframe called '{}' in the store, ignoring the join operation",
          rightTableName);
      return false;
    }
    Dataset<Row> df = store.get(tableName);
    if (df == null) {
      log.error(
          "Could not find a dataframe called '{}' in the store, ignoring the join operation",
          tableName);
      return false;
    }

    // Processing
    try {
      df = df.join(rightDf,
          df.col(columnName).equalTo(rightDf.col(rightColumnName)),
          joinType);
    } catch (Exception e) {
      log.error("An error occured while performing a join operation: {}",
          e.getMessage(), e);
      log.info("Execution plan written to stdout");
      df.explain();
      log.info("Left dataframe info:");
      SparkUtils.debug(df);
      log.info("Right dataframe info:");
      SparkUtils.debug(rightDf);
      status = false;
    }

    if (status) {
      // Drops the join column on the right dataframe
      df = df.drop(rightDf.col(rightColumnName));
      // Saves the result
      store.add(tableName, df);
    }

    log.debug("{} completed in {}s, status: {}",
        this.getClass().getSimpleName(),
        (System.currentTimeMillis() - t0) / 1000.0, status);
    return status;
  }

}
