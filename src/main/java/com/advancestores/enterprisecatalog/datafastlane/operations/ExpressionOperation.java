package com.advancestores.enterprisecatalog.datafastlane.operations;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

/**
 * Operation to generate a SQL expression. The syntax is:
 * 
 * <pre>
 * - attribute: 'GPS Speed (Meters/second)'
 *   operations:
 *   - operation: expression
 *     using: "'GPS Speed (MPH)' / 2.237"
 * </pre>
 *
 */
public class ExpressionOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(ExpressionOperation.class);

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

    String expression = operationDefinition.getFrom();
    if (expression == null || expression.isEmpty()) {
      log.error("No value for expression");
      return false;
    }
    log.debug("Expression to evaluate: [{}] expanded to: [{}]", expression,
        store.expandProperties(expression));

    // real run!
    df = df.withColumn(newColumnName,
        expr(store.expandProperties(expression)));
    store.add(dataframeName, df);

    log.debug("{} completed in {}s", this.getClass().getName(),
        (System.currentTimeMillis() - runStart) / 1000.0);

    return true;
  }

}
