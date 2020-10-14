package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

public class SaveOperation extends CoreOperation {
  private static final Logger log =
      LoggerFactory.getLogger(SaveOperation.class);

  @Override
  public boolean run() {
    log.debug("-> SaveOperation:run()");

    boolean retval = false;
    long runStart = System.currentTimeMillis();
    Operation operationDefinition = getOperationDefinition();
    DataStore store = getStore();

    if (operationDefinition != null && store != null) {
      String dataframeName = getAttributeName();
      String format = operationDefinition.getFormat();

      if (StringUtils.isEmpty(format)) {
        String errMsg =
            "The data format was not specified. Cannot continue.";
        log.error(errMsg);
        throw new FastLaneException(errMsg);
      } else {
        log.debug("Processing data format: {}", format);
      }

      Dataset<Row> dataframe = store.get(dataframeName);
      if (dataframe == null) {
        String errMsg =
            "Unknown dataframe '" + dataframeName + "' in store.";
        log.error(errMsg);
        throw new FastLaneException(errMsg);
      }

      retval = buildWriter(dataframe).save();
    } else {
      if (operationDefinition == null) {
        log.error("No operation associated to this load.");
      }

      if (store == null) {
        log.error("No data store associated to this load.");
      }
    }

    log.debug("Run of " + this.getClass().getName() + " completed in " +
        (System.currentTimeMillis() - runStart) / 1000
        + " seconds with a status of " + retval);

    return retval;
  }

}
