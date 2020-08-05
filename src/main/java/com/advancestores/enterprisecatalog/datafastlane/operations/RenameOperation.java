package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;
import com.advancestores.enterprisecatalog.datafastlane.util.SparkUtils;

public class RenameOperation extends CoreOperation {
    private static final Logger log = LoggerFactory.getLogger(RenameOperation.class);

    @Override
    public boolean run() {
        log.debug("-> {}:run()", this.getClass().getSimpleName());
        boolean status = true;
        long t0 = System.currentTimeMillis();

        // initialization
        
        // Get the operation definition, which is the whole block of the YAML-based recipe file
        Operation operationDefinition = getOperationDefinition();
        if (operationDefinition == null) {
            log.error("No operation associated to this load.");
            return false;
        }

        // Get the store where the dataframe has been previously loaded
        DataStore store = getStore();
        if (store == null) {
            log.error("No data store associated to this load.");
            return false;
        }

        // Get the dataframe from the store
        String dataframeName = super.getContainerName();
        Dataset<Row> df = store.get(dataframeName);
        if (df == null) {
            log.error("Could not find a dataframe called '{}' in the store", dataframeName);
            return false;
        }

        // Get the column to work on and checks that it is in the dataframe
        String columnName = getAttributeName();
        if (!SparkUtils.hasColumn(df, columnName)) {
            log.error("Dataframe {} does not contain column {}, dataframe has columns: {}", dataframeName, columnName,
                      df.columns());
            return false;
        }

        // Get the new name of the column
        String to = operationDefinition.getTo();
        if (to == null) {
            log.error("Target name for renaming column '{}' is null, in dataframe '{}'", columnName, dataframeName);
            return false;
        }

        // Processing
        log.info("About to rename column '{}' to '{}' in dataframe '{}'", columnName, to, dataframeName);
        df = df.withColumnRenamed(columnName, to);

        // Post-processing: save results
        store.add(dataframeName, df);

        // Status update
        log.debug("{} completed in {}s, status: {}", this.getClass().getSimpleName(),
                  (System.currentTimeMillis() - t0) / 1000.0, status);
        return status;
    }
}
