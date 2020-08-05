package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;

public class PrintSchemaOperation extends CoreOperation {
    private static final Logger log = LoggerFactory.getLogger(PrintSchemaOperation.class);

    @Override
    public boolean run() {
        long runStart = System.currentTimeMillis();
        DataStore store = getStore();
        String dataframeName = getContainerName();

        if (dataframeName == null) {
            // We are working with the store as a container
            dataframeName = getAttributeName();
        }

        Dataset<Row> df = store.get(dataframeName);
        if (df == null) {
            log.error("Could not find a dataframe called '{}' in the store, cannot print schema", dataframeName);
            return false;
        }

        df.printSchema();

        log.debug(this.getClass().getName() + " completed in " + (System.currentTimeMillis() - runStart) / 1000 +
                  " seconds.");

        return true;
    }

}
