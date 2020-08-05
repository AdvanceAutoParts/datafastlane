package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

public class RepartitionOperation extends CoreOperation {
    private static final Logger log = LoggerFactory.getLogger(RepartitionOperation.class);

    @Override
    public boolean run() {
        long runStart = System.currentTimeMillis();
        DataStore store = super.getStore();
        String dataframeName = super.getContainerName();

        if (dataframeName == null) {
            // We are working with the store as a container
            dataframeName = super.getAttributeName();
        }

        Operation operation = super.getOperationDefinition();
        if (operation == null) {
            log.error("There is no linked operation");
            return false;
        }

        Dataset<Row> df = store.get(dataframeName);
        if (df == null) {
            log.error("Unknown dataframe '{}' in store", dataframeName);
            return false;
        }

        int partitionCount = operation.getToAsInteger();
        df = df.repartition(partitionCount);
        store.add(dataframeName, df);

        log.debug(this.getClass().getName() + " completed in " + (System.currentTimeMillis() - runStart) / 1000.0 +
                  " seconds.");

        return true;
    }

}
