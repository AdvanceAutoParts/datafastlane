package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;

/**
 * Drops a column from a table.
 */
public class DropOperation extends CoreOperation {
    private static final Logger log = LoggerFactory.getLogger(DropOperation.class);

    @Override
    public boolean run() {
        log.debug("-> run()");
        long runStart = System.currentTimeMillis();
        DataStore store = super.getStore();
        if (super.isStore()) {
            // The operation is on the store, will drop the container
            String elementToDrop = super.getAttributeName();
            log.info("About to drop {} from store", elementToDrop);
            store.drop(elementToDrop);
        }
        else {
        	// Drop one column at a time from database
            String elementToDrop = super.getAttributeName();
            String dataframeName = super.getContainerName();
            Dataset<Row> df = store.get(dataframeName);
            if (df == null) {
                log.error("Trying to get '{}' from the store, but it does not exist", dataframeName);
                return false;
            }
            df = df.drop(elementToDrop);
            store.add(dataframeName, df);
        }

        log.debug("{} completed in {}s", this.getClass().getName(), (System.currentTimeMillis() - runStart) / 1000.0);

        return true;
    }

}
