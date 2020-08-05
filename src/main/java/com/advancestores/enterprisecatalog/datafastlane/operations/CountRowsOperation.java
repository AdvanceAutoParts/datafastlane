package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;

public class CountRowsOperation extends CoreOperation {
    private static final Logger log = LoggerFactory.getLogger(CountRowsOperation.class);

    private Long rowCount = null;

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
            log.error("Unknown dataframe '{}' in store", dataframeName);
            return false;
        }

        rowCount = df.count();

        log.info("{} has {} row(s)", dataframeName, rowCount);
        log.debug(this.getClass().getName() + " completed in " + (System.currentTimeMillis() - runStart) / 1000 +
                  " seconds.");

        return true;
    }

    public Long getRowCount() {
        return rowCount;
    }

}
