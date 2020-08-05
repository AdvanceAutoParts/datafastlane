package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

public class ShowOperation extends CoreOperation {
    private static final Logger log = LoggerFactory.getLogger(ShowOperation.class);

    private static final int DEFAULT_NUMBER_OF_ROWS = 20;
    private static final String TRUNCATE = "truncate";
    private static final String NUM_ROWS = "numRows";

    private Operation operationDefinition;

    private boolean getTruncate() {
        String value = operationDefinition.getOptions().getOptionAsString(TRUNCATE);
        if (value == null) {
            return false;
        }
        if (value.charAt(0) == 't' || value.charAt(0) == 'T' || value.charAt(0) == '1') {
            return true;
        }
        return false;
    }

    private int getNumberOfRows() {
        if (!operationDefinition.getOptions().hasOption(NUM_ROWS)) {
            return DEFAULT_NUMBER_OF_ROWS;
        }
        Integer value = operationDefinition.getOptions().getOptionAsInteger(NUM_ROWS);
        if (value == null) {
            return DEFAULT_NUMBER_OF_ROWS;
        }

        return value;
    }

    @Override
    public boolean run() {
        long runStart = System.currentTimeMillis();
        this.operationDefinition = getOperationDefinition();
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
        df.show(getNumberOfRows(), getTruncate());

        log.debug(this.getClass().getName() + " completed in " + (System.currentTimeMillis() - runStart) / 1000.0 +
                  " seconds.");

        return true;
    }

}
