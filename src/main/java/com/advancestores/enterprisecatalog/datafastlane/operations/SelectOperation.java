package com.advancestores.enterprisecatalog.datafastlane.operations;

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
public class SelectOperation extends CoreOperation {
    private static final Logger log = LoggerFactory.getLogger(SelectOperation.class);

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
            log.error("Unknown dataframe '{}' in store", operationDefinition.getFrom());
            return false;
        }

        Dataset<Row> resDf = df.select(operationDefinition.getWithAsColumnArray());

        if (operationDefinition.getOptionValue(K.DISTINCT, "false").toLowerCase().charAt(0) == 't') {
            resDf = resDf.distinct();
        }

        // Save results
        store.add(targetDataframeName, resDf);

        log.debug("{} completed in {}s, status: {}", this.getClass().getSimpleName(),
                  (System.currentTimeMillis() - t0) / 1000.0, status);
        return status;
    }
}
