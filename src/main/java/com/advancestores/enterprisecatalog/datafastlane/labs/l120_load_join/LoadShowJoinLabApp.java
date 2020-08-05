package com.advancestores.enterprisecatalog.datafastlane.labs.l120_load_join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.DataTransformer;

class LoadShowJoinLabApp {
    private static final Logger log = LoggerFactory.getLogger(LoadShowJoinLabApp.class);

    public static void main(String[] args) {
        LoadShowJoinLabApp app = new LoadShowJoinLabApp();
        app.start();
    }

    private boolean start() {
        boolean status = true;

        // Timer
        long t0 = System.currentTimeMillis();

        // Run recipe
        DataStore store = new DataStore("src/main/resources/recipe-load-show-transform-v1.yaml");
        status = DataTransformer.transform(store);

        log.info("Execution took {} s. Status: {}.", (System.currentTimeMillis() - t0) / 1000.0, status);
        return status;
    }
}
