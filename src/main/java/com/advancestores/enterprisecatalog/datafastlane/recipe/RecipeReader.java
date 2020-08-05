package com.advancestores.enterprisecatalog.datafastlane.recipe;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class RecipeReader {
    private static final Logger log = LoggerFactory.getLogger(RecipeReader.class);

    public static Recipe read(InputStream in) {
        Recipe rules = null;

        if (in != null) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

            try {
                rules = mapper.readValue(in, Recipe.class);
            }
            catch (IOException e) {
                log.error("Error while reading recipe's input stream: {}", e.getMessage(), e);
            }
        }
        else {
            String errMsg = "The input stream is null.  Nothing to process.";
            log.warn(errMsg);
            throw new FastLaneException(errMsg);
        }

        return rules;
    }

    public static Recipe read(String filename) {
        Recipe rules = null;

        if (StringUtils.isNotEmpty(filename)) {
            File recipeFile = new File(filename);

            if (recipeFile.exists()) {
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

                try {
                    rules = mapper.readValue(recipeFile, Recipe.class);
                }
                catch (IOException e) {
                    String errMsg = "Error while reading recipe file " + filename + " : " + e.getMessage();
                    log.error(errMsg, e);
                    throw new FastLaneException(errMsg);
                }
            }
            else {
                String errMsg = "The recipe file with name '" + filename + "' does not exist.  Cannot continue.";
                log.error(errMsg);
                throw new FastLaneException(errMsg);
            }
        }
        else {
            String errMsg = "The recipe file name is null.  Cannot continue.";
            log.error(errMsg);
            throw new FastLaneException(errMsg);
        }

        return rules;
    }
}
