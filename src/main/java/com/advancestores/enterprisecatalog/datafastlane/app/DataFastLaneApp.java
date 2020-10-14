package com.advancestores.enterprisecatalog.datafastlane.app;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.DataTransformer;
import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.K;
import com.advancestores.enterprisecatalog.datafastlane.context.AwsCredentialsContext;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;

/**
 * Configure the application to process a DFL recipe file. The details of
 * what will happen are specified in the recipe. This includes connections
 * (input/output), schemas, and operations.
 * 
 * The input arguments are: o recipeFilename - name of recipe file to
 * process o -dryrun - optional flag to signify the recipe is to be
 * processed, but the transformation is to be skipped
 */
public class DataFastLaneApp {
  private static final Logger log =
      LoggerFactory.getLogger(DataFastLaneApp.class);

  public static void main(String[] args) {
    StringSubstitutor interpolator = Utils.getInterpolatorInstance();
    boolean isDryRun = false;
    String cmdLineRecipe = null;

    // process command line arguments - the first argument that is not
    // '-dryrun' will
    // be considered to be the recipe. check the balance of the args for
    // '-dryrun'
    // there shouldn't be a lot of args, so don't worry about breaking early
    for (String arg : args) {
      if (arg.equalsIgnoreCase("--dryrun")) {
        isDryRun = true;
      } else if (cmdLineRecipe == null) {
        cmdLineRecipe = arg;
      }
    }

    log.debug("Command Line Recipe File: {}", cmdLineRecipe);
    log.debug("is dryRun: {}", isDryRun);

    if (StringUtils.isEmpty(cmdLineRecipe)) {
      String errMsg =
          "ERROR: The recipe filename is a required parameter to the application.\n"
              +
              "Usage: dfl.sh <recipe-filename> [--dryrun]";
      System.out.println(errMsg);
    } else {
      log.debug("Command Line Recipe File after Interpolation: {}",
          cmdLineRecipe);

      String dest = Utils.getEnv(K.KEY_DESTINATION,
          System.getProperty(K.KEY_DESTINATION));
      AwsCredentialsContext ctx = new AwsCredentialsContext(
          Utils.getEnv(K.KEY_AWS_ACCESS_KEY_ID,
              System.getProperty(K.KEY_AWS_ACCESS_KEY_ID)),
          Utils.getEnv(K.KEY_AWS_SECRET_ACCESS_KEY,
              System.getProperty(K.KEY_AWS_SECRET_ACCESS_KEY)),
          Utils.getEnv(K.KEY_AWS_SESSION_TOKEN,
              System.getProperty(K.KEY_AWS_SESSION_TOKEN)),
          Utils.getEnv(K.KEY_AWS_REGION,
              System.getProperty(K.KEY_AWS_REGION)),
          Utils.getEnv(K.KEY_AWS_ROLE_ARN,
              System.getProperty(K.KEY_AWS_ROLE_ARN)));
      String recipe = interpolator.replace(cmdLineRecipe); // check for
                                                           // variable
                                                           // interpolation

      log.debug("Recipe File after Interpolation: {}", recipe);

      if (recipe.startsWith(K.S3_PREFIX)) {
        // You can output the credentials by adding the ctx variable to the
        // debug, but it will expose the credentials
        log.debug("Destination: {}\nCredentials: {}\n", dest, "N/A");

        // if the environment variables are not set, no need to continue -
        // check required ones
        if (StringUtils.isEmpty(ctx.getAccessKeyId())
            || StringUtils.isEmpty(ctx.getSecretAccessKey()) ||
            StringUtils.isEmpty(ctx.getRegion())) {
          String errMsg =
              "The AWS credentials are required before a file can be copied from S3. Check the enviornment variables.";
          log.error(errMsg);
          throw new FastLaneException(errMsg);
        }

        // if the input file name is an S3 bucket, it will be copied locally
        recipe = Utils.downloadS3File(recipe, dest, ctx);
      }

      // TODO add support for a silent / quiet mode
      System.out.printf(
          "DataFastLaneApp is starting and will process recipe: %s\n",
          recipe);

      log.debug("DataFastLaneApp is starting and will process recipe: {}",
          recipe);

      try {
        long transformStart = System.currentTimeMillis();
        DataStore store = new DataStore(recipe);
        boolean status = false;

        if (!isDryRun) {
          status = DataTransformer.transform(store);
        } else {
          log.info(
              "***** This is a dryrun. Transformation not executed. *****");
        }

        // TODO add support for a silent / quiet mode
        System.out.printf(
            "DFL completed in %.1f seconds with a status of %b.",
            ((System.currentTimeMillis() - transformStart) / 1000), status);

        log.debug(
            "DFL completed in {} seconds with a status of {}.",
            ((System.currentTimeMillis() - transformStart) / 1000), status);

        log.debug("Dataframe Count: {}", store.getDataframeCount());
        log.trace("\nRecipe:\n{}\n", store.getRecipe());
      } catch (Exception e) {
        String errMsg = "An error occured while processing recipe " + recipe
            + ".\nReason: " + e.getMessage();
        log.error(errMsg, e);
        throw new FastLaneException(errMsg);
      }
    }
  }
}
