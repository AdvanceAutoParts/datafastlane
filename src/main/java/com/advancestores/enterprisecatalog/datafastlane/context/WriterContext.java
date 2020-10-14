package com.advancestores.enterprisecatalog.datafastlane.context;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;
import com.google.gson.annotations.Expose;

/**
 * Stores information surrounding a DataFrameWriter. The goal was to use the
 * DataFrameWriter options map to store extra things; however, the property
 * is private. Things can be put in, but not read out.
 */
public class WriterContext {
  private static final Logger log =
      LoggerFactory.getLogger(WriterContext.class);

  // TODO Remove this. There is not this concept. This is too dangerous.
  private static final List<String> MERGABLE_FORMATS =
      Arrays.asList("csv", "xml", "text", "json");

  private Dataset<Row> dataframe;
  private DataFrameWriter<Row> writer;
  @Expose()
  private String destination;
  @Expose()
  private String dataframeName;
  @Expose()
  private String connectionName;
  @Expose()
  private boolean hasHeader;
  @Expose()
  private String format;

  public WriterContext() {
  }

  public DataFrameWriter<Row> getWriter() {
    return writer;
  }

  private boolean isMergable() {
    return MERGABLE_FORMATS.contains(format);
  }

  // allows external classes to check whether the format is a mergable type
  public static boolean isMergable(String format) {
    return MERGABLE_FORMATS.contains(format);
  }

  /**
   * If not a jdbc connection, i.e. writes to file, multiple parts are
   * created that may need to be merged. In our case, we will set
   * coalesce(1) to force storing the results in a single file. The file
   * will be renamed to match the destination. The real effect of merge will
   * be to rename the file to match the destination name.
   * 
   * Spark creates a directory named as the destination. In that directory,
   * at least two files are created (more than two is possible - depends on
   * size of dataset). Each partial file name begins with 'part-'. The merge
   * takes those partition files and merges into a single file with the name
   * specified in the destination param
   * 
   * Merge is not allowed for some formats. Basically, standard text-type
   * files such as csv, xml, text, json, etc can be merged into a single
   * name. Formats such as parquet will not be merged into a single file
   * 
   * @return boolean - true if successful merge
   */
  private boolean merge() {
    boolean retval = false;
    long mergeStart = System.currentTimeMillis();

    if (isMergable()) {
      try {
        if (StringUtils.isEmpty(connectionName)) {
          log.debug("Merging files from {} into single file.", destination);

          File file = new File(destination);

          if (file.isDirectory()) {
            File folder = new File(destination + ".folder");

            if (file.renameTo(folder)) {
              file = null; // finished with this
              Iterator<File> files = FileUtils.iterateFiles(folder,
                  new WildcardFileFilter("part-*"),
                  null);

              BufferedWriter dest =
                  new BufferedWriter(new FileWriter(destination));
              long totalRowsProcessed = 0;
              long fileCount = 0;
              boolean headerWritten = false;

              while (files.hasNext()) {
                File partialFile = files.next();
                LineIterator li = FileUtils.lineIterator(partialFile);
                long rowsProcessed = 0;
                String lineSep = System.lineSeparator();

                log.debug("Merging file ({}): {}", ++fileCount,
                    partialFile.getName());

                while (li.hasNext()) {
                  String line = li.next();

                  line =
                      (!line.endsWith(lineSep) ? (line + lineSep) : line);

                  if (hasHeader && !headerWritten) {
                    headerWritten = true;
                    dest.write(line);
                    rowsProcessed++;
                  } else if (!hasHeader || ++rowsProcessed > 1) {
                    dest.write(line);
                  } else {
                    log.trace("***** skipping line: {}", line);
                  }
                }

                li.close();
                totalRowsProcessed += rowsProcessed;
              }

              dest.flush();
              dest.close();
              FileUtils.forceDelete(folder);
              retval = true;

              log.debug(
                  "Destination: {}, Files Merged: {}, Total Rows Written: {}",
                  destination,
                  fileCount, totalRowsProcessed);
            } else {
              log.error(
                  "Unable to rename destination file '{}' to a folder name.",
                  destination);
              FileUtils.forceDelete(file);
            }
          } else {
            log.error(
                "Expected the destination reference '{}' to be a folder name; however, it was not.",
                destination);
            FileUtils.forceDelete(file);
          }
        } else {
          log.debug("Merging not required for {}.", connectionName);
          retval = true;
        }
      } catch (Exception e) {
        String errMsg = "An error occurred while merging:\n" + toString()
            + "\nError: " + e.getMessage();
        log.error(errMsg);
        throw new FastLaneException(errMsg);
      } finally {
        log.debug(
            "File merging completed in {} seconds with a status of {}",
            ((System.currentTimeMillis() - mergeStart) / 1000), retval);
      }
    } else {
      log.warn("Files with format {} are not mergable.", format);
    }

    return retval;
  }

  public boolean save() {
    long saveStart = System.currentTimeMillis();

    try {
      log.info(">>>> Saving using:\n{}", toString());

      // the text format requires that all columns be converted to a single
      // string
      if ("text".contentEquals(format)) {
        log.debug("Using RDD to save as text file.");
        dataframe.coalesce(1).rdd().saveAsTextFile(destination);
      } else {
        writer.save(destination);
      }

      log.info(">>>> Save completed in {} seconds",
          ((System.currentTimeMillis() - saveStart) / 1000));

      return (isMergable() ? merge() : true);
    } catch (Exception e) {
      String errMsg = "An error occurred while saving:\n" + toString()
          + "\nError: " + e.getMessage();
      log.error(errMsg);
      throw new FastLaneException(errMsg);
    }
  }

  public String getFormat() {
    return format;
  }

  public WriterContext setFormat(String format) {
    this.format = format;
    return this;
  }

  public WriterContext setWriter(DataFrameWriter<Row> writer) {
    this.writer = writer;
    return this;
  }

  public String getDestination() {
    return destination;
  }

  public WriterContext setDestination(String destination) {
    this.destination = destination;
    return this;
  }

  public String getDataframeName() {
    return dataframeName;
  }

  public WriterContext setDataframeName(String dataframeName) {
    this.dataframeName = dataframeName;
    return this;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public WriterContext setConnectionName(String connectionName) {
    this.connectionName = connectionName;
    return this;
  }

  public boolean hasHeader() {
    return hasHeader;
  }

  public WriterContext setHasHeader(boolean hasHeader) {
    this.hasHeader = hasHeader;
    return this;
  }

  public Dataset<Row> getDataframe() {
    return dataframe;
  }

  public WriterContext setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }

  public String toString() {
    return Utils.getGsonExpose().toJson(this);
  }
}
