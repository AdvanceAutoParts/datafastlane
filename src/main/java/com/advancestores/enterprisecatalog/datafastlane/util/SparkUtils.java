package com.advancestores.enterprisecatalog.datafastlane.util;

import static org.apache.spark.sql.functions.col;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SparkUtils {
  private static final Logger log =
      LoggerFactory.getLogger(SparkUtils.class);

  /**
   * Returns true is a column is in a dataframe. Case is ignored.
   * 
   * @param df
   *          Dataframe to analyze.
   * @param colName
   *          Column name
   * @return
   */
  public static boolean hasColumn(Dataset<Row> df, String colName) {
    String[] cols = df.columns();
    for (int i = 0; i < cols.length; i++) {
      if (cols[i].compareToIgnoreCase(colName) == 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Debugs a dataframe by displaying its schema, up to 10 rows of its
   * content, and the row count. Counting rows may be an expensive
   * operation.
   * 
   * @param df
   * @return
   */
  public static boolean debug(Dataset<Row> df) {
    if (df == null) {
      log.error("The dataframe you passed is null.");
      return false;
    }
    log.debug("Dataframe '{}' analysis", df);
    df.printSchema();
    df.show(10, false);
    log.debug("The dataframe has {} row(s).", df.count());
    return true;
  }

  public static void deepDive(Dataset<Row> df) {
    deepDive(df, null);
  }

  public static void deepDive(Dataset<Row> df, String... fieldsToAnalyze) {
    String[] columns = df.columns();
    StructType schema = df.schema();
    long rowCount = df.count();
    for (int i = 0; i < columns.length; i++) {
      String column = columns[i];
      if (fieldsToAnalyze == null || isInArray(fieldsToAnalyze, column)) {
        // null = all the fields are analyzed
        DataType type = getType(schema, column);
        log.debug("Column {}, type {}", column, type);
        if (isNumericOrDate(type)) {
          String min =
              df.orderBy(col(column)).first().getAs(column).toString();
          String max = df.orderBy(col(column).desc()).first().getAs(column)
              .toString();
          log.debug("Min value: {} / max value: {}", min, max);
        }
        Dataset<Row> subDf = df.groupBy(col(column)).count();
        long subDfRowCount = subDf.count();
        log.debug("Distinct values for {}/{} ({}%)", subDfRowCount,
            rowCount,
            Math.round(10000d * subDfRowCount / rowCount) / 100f);
        if (subDfRowCount < 11) {
          log.debug("Values: {}", prettyPrint(subDf, column));
        }
      }
    }
  }

  private static boolean isInArray(String[] array, String value) {
    for (String element : array) {
      if (element.compareTo(value) == 0) {
        return true;
      }
    }
    return false;
  }

  public static String prettyPrint(Dataset<Row> df, String column) {
    StringBuilder sb = new StringBuilder();
    List<Row> list = df.select(column).collectAsList();
    boolean first = true;
    for (Row r : list) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(r.get(0));
      first = false;
    }
    return sb.toString();
  }

  public static boolean isNumericOrDate(DataType type) {
    return isNumeric(type) || isDate(type);
  }

  public static boolean isNumeric(DataType type) {
    if (type == DataTypes.DoubleType || type == DataTypes.FloatType
        || type == DataTypes.ShortType ||
        type == DataTypes.IntegerType || type == DataTypes.LongType) {
      return true;
    }
    return false;
  }

  public static boolean isDate(DataType type) {
    if (type == DataTypes.DateType || type == DataTypes.TimestampType) {
      return true;
    }
    return false;
  }

  public static DataType getType(StructType schema, String fieldName) {
    StructField[] fields = schema.fields();
    StructField field = null;
    for (int i = 0; i < fields.length; i++) {
      field = fields[i];
      if (field.name().compareTo(fieldName) == 0) {
        break;
      }
    }
    if (field == null) {
      return null;
    }
    return field.dataType();
  }

}
