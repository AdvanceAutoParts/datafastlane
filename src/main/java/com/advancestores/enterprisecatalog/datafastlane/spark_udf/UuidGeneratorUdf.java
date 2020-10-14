package com.advancestores.enterprisecatalog.datafastlane.spark_udf;

import java.util.List;
import java.util.UUID;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.Seq;

/**
 * A Spark UDF that will generate a UUID from any number of columns from a
 * dataframe.
 */
public class UuidGeneratorUdf implements UDF1<Seq<Object>, String> {
  private static final Logger log =
      LoggerFactory.getLogger(UuidGeneratorUdf.class);

  /**
   * Required for serialization.
   */
  private static final long serialVersionUID = 5282765970791291641L;

  @Override
  public String call(Seq<Object> t1) throws Exception {
    List<Object> listA =
        scala.collection.JavaConverters.seqAsJavaListConverter(t1).asJava();
    StringBuilder seed = new StringBuilder();
    for (Object val : listA) {
      log.trace("Getting {}", val);
      seed.append(val);
      seed.append("*-*");
    }
    return UUID.nameUUIDFromBytes(seed.toString().getBytes()).toString();
  }

}
