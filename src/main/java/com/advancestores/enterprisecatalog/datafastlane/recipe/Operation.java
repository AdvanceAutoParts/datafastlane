package com.advancestores.enterprisecatalog.datafastlane.recipe;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.sql.Column;

import com.advancestores.enterprisecatalog.datafastlane.util.Utils;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Operation {
  private String appName;
  private List<String> arguments;
  private String excluding;
  private String format;
  private String fraction;
  private String from;
  transient StringSubstitutor interpolator;
  private String master;
  private String mode;
  @JsonProperty("operation")
  private String name;
  private String nesting;
  private String on;
  private Options options;
  private String path;
  private String schemaName;
  private String to;
  private String type;
  private String with;

  public Operation() {
    this.arguments = new ArrayList<>();
    this.options = new Options();
    this.interpolator = Utils.getInterpolatorInstance();
  }

  public void addArgument(String arg) {
    this.arguments.add(arg);
  }

  private Column[] buildColumnArray(List<String> list) {
    List<String> columnsName = list;
    Column[] columns = new Column[columnsName.size()];
    int i = 0;
    for (String columnName : columnsName) {
      columns[i] = col(columnName);
      i++;
    }
    return columns;
  }

  private List<String> buildList(String arg) {
    String[] vals = arg.split(",");
    List<String> valsArray = new ArrayList<>();
    for (String val : vals) {
      valsArray.add(val.trim());
    }
    return valsArray;
  }

  public String debug() {
    StringBuilder sb = new StringBuilder();
    sb.append("***.. ops ... |  +- ");
    sb.append(name);
    int i = 0;
    for (String arg : arguments) {
      sb.append('|');
      sb.append(i);
      sb.append(':');
      sb.append(arg);
      i++;
    }
    sb.append('\n');
    if (options != null) {
      for (Option option : options) {
        sb.append(option.debug());
      }
    }
    return sb.toString();
  }

  public String getAppName() {
    return appName;
  }

  public String getArgumentAt(int i) {
    if (i >= this.arguments.size()) {
      return null;
    }
    return this.arguments.get(i);
  }

  public List<String> getArguments() {
    return arguments;
  }

  public String getExcluding() {
    return this.excluding;
  }

  public List<String> getExcludingAsList() {
    return buildList(this.excluding);
  }

  public String getFormat() {
    return format;
  }

  public String getFraction() {
    return fraction;
  }

  public double getFractionAsPercent() {
    Double f;
    if (fraction.contains("%") == true) {
      String tmp = fraction.split("\\%")[0];
      f = Double.valueOf(tmp);
      f = f / 100.0;
    } else {
      f = Double.valueOf(fraction);
    }
    return f;
  }

  public String getFrom() {
    return from;
  }

  public String getMaster() {
    return master;
  }

  public String getMode() {
    return mode;
  }

  public String getName() {
    return name;
  }

  public String getNesting() {
    return nesting;
  }

  public Column[] getNestingAsColumnArray() {
    return buildColumnArray(getNestingAsList());
  }

  public List<String> getNestingAsList() {
    return buildList(this.nesting);
  }

  public String[] getNestingAsArray() {
    return getNestingAsList().toArray(new String[0]);
  }

  public String getOn() {
    return on;
  }

  public Column[] getOnAsColumnArray() {
    return buildColumnArray(getOnAsList());
  }

  public List<String> getOnAsList() {
    return buildList(this.on);
  }

  public Option getOption(String key) {
    return (options != null ? options.getOption(key) : null);
  }

  public Options getOptions() {
    return options;
  }

  public String getOptionValue(String key) {
    Option o = (options != null ? options.getOption(key) : null);
    return (o != null ? o.getValue() : null);
  }

  public String getOptionValue(String key, String defaultValue) {
    Option o = (options != null ? options.getOption(key) : null);
    return (o != null ? o.getValue() : defaultValue);
  }

  public String getPath() {
    return path;
  }

  public String getSchemaName() {
    return this.schemaName;
  }

  public String getTo() {
    return to;
  }

  public int getToAsInteger() {
    return Integer.valueOf(this.to);
  }

  public String getType() {
    return type;
  }

  public String getWith() {
    return with;
  }

  public Column[] getWithAsColumnArray() {
    return buildColumnArray(getWithAsList());
  }

  public List<String> getWithAsList() {
    return buildList(this.with);
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public void setExcluding(String excluding) {
    this.excluding = excluding;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public void setFraction(String ratio) {
    this.fraction = ratio;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public void setMaster(String master) {
    this.master = master;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public void setName(String name) {
    String[] args = name.split(" ");
    this.name = args[0];
    for (int i = 1; i < args.length; i++) {
      this.addArgument(args[i]);
    }
  }

  public void setNesting(String nesting) {
    this.nesting = nesting;
  }

  public void setOn(String on) {
    this.on = on;
  }

  public void setOptions(Options options) {
    this.options = options;
  }

  // important to call setter so variable interpolation can occur
  public void setPath(String path) {
    this.path =
        (StringUtils.isEmpty(path) ? path : interpolator.replace(path));
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setTo(String to) {
    this.to = to;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setWith(String with) {
    this.with = with;
  }

  public String toString() {
    return Utils.getGson().toJson(this);
  }
}
