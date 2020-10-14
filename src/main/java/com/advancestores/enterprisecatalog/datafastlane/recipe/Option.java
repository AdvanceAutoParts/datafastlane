package com.advancestores.enterprisecatalog.datafastlane.recipe;

public class Option {

  private String value;
  private String key;

  public Option(String key, String value) {
    this.setKey(key);
    this.setValue(value);
  }

  public String debug() {
    return "**... opt ... |  |  +- " + key + ":" + value + '\n';
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setValue(String value) {
    this.value = value;
  }

}
