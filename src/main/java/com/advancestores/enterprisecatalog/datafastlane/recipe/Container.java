package com.advancestores.enterprisecatalog.datafastlane.recipe;

import java.util.List;

import com.advancestores.enterprisecatalog.datafastlane.util.Utils;

/**
 * Container of attributes.
 */
public class Container {

  private String containerName;
  private List<Attribute> attributes;

  public Attribute getAttribute(String name) {
    return (attributes != null ? attributes.stream()
        .filter(a -> a.getName().equals(name)).findFirst().orElse(null)
        : null);
  }

  public List<Attribute> getAttributes() {
    return this.attributes;
  }

  public String getContainerName() {
    return this.containerName;
  }

  public void setAttributes(List<Attribute> attributes) {
    this.attributes = attributes;
  }

  public void setContainerName(String name) {
    this.containerName = name;

  }

  public String toString() {
    return Utils.getGson().toJson(this);
  }
}
