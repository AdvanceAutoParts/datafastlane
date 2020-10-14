package com.advancestores.enterprisecatalog.datafastlane.recipe;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.util.Utils;
import com.fasterxml.jackson.annotation.JsonAnySetter;

public class Recipe {
  private static final Logger log = LoggerFactory.getLogger(Recipe.class);

  private List<Container> containers;

  public Recipe() {
    this.containers = new ArrayList<>();
  }

  @JsonAnySetter
  public void addDynamicProperty(String name, List<Attribute> steps) {
    log.debug("-> addDynamicProperty({}, {})", name, steps);

    Container recipe = new Container();
    recipe.setContainerName(name);
    recipe.setAttributes(steps);

    if (getContainer(name) != null) {
      log.warn(
          "Container with name '{}' already exists in recipe. Adding container with duplicate name.",
          name);
    }

    containers.add(recipe);
  }

  public String debug() {
    StringBuilder sb = new StringBuilder();
    for (Container c : this.containers) {
      sb.append("***** ct .... ");
      sb.append(c.getContainerName());
      sb.append(":\n");
      for (Attribute t : c.getAttributes()) {
        sb.append(t.debug());
      }
    }
    return sb.toString();
  }

  public List<Container> getContainers() {
    return this.containers;
  }

  /**
   * This will find the first container with the name provided. If multiple
   * values of the same name exist, use getContainers to get all containers
   * with the same name.
   * 
   * @param String
   *          name - name of container for which to search
   * 
   * @return Container - the first container that matches the name provided
   */
  public Container getContainer(String name) {
    return containers.stream()
        .filter(c -> c.getContainerName().equals(name)).findFirst()
        .orElse(null);
  }

  /**
   * Returns all containers that match the given name.
   * 
   * @param String
   *          name - name of container for which to search
   * 
   * @return Container[] - an array of all containers that match the name
   */
  public Container[] getContainers(String name) {
    return containers.stream()
        .filter(c -> c.getContainerName().equals(name))
        .toArray(Container[]::new);
  }

  public int getCount() {
    return this.containers.size();
  }

  public String toString() {
    return Utils.getGson().toJson(this);
  }
}
