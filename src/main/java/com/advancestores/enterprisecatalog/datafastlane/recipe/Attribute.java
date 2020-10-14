package com.advancestores.enterprisecatalog.datafastlane.recipe;

import java.util.ArrayList;
import java.util.List;

import com.advancestores.enterprisecatalog.datafastlane.util.Utils;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A transformation starts to work on a name and has operations, which can
 * have argument.
 */
public class Attribute {

  /**
   * An name can be a column or a dataframe, it composes the current
   * dataframe or store.
   */
  @JsonProperty("attribute")
  private String name;
  private List<Operation> operations;

  public Operation getOperation(String name) {
    return (operations != null ? operations.stream()
        .filter(o -> o.getName().equals(name)).findFirst().orElse(null)
        : null);
  }

  public Operation getFirstOperation() {
    return (operations != null ? operations.get(0) : null);
  }

  public String debug() {
    StringBuilder sb = new StringBuilder();
    sb.append("****. at .... +- ");
    sb.append(this.name);
    sb.append('\n');
    if (operations != null) {
      for (Operation action : this.operations) {
        sb.append(action.debug());
      }
    }

    return sb.toString();
  }

  public String getName() {
    return this.name;
  }

  public List<Operation> getOperations() {
    return operations;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setSteps(List<String> stepsAsString) {
    this.operations = new ArrayList<>();
    for (String actionName : stepsAsString) {
      Operation action = new Operation();
      String actionNameArray[] = actionName.split(" ");
      action.setName(actionNameArray[0]);
      for (int i = 1; i < actionNameArray.length; i++) {
        action.addArgument(actionNameArray[i]);
      }
      this.operations.add(action);
    }
  }

  public String toString() {
    return Utils.getGson().toJson(this);
  }
}
