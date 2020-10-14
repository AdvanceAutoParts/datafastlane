package com.advancestores.enterprisecatalog.datafastlane.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.DataStore;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Operation;

/**
 * Builder for operation.
 *
 */
public abstract class OperationBuilder {
  private static final Logger log =
      LoggerFactory.getLogger(OperationBuilder.class);

  private static String OPERATION_CLASS_PACKAGE =
      "com.advancestores.enterprisecatalog.datafastlane.operations.";

  public static CoreOperation build(DataStore store, String attributeName,
      Operation operation) {
    return build(store, null, attributeName, operation);
  }

  public static CoreOperation build(DataStore store, String containerName,
      String attributeName,
      Operation operation) {
    log.debug("-> build(..., {})", operation.getName());

    String className = getClassNameForOperation(operation.getName());

    CoreOperation op = null;
    try {
      op = (CoreOperation) Class.forName(className).newInstance();
    } catch (IllegalAccessException e) {
      log.error("Class {} could not be accessed: {}", className,
          e.getMessage());
      ;
      return null;
    } catch (InstantiationException e) {
      log.error("Class {} could not be instanciated: {}", className,
          e.getMessage());
      ;
      return null;
    } catch (ClassNotFoundException e) {
      log.error("Class {} could not be found: {}", className,
          e.getMessage());
      ;
      return null;
    }

    op.setStore(store);
    op.setContainerName(containerName);
    op.setAttributeName(attributeName);
    op.setOperationDefinition(operation);

    return op;
  }

  /**
   * Gets a class name from a name. It can be a full qualified class name.
   * 
   * @param name
   * @return
   */
  private static String getClassNameForOperation(String operationName) {
    String className = null;
    if (operationName.contains(".") == true) {
      // assuming fully qualified class name
      className = operationName;
    } else {
      className = OPERATION_CLASS_PACKAGE
          + operationName.substring(0, 1).toUpperCase() +
          operationName.substring(1) + "Operation";
    }
    log.debug("<- getClassNameForOperation(): {}", className);
    return className;
  }

}
