package com.advancestores.enterprisecatalog.datafastlane.context;

import com.advancestores.enterprisecatalog.datafastlane.K;

/**
 * Manages properties used by the Spark session object. The properties/options are read from
 * the recipe file under the $spark container section
 */
public class SparkContext extends BaseContext {
    private String appName;
    private String master;

    public SparkContext() {
        super();
    }

    public SparkContext(String appName, String master) {
        this();
        this.appName = appName;
        this.master = master;
    }

    /**
     * do not override the BaseContext toString and accidently expose secret properties. Add any custom toString here,
     * then call the super method which will return a properties JSON object
     * 
     * @return String - JSON formatted value
     */
    @Override
    public String toString() {
        return super.toString();
    }

    // expected to be in the properties map
    public String getExecutorMemory() {
        return getProperty(K.KEY_SPARK_EXECUTOR_MEMORY);
    }

    public void setExecutorMemory(String executorMemory) {
        addProperty(K.KEY_SPARK_EXECUTOR_MEMORY, executorMemory);
    }

    // expected to be in the properties map
    public String getMemoryOffHeapEnabled() {
        return getProperty(K.KEY_SPARK_MEMORY_OFFHEAP_ENABLED);
    }

    public void setMemoryOffHeapEnabled(String memoryOffHeapEnabled) {
        addProperty(K.KEY_SPARK_MEMORY_OFFHEAP_ENABLED, memoryOffHeapEnabled);
    }

    // expected to be in the properties map
    public String getMemoryOffHeapSize() {
        return getProperty(K.KEY_SPARK_MEMORY_OFFHEAP_SIZE);
    }

    public void setMemoryOffHeapSize(String memoryOffHeapSize) {
        addProperty(K.KEY_SPARK_MEMORY_OFFHEAP_SIZE, memoryOffHeapSize);
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

}
