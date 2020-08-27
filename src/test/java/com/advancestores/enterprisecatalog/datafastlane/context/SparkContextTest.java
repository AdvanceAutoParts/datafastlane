package com.advancestores.enterprisecatalog.datafastlane.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Test;

import com.advancestores.enterprisecatalog.datafastlane.K;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Option;

/**
 * Test the SparkContext class.
 */
class SparkContextTest {

    @Test
    void testConstructorWithParams() {
        String appName = "application name";
        String master = "local[a*]";
        SparkContext ctx = new SparkContext(appName, master);

        assertEquals(appName, ctx.getAppName());
        assertEquals(master, ctx.getMaster());
        assertNull(ctx.getExecutorMemory());
        assertNull(ctx.getMemoryOffHeapEnabled());
        assertNull(ctx.getMemoryOffHeapSize());
        assertNull(ctx.getProperty(K.KEY_SPARK_EXECUTOR_MEMORY));
        assertNull(ctx.getProperty(K.KEY_SPARK_MEMORY_OFFHEAP_ENABLED));
        assertNull(ctx.getProperty(K.KEY_SPARK_MEMORY_OFFHEAP_SIZE));
    }

    @Test
    void testConstructorNoParams() {
        SparkContext ctx = new SparkContext();

        assertNull(ctx.getAppName());
        assertNull(ctx.getExecutorMemory());
        assertNull(ctx.getMemoryOffHeapEnabled());
        assertNull(ctx.getMemoryOffHeapSize());
    }

    @Test
    void testSetters() {
        String appName = "application name";
        String master = "local[a*]";
        String executorMemory = "16g";
        String memoryOffHeapEnabled = "false";
        String memoryOffHeapSize = "64g";

        SparkContext ctx = new SparkContext();

        ctx.setAppName(appName);
        ctx.setMaster(master);
        ctx.addProperty(K.KEY_SPARK_EXECUTOR_MEMORY, executorMemory);
        ctx.addProperty(K.KEY_SPARK_MEMORY_OFFHEAP_ENABLED, memoryOffHeapEnabled);

        assertEquals(appName, ctx.getAppName());
        assertEquals(master, ctx.getMaster());
        assertEquals(memoryOffHeapEnabled, ctx.getMemoryOffHeapEnabled());
        assertNull(ctx.getMemoryOffHeapSize());
        assertEquals(executorMemory, ctx.getProperty(K.KEY_SPARK_EXECUTOR_MEMORY));
        assertEquals(memoryOffHeapEnabled, ctx.getProperty(K.KEY_SPARK_MEMORY_OFFHEAP_ENABLED));
        assertEquals(memoryOffHeapSize, ctx.getProperty(K.KEY_SPARK_MEMORY_OFFHEAP_SIZE, memoryOffHeapSize));
    }

    @Test
    void tesProperties() {
        String appName = "application name";
        String master = "local[a*]";
        String executorMemory = "16g";
        String memoryOffHeapSize = "64g";
        Option option = new Option(K.KEY_SPARK_MEMORY_OFFHEAP_SIZE, memoryOffHeapSize);

        SparkContext ctx = new SparkContext();

        ctx.setAppName(appName);
        ctx.setMaster(master);
        ctx.addProperty(K.KEY_SPARK_EXECUTOR_MEMORY, executorMemory);
        ctx.addProperty(option);
        ctx.addProperty("null-value", null);
        ctx.addProperty(null, "null-key");
        ctx.addProperty(null, null);
        ctx.addProperty("empty-value", "");
        ctx.addProperty("", "empty-key");

        assertEquals(appName, ctx.getAppName());
        assertEquals(master, ctx.getMaster());
        assertEquals(2, ctx.getPropertyKeys().length);
        assertNull(ctx.getMemoryOffHeapEnabled());
        assertEquals(memoryOffHeapSize, ctx.getMemoryOffHeapSize());
        assertEquals(executorMemory, ctx.getProperty(K.KEY_SPARK_EXECUTOR_MEMORY));
        assertNull(ctx.getProperty("null-value"));
        assertNull(ctx.getProperty("empty-value"));
        assertNull(ctx.getProperty(null));
        assertNull(ctx.getProperty(""));
        assertTrue(ctx.containsKey(K.KEY_SPARK_EXECUTOR_MEMORY));
        assertFalse(ctx.containsKey("missing"));
        assertFalse(ctx.containsKey(null));
        assertFalse(ctx.containsKey(""));

        try {
            ctx.addProperty("some-key", "${env:some-key}");
            fail("should not have set property");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("expected variable subsitution failed"));
        }

        for (String key : BaseContext.PROTECTED_PROPERTIES) {
            ConnectionContext localctx = (ConnectionContext) new ConnectionContext().addProperty(key, "my-" + key);
            assertTrue(StringUtils.deleteWhitespace(localctx.toString()).contains("\"" + key + "\":\"************\""));
        }

    }
}
