package com.advancestores.enterprisecatalog.datafastlane.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Test;

import com.advancestores.enterprisecatalog.datafastlane.recipe.Option;

/**
 * Test the ConnectionContext class.
 */
class ConnectionContextTest {

    @Test
    void testConstructorWithStandardChain() {
        String driver = "some-driver";
        String host = "somehost.com";
        String port = "3300";
        String username = "j.doe";
        String password = "password4j.doe";
        String database = "some-database";
        String url = "jdbc:xyz";
        String enableWanOnly = "false";
        String enableNetSsl = "true";

        ConnectionContext ctx = new ConnectionContext().setDriver(driver)
                                                       .setHost(host)
                                                       .setPort(port)
                                                       .setUsername(username)
                                                       .setPassword(password)
                                                       .setDatabase(database)
                                                       .setUrl(url)
                                                       .setEnableWanOnly(enableWanOnly)
                                                       .setEnableNetSsl(enableNetSsl);

        assertEquals(driver, ctx.getDriver());
        assertEquals(host, ctx.getHost());
        assertEquals(port, ctx.getPort());
        assertEquals(username, ctx.getUsername());
        assertEquals(password, ctx.getPassword());
        assertEquals(database, ctx.getDatabase());
        assertEquals(url, ctx.getUrl());
        assertEquals(enableWanOnly, ctx.getEnableWanOnly());
        assertEquals(enableNetSsl, ctx.getEnableNetSsl());
    }

    @Test
    void testProperties() {
        Option option = new Option("Option1", "option1_value");
        ConnectionContext ctx = new ConnectionContext();

        ctx.addProperty("key1", "value4key1");
        ctx.addProperty(option);
        ctx.addProperty("null-value", null);
        ctx.addProperty(null, "null-key");
        ctx.addProperty(null, null);
        ctx.addProperty("empty-value", "");
        ctx.addProperty("", "empty-key");

        assertEquals(2, ctx.getPropertyKeys().length);
        assertNull(ctx.getProperty("null-value"));
        assertNull(ctx.getProperty("empty-value"));
        assertNull(ctx.getProperty(null));
        assertNull(ctx.getProperty(""));
        assertTrue(ctx.containsKey("key1"));
        assertTrue(ctx.containsKey("Option1"));
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
