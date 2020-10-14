package com.advancestores.enterprisecatalog.datafastlane.context;

import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;

/**
 * The connection context will have some standard items as "first class"
 * properties. Other properties may be added to a map. This will provide
 * flexibility to the connection context object.
 */
public class ConnectionContext extends BaseContext {
  // for convenience - some standard properties that are typically used in a
  // connection config
  public static final String OPTION_KEY_DRIVER = "driver";
  public static final String OPTION_KEY_HOST = "host";
  public static final String OPTION_KEY_PORT = "port";
  public static final String OPTION_KEY_USERNAME = "username";
  public static final String OPTION_KEY_PASSWORD = "password";
  public static final String OPTION_KEY_DATABASE = "database";
  public static final String OPTION_KEY_URL = "url";

  // for convenience - Elasticsearch
  public static final String OPTION_KEY_ENABLEWANONLY = "enablewanonly";
  public static final String OPTION_KEY_ENABLENETSSL = "enablenetssl";

  public ConnectionContext() throws FastLaneException {
    super();
  }

  /**
   * do not override the BaseContext toString and accidently expose secret
   * properties. Add any custom toString here, then call the super method
   * which will return a properties JSON object
   * 
   * @return String - JSON formatted value
   */
  public String toString() {
    return super.toString();
  }

  public String getDriver() {
    return getProperty(OPTION_KEY_DRIVER);
  }

  public ConnectionContext setDriver(String driver) {
    addProperty(OPTION_KEY_DRIVER, driver);
    return this;
  }

  public String getHost() {
    return getProperty(OPTION_KEY_HOST);
  }

  public ConnectionContext setHost(String host) {
    addProperty(OPTION_KEY_HOST, host);
    return this;
  }

  public String getPort() {
    return getProperty(OPTION_KEY_PORT);
  }

  public ConnectionContext setPort(String port) {
    addProperty(OPTION_KEY_PORT, port);
    return this;
  }

  public String getUsername() {
    return getProperty(OPTION_KEY_USERNAME);
  }

  public ConnectionContext setUsername(String username) {
    addProperty(OPTION_KEY_USERNAME, username);
    return this;
  }

  public String getPassword() {
    return getProperty(OPTION_KEY_PASSWORD);
  }

  public ConnectionContext setPassword(String password) {
    addProperty(OPTION_KEY_PASSWORD, password);
    return this;
  }

  public String getDatabase() {
    return getProperty(OPTION_KEY_DATABASE);
  }

  public ConnectionContext setDatabase(String database) {
    addProperty(OPTION_KEY_DATABASE, database);
    return this;
  }

  public String getUrl() {
    return getProperty(OPTION_KEY_URL);
  }

  public ConnectionContext setUrl(String url) {
    addProperty(OPTION_KEY_URL, url);
    return this;
  }

  public String getEnableWanOnly() {
    return getProperty(OPTION_KEY_ENABLEWANONLY);
  }

  public ConnectionContext setEnableWanOnly(String enableWanOnly) {
    addProperty(OPTION_KEY_ENABLEWANONLY, enableWanOnly);
    return this;
  }

  public String getEnableNetSsl() {
    return getProperty(OPTION_KEY_ENABLENETSSL);
  }

  public ConnectionContext setEnableNetSsl(String enableNetSsl) {
    addProperty(OPTION_KEY_ENABLENETSSL, enableNetSsl);
    return this;
  }

}
