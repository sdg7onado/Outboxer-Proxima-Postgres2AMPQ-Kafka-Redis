package org.jvalue.outboxer.util;

import com.azure.data.appconfiguration.ConfigurationClient;
import com.azure.data.appconfiguration.ConfigurationClientBuilder;
import com.azure.data.appconfiguration.models.ConfigurationSetting;
import com.azure.data.appconfiguration.models.SettingSelector;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

public class ConfigurationManager {
  private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);

  private final Properties properties = new Properties();
  private final ConfigurationClient appConfigClient;
  private final SecretClient keyVaultClient;
  private final String activeProfile;
  private final List<String> secretNames;

  public ConfigurationManager() {
    this(System.getenv("AZURE_APP_CONFIG_CONNECTION_STRING"),
        System.getenv("AZURE_KEY_VAULT_ENDPOINT"),
        System.getenv("ACTIVE_PROFILE"),
        null);
  }

  public ConfigurationManager(String appConfigConnStr, String keyVaultEndpoint, String activeProfile,
      List<String> customSecrets) {
    this.appConfigClient = (appConfigConnStr != null && !appConfigConnStr.isEmpty())
        ? new ConfigurationClientBuilder().connectionString(appConfigConnStr).buildClient()
        : null;

    this.keyVaultClient = (keyVaultEndpoint != null && !keyVaultEndpoint.isEmpty())
        ? new SecretClientBuilder()
            .vaultUrl(keyVaultEndpoint)
            .credential(new DefaultAzureCredentialBuilder().build())
            .buildClient()
        : null;

    this.activeProfile = (activeProfile != null && !activeProfile.isEmpty())
        ? activeProfile
        : System.getProperty("active.profile", "default");

    this.secretNames = (customSecrets != null) ? customSecrets
        : List.of(
            "debezium-postgres-hostname",
            "debezium-postgres-port",
            "debezium-postgres-username",
            "debezium-postgres-password",
            "debezium-postgres-dbname",
            "debezium-postgres-schema",
            "debezium-postgres-database-server-name",

            "debezium-rabbitmq-host",
            "debezium-rabbitmq-port",
            "debezium-rabbitmq-username",
            "debezium-rabbitmq-password",

            "debezium-redis-username",
            "debezium-redis-password",
            "debezium-redis-host",
            "debezium-redis-port",

            "debezium-kafka-host",
            "debezium-kafka-port",
            "debezium-kafka-username",
            "debezium-kafka-password");

    loadConfiguration();
  }

  private void loadConfiguration() {
    properties.putAll(System.getProperties());
    System.getenv().forEach(properties::put);

    if (appConfigClient != null) {
      logger.info("Loading configuration from Azure App Configuration...");
      loadFromAppConfiguration();
    } else {
      logger.warn("Azure App Configuration is not configured.");
    }

    if (keyVaultClient != null) {
      logger.info("Loading secrets from Azure Key Vault...");
      loadFromKeyVault();
    } else {
      logger.warn("Azure Key Vault is not configured.");
    }

    logger.info("Configuration loaded for profile: {}", activeProfile);
  }

  private void loadFromAppConfiguration() {
    try {
      SettingSelector selector = new SettingSelector()
          .setKeyFilter("/publisher/*")
          .setLabelFilter(activeProfile);

      for (ConfigurationSetting setting : appConfigClient.listConfigurationSettings(selector)) {
        String key = setting.getKey().substring(1).replace("/", ".");
        String value = setting.getValue();

        if (value != null && value.startsWith("@Microsoft.KeyVault(")) {
          value = resolveKeyVaultReference(value);
        }

        properties.setProperty(key, value);
        logger.info("Loaded from App Config: {} = {}", key,
            key.toLowerCase().contains("password") ? "***" : value);
      }
    } catch (Exception e) {
      logger.error("Failed to load from Azure App Configuration: {}", e.getMessage(), e);
    }
  }

  private void loadFromKeyVault() {
    for (String secretName : secretNames) {
      try {
        KeyVaultSecret secret = keyVaultClient.getSecret(secretName);
        String propertyKey = secretName.replace("-", ".");
        properties.setProperty(propertyKey, secret.getValue());
        logger.info("Loaded secret: {}", propertyKey);
      } catch (Exception e) {
        logger.warn("Failed to load secret '{}': {}", secretName, e.getMessage());
      }
    }
  }

  private String resolveKeyVaultReference(String keyVaultReference) {
    try {
      int uriStart = keyVaultReference.indexOf("SecretUri=") + 10;
      String secretUri = keyVaultReference.substring(uriStart).replace(")", "");
      URI uri = new URI(secretUri);

      String[] pathParts = uri.getPath().split("/");
      if (pathParts.length < 3 || !"secrets".equalsIgnoreCase(pathParts[1])) {
        throw new IllegalArgumentException("Invalid Key Vault URI path format: " + uri.getPath());
      }

      String secretName = pathParts[2];

      if (keyVaultClient == null) {
        throw new IllegalStateException("KeyVaultClient not initialized for resolving references.");
      }

      return keyVaultClient.getSecret(secretName).getValue();
    } catch (Exception e) {
      logger.error("Failed to resolve Key Vault reference '{}': {}", keyVaultReference, e.getMessage(), e);
      return keyVaultReference;
    }
  }

  public String getProperty(String key) {
    return properties.getProperty(key);
  }

  public String getProperty(String key, String defaultValue) {
    return properties.getProperty(key, defaultValue);
  }

  public int getIntProperty(String key, int defaultValue) {
    String value = properties.getProperty(key);
    if (value != null) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        logger.warn("Invalid integer value for '{}': {}", key, value);
      }
    }
    return defaultValue;
  }

  public long getLongProperty(String key, long defaultValue) {
    String value = properties.getProperty(key);
    if (value != null) {
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException e) {
        logger.warn("Invalid long value for '{}': {}", key, value);
      }
    }
    return defaultValue;
  }

  public boolean getBooleanProperty(String key, boolean defaultValue) {
    String value = properties.getProperty(key);
    return (value != null) ? Boolean.parseBoolean(value) : defaultValue;
  }

  public Properties getAllProperties() {
    return new Properties(properties);
  }

  public void refresh() {
    logger.info("Refreshing configuration...");
    properties.clear();
    loadConfiguration();
  }
}
