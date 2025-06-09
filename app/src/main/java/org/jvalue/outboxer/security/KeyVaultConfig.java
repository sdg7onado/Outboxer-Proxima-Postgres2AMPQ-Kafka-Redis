package org.jvalue.outboxer.security;

import java.util.HashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;

@Configuration
public class KeyVaultConfig {

  @Bean
  public SecretClient secretClient() {
    return new SecretClientBuilder()
        .vaultUrl("https://your-keyvault.vault.azure.net/")
        .credential(new DefaultAzureCredentialBuilder().build())
        .buildClient();
  }

  @Bean
  @Primary
  public PropertySource<?> keyVaultPropertySource(SecretClient secretClient) {
    Map<String, Object> properties = new HashMap<>();

    // Fetch secrets and map to your property names
    properties.put("RABBITMQ_USER", secretClient.getSecret("rabbitmq-user").getValue());
    properties.put("RABBITMQ_PASS", secretClient.getSecret("rabbitmq-pass").getValue());
    properties.put("RABBITMQ_HOST", secretClient.getSecret("rabbitmq-host").getValue());

    return new MapPropertySource("keyVault", properties);
  }
}
