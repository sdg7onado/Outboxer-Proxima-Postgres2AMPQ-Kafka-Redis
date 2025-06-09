package org.jvalue.outboxer;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is a {@link io.debezium.engine.DebeziumEngine.ChangeConsumer} that
 * publishes the events via AMQP.
 */
@Slf4j
public class AmqpPublisher implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>>, Closeable {

  private static final String AMQP_URL_CONFIG_NAME = "amqp.url";
  private static final String AMQP_EXCHANGE_CONFIG_NAME = "amqp.exchange";
  private static final String AMQP_RETRIES_CONFIG_NAME = "amqp.retries";
  private static final String AMQP_RETRY_DELAY_MS_CONFIG_NAME = "amqp.retry.delay.ms";

  private CachingConnectionFactory connectionFactory;
  private AmqpTemplate template;
  private String exchange;
  private int retries;
  private long retryDelayMs;

  public void init(Properties config) {
    this.exchange = config.getProperty(AMQP_EXCHANGE_CONFIG_NAME);
    this.retries = Integer.parseInt(config.getProperty(AMQP_RETRIES_CONFIG_NAME, "5"));
    this.retryDelayMs = Long.parseLong(config.getProperty(AMQP_RETRY_DELAY_MS_CONFIG_NAME, "1000"));
    this.connectionFactory = new CachingConnectionFactory(URI.create(config.getProperty(AMQP_URL_CONFIG_NAME)));
    this.template = new RabbitTemplate(connectionFactory);
  }

  @Override
  public void handleBatch(List<ChangeEvent<String, String>> records,
      DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
      throws InterruptedException {
    for (var record : records) {
      publishEvent(record);
      //committer.markProcessed(record);
    }
    //committer.markBatchFinished();
  }

  void publishEvent(ChangeEvent<String, String> record) {
    var routingKey = record.destination(); // Use destination() instead of topic()
    var eventId = record.key();
    var payload = record.value();

    log.info("Publishing event {} with routingKey {} and payload {}", eventId, routingKey, payload);

    var message = createAmqpMessage(eventId, payload);

    for (int i = 0; i <= retries; i++) {
      try {
        template.send(exchange, routingKey, message);
        return;
      } catch (RuntimeException e) {
        if (i >= retries) {
          throw new AmqpException("Could not publish event after " + retries + " retries", e);
        }
        try {
          Thread.sleep(retryDelayMs);
        } catch (InterruptedException ignore) {
          // Ignore
        }
      }
    }
  }

  private Message createAmqpMessage(String eventId, String payload) {
    var messageProps = new MessageProperties();
    messageProps.setContentType(MessageProperties.CONTENT_TYPE_JSON);
    messageProps.setContentEncoding(StandardCharsets.UTF_8.name());
    messageProps.setMessageId(eventId);
    messageProps.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
    return new Message(payload.getBytes(StandardCharsets.UTF_8), messageProps);
  }

  @Override
  public boolean supportsTombstoneEvents() {
    return false;
  }

  @Override
  public void close() throws IOException {
    if (connectionFactory != null) {
      connectionFactory.resetConnection();
    }
  }
}
