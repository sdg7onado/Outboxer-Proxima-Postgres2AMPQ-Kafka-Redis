package org.jvalue.outboxer;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

@Slf4j
public class KafkaPublisher implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>>, Closeable {
  private static final String KAFKA_BOOTSTRAP_SERVERS_CONFIG_NAME = "kafka.bootstrap.servers";
  private static final String KAFKA_TOPIC_CONFIG_NAME = "kafka.topic";
  private static final String KAFKA_RETRIES_CONFIG_NAME = "kafka.retries";
  private static final String KAFKA_RETRY_DELAY_MS_CONFIG_NAME = "kafka.retry.delay.ms";

  private KafkaProducer<String, String> producer;
  private String topic;
  private int retries;
  private long retryDelayMs;

  public void init(Properties config) {
    this.topic = config.getProperty(KAFKA_TOPIC_CONFIG_NAME, "debezium.events");
    this.retries = Integer.parseInt(config.getProperty(KAFKA_RETRIES_CONFIG_NAME, "5"));
    this.retryDelayMs = Long.parseLong(config.getProperty(KAFKA_RETRY_DELAY_MS_CONFIG_NAME, "1000"));

    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", config.getProperty(KAFKA_BOOTSTRAP_SERVERS_CONFIG_NAME, "localhost:9092"));
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("acks", "all");
    this.producer = new KafkaProducer<>(producerProps);
  }

  @Override
  public void handleBatch(List<ChangeEvent<String, String>> records,
      DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
      throws InterruptedException {
    for (var record : records) {
      publishEvent(record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
  }

  void publishEvent(ChangeEvent<String, String> record) {
    String routingKey = record.destination();
    String eventId = record.key();
    String payload = record.value();

    log.info("Publishing to Kafka event {} with topic {} and payload {}", eventId, routingKey, payload);

    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, eventId, payload);

    for (int i = 0; i <= retries; i++) {
      try {
        producer.send(producerRecord).get(); // Synchronous send for simplicity
        return;
      } catch (Exception e) {
        if (i >= retries) {
          throw new RuntimeException("Could not publish event to Kafka after " + retries + " retries", e);
        }
        try {
          Thread.sleep(retryDelayMs);
        } catch (InterruptedException ignore) {
          // Ignore
        }
      }
    }
  }

  @Override
  public boolean supportsTombstoneEvents() {
    return false;
  }

  @Override
  public void close() throws IOException {
    if (producer != null) {
      producer.close();
    }
  }
}
