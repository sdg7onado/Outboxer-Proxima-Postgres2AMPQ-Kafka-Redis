package org.jvalue.outboxer;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

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
  private Properties kafkaProps;

  // Simple metrics tracking
  private final AtomicInteger totalEvents = new AtomicInteger();
  private final AtomicInteger failedEvents = new AtomicInteger();
  private final AtomicInteger retryCount = new AtomicInteger();

  public void init(Properties config) {
    this.topic = config.getProperty(KAFKA_TOPIC_CONFIG_NAME, "debezium.events");
    this.retries = Integer.parseInt(config.getProperty(KAFKA_RETRIES_CONFIG_NAME, "5"));
    this.retryDelayMs = Long.parseLong(config.getProperty(KAFKA_RETRY_DELAY_MS_CONFIG_NAME, "1000"));

    this.kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", config.getProperty(KAFKA_BOOTSTRAP_SERVERS_CONFIG_NAME, "localhost:9092"));
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("acks", "all");

    this.producer = new KafkaProducer<>(kafkaProps);
  }

  private synchronized void ensureProducerAlive() {
    try {
      producer.partitionsFor(topic);
    } catch (Exception e) {
      log.warn("KafkaProducer appears to be closed or broken. Reinitializing...", e);
      try {
        producer.close();
      } catch (Exception ignored) {
      }
      this.producer = new KafkaProducer<>(kafkaProps);
    }
  }

  @Override
  public void handleBatch(List<ChangeEvent<String, String>> records,
      DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
      throws InterruptedException {
    for (var record : records) {
      try {
        publishEvent(record);
        //committer.markProcessed(record);
      } catch (Exception e) {
        log.error("Failed to publish record {}: {}", record.key(), e.getMessage(), e);
        failedEvents.incrementAndGet();
      }
    }
    //committer.markBatchFinished();
    log.info("Metrics -- Total: {}, Failed: {}, Retries: {}", totalEvents.get(), failedEvents.get(), retryCount.get());
  }

  /*
   * void publishEvent(ChangeEvent<String, String> record) {
   * String routingKey = record.destination();
   * String eventId = record.key();
   * String payload = record.value();
   *
   * log.info("Publishing to Kafka event {} with topic {} and payload {}",
   * eventId, routingKey, payload);
   *
   * ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,
   * eventId, payload);
   *
   * for (int i = 0; i <= retries; i++) {
   * try {
   * producer.send(producerRecord).get(); // Synchronous send for simplicity
   * return;
   * } catch (Exception e) {
   * if (i >= retries) {
   * throw new RuntimeException("Could not publish event to Kafka after " +
   * retries + " retries", e);
   * }
   * try {
   * Thread.sleep(retryDelayMs);
   * } catch (InterruptedException ignore) {
   * // Ignore
   * }
   * }
   * }
   * }
   */

  public void publishEvent(ChangeEvent<String, String> record) {
    String key = record.key();
    String value = record.value();
    String routingKey = record.destination();

    log.info("Publishing to Kafka event {} with topic {} and payload {}", key, routingKey, value);

    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
    totalEvents.incrementAndGet();

    for (int attempt = 0; attempt <= retries; attempt++) {
      ensureProducerAlive();
      boolean isFinalAttempt = attempt == retries;
      final int currentAttempt = attempt;

      Object lock = new Object();
      final boolean[] completed = { false };
      final Exception[] exception = { null };

      producer.send(producerRecord, (metadata, ex) -> {
        synchronized (lock) {
          if (ex != null) {
            log.warn("Kafka publish attempt {} failed: {}", currentAttempt + 1, ex.getMessage());
            exception[0] = ex;
          }
          completed[0] = true;
          lock.notify();
        }
      });

      synchronized (lock) {
        while (!completed[0]) {
          try {
            lock.wait();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Kafka publish interrupted", ie);
          }
        }
      }

      if (exception[0] == null) {
        return; // Success
      }

      retryCount.incrementAndGet();

      if (isFinalAttempt) {
        failedEvents.incrementAndGet();
        throw new RuntimeException("Could not publish event to Kafka after " + retries + " retries", exception[0]);
      }

      try {
        Thread.sleep(retryDelayMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Kafka retry sleep interrupted", e);
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
