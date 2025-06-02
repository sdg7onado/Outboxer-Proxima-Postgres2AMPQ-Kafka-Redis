package org.jvalue.outboxer;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

@Slf4j
public class CompositeChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>>, Closeable {
  private final AmqpPublisher amqpPublisher;
  private final RedisPublisher redisPublisher;
  private final KafkaPublisher kafkaPublisher;

  public CompositeChangeConsumer(Properties config) {
    this.amqpPublisher = new AmqpPublisher();
    this.redisPublisher = new RedisPublisher();
    this.kafkaPublisher = new KafkaPublisher();
    this.amqpPublisher.init(config);
    this.redisPublisher.init(config);
    this.kafkaPublisher.init(config);
  }

  @Override
  public void handleBatch(List<ChangeEvent<String, String>> records,
      DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
      throws InterruptedException {
    for (var record : records) {
      // Publish to AMQP
      try {
        amqpPublisher.publishEvent(record);
      } catch (Exception e) {
        log.error("Failed to publish to AMQP: {}", e.getMessage());
        throw e; // Rethrow to fail fast; adjust based on your error handling policy
      }

      // Publish to Redis
      try {
        redisPublisher.publishEvent(record);
      } catch (Exception e) {
        log.error("Failed to publish to Redis: {}", e.getMessage());
        throw e;
      }

      // Publish to Kafka
      try {
        kafkaPublisher.publishEvent(record);
      } catch (Exception e) {
        log.error("Failed to publish to Kafka: {}", e.getMessage());
        throw e;
      }

      committer.markProcessed(record);
    }
    committer.markBatchFinished();
  }

  @Override
  public boolean supportsTombstoneEvents() {
    return false;
  }

  @Override
  public void close() throws IOException {
    amqpPublisher.close();
    redisPublisher.close();
    kafkaPublisher.close();
  }
}
