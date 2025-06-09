package org.jvalue.outboxer;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

    // Publish to AMQP
    try {
      amqpPublisher.handleBatch(records, committer);
    } catch (Exception e) {
      log.error("Failed to publish to AMQP: {}", e.getMessage());
      throw e; // Rethrow to fail fast; adjust based on your error handling policy
    }

    // Publish to Kafka
    try {
      kafkaPublisher.handleBatch(records, committer);
    } catch (Exception e) {
      log.error("Failed to publish to Kafka: {}", e.getMessage());
      throw e;
    }

    // Collect all Redis publish calls
    List<Mono<Void>> redisPublishes = records.stream()
        // .map(redisPublisher::publishEvent)
        .map(redisPublisher::persistData)
        .toList();

    // Wait for all Redis events to complete
    try {
      Flux.merge(redisPublishes).then().block();
    } catch (Exception e) {
      log.error("One or more Redis publishes failed: {}", e.getMessage(), e);
      throw e;
    }

    for (var record : records) {
      // committer.markProcessed(record);
    }
    // committer.markBatchFinished();
  }

  @Override
  public boolean supportsTombstoneEvents() {
    return false;
  }

  @Override
  public void close() throws IOException {
    amqpPublisher.close();
    // redisPublisher.close();
    kafkaPublisher.close();
  }
}
