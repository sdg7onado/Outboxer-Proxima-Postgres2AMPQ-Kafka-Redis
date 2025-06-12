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

//import org.apache.kafka.common.utils.Utils.ThrowingRunnable;

@FunctionalInterface
interface ThrowingRunnable {
  void run() throws Exception;
}

@Slf4j
public class CompositeChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>>, Closeable {
  private final AmqpPublisher amqpPublisher = new AmqpPublisher();
  private final RedisPublisher redisPublisher = new RedisPublisher();
  private final KafkaPublisher kafkaPublisher = new KafkaPublisher();

  public CompositeChangeConsumer(Properties config) {
    initAll(config);
  }

  private void initAll(Properties config) {
    amqpPublisher.init(config);
    redisPublisher.init(config);
    kafkaPublisher.init(config);
  }

  @Override
  public void handleBatch(List<ChangeEvent<String, String>> records,
      DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
      throws InterruptedException {

    executeSafely("AMQP", () -> amqpPublisher.handleBatch(records, committer));
    executeSafely("Kafka", () -> kafkaPublisher.handleBatch(records, committer));

    try {
      Flux.merge(
          records.stream()
              .map(redisPublisher::persistData)
              .toList())
          .then().block();
    } catch (Exception e) {
      log.error("One or more Redis publishes failed: {}", e.getMessage(), e);
      throw e;
    }

    // Optional committer logic
    // for (var record : records) committer.markProcessed(record);
    // committer.markBatchFinished();
  }

  private void executeSafely(String name, ThrowingRunnable action) throws InterruptedException {
    try {
      action.run();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // restore interrupt status
      log.error("Interrupted while publishing to {}: {}", name, e.getMessage(), e);
      throw e;
    } catch (Exception e) {
      log.error("Failed to publish to {}: {}", name, e.getMessage(), e);
      throw new RuntimeException(e); // or handle as per your policy
    }
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
