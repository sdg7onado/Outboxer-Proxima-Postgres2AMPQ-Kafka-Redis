package org.jvalue.outboxer;

import io.debezium.config.Configuration;
import io.debezium.engine.format.Json;
import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;

@Slf4j
public class Outboxer {

  private static final String DEFAULT_CONFIG_FILE = "/outboxer.properties";
  private static final String ENV_VAR_PREFIX = "outboxer.";
  private static final String STOP_TIMEOUT_MS_KEY = "stop.timeout.ms";

  private Configuration config;
  private DebeziumEngine<ChangeEvent<String, String>> engine;
  private AmqpPublisher amqpPublisher;
  private CompositeChangeConsumer compositeConsumer;
  private ExecutorService executorService;

  public void init() {
    config = ConfigHelper.fromResource(DEFAULT_CONFIG_FILE)
        .edit()
        .apply(ConfigHelper.fromEnvVar(ENV_VAR_PREFIX))
        .build();

    amqpPublisher = new AmqpPublisher();
    amqpPublisher.init(config.subset("publisher.", true).asProperties());

    compositeConsumer = new CompositeChangeConsumer(
        config.subset("publisher.", true).asProperties());

  }

  public void start() {
    if (config == null || amqpPublisher == null) {
      throw new IllegalStateException("Outboxer is not initialized.");
    }

    try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
        .using(config.asProperties())
        .notifying(
            compositeConsumer)
        .build()) {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.execute(engine);

      // Wait for some time or a signal
      Thread.sleep(60000);
      executor.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        amqpPublisher.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void stop() {
    log.info("Stopping the Outboxer");
    if (engine != null) {
      try {
        engine.close();
        executorService.shutdown();
        executorService.awaitTermination(
            config.getInteger(STOP_TIMEOUT_MS_KEY, 5000),
            TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        log.warn("Exception while stopping engine", e);
      }
    }

    if (amqpPublisher != null) {
      try {
        amqpPublisher.close();
      } catch (IOException ignore) {
      }
    }
  }
}
