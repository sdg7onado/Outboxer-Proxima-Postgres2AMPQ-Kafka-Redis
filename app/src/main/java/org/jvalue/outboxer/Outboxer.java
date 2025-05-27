package org.jvalue.outboxer;

import io.debezium.config.Configuration;
import io.debezium.data.Json;
import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
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
  private DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
  private AmqpPublisher amqpPublisher;
  private ExecutorService executorService;

  public void init() {
    config = ConfigHelper.fromResource(DEFAULT_CONFIG_FILE)
        .edit()
        .apply(ConfigHelper.fromEnvVar(ENV_VAR_PREFIX))
        .build();

    amqpPublisher = new AmqpPublisher();
    amqpPublisher.init(config.subset("publisher.", true).asProperties());

  }

  public void start() {
    if (config == null || amqpPublisher == null) {
      throw new IllegalStateException("Outboxer is not initialized.");
    }

    engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
        .using(config.asProperties())
        .notifying(record -> {
          SourceRecord sourceRecord = record.record();
          if (sourceRecord != null) {
            amqpPublisher.publishEvent(sourceRecord);
          }
        })
        .build();

    executorService = Executors.newSingleThreadExecutor();
    executorService.execute(engine);
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
