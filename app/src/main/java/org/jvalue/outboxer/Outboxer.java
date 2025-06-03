package org.jvalue.outboxer;

import io.debezium.config.Configuration;
import io.debezium.engine.format.Json;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;

@Slf4j
public class Outboxer {

  private static final String DEFAULT_CONFIG_FILE = "/outboxer.properties";
  private static final String ENV_VAR_PREFIX = "outboxer.";
  private static final String STOP_TIMEOUT_MS_KEY = "stop.timeout.ms";

  private Configuration config;
  private DebeziumEngine<ChangeEvent<String, String>> engine;
  private CompositeChangeConsumer compositeConsumer;
  private ExecutorService executorService;

  public void init() {
    config = ConfigHelper.fromResource(DEFAULT_CONFIG_FILE)
        .edit()
        .apply(ConfigHelper.fromEnvVar(ENV_VAR_PREFIX))
        .build();

    String offsetFile = config.getString("offset.storage.file.filename");
    if (offsetFile != null) {
      try {
        Path offsetPath = Paths.get(offsetFile);
        if (Files.exists(offsetPath)) {
          Files.setPosixFilePermissions(offsetPath, PosixFilePermissions.fromString("rw-------"));
        }
      } catch (Exception e) {
        log.warn("Could not set permissions on offset file: " + offsetFile, e);
      }
    }

    compositeConsumer = new CompositeChangeConsumer(
        config.subset("publisher.", true).asProperties());

  }

  public void start() {
    if (config == null || compositeConsumer == null) {
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
        compositeConsumer.close();
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

    if (compositeConsumer != null) {
      try {
        compositeConsumer.close();
      } catch (IOException ignore) {
      }
    }
  }
}
