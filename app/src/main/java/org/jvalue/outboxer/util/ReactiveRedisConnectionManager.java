package org.jvalue.outboxer.util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.time.Duration;

@Slf4j
public class ReactiveRedisConnectionManager implements Closeable {
  private final RedisClient redisClient;
  private final StatefulRedisConnection<String, String> connection;
  private final RedisReactiveCommands<String, String> reactiveCommands;

  public ReactiveRedisConnectionManager(String redisUri, int redisPort) {
    RedisURI redisUrl = new RedisURI(redisUri, redisPort, Duration.ofSeconds(60));
    this.redisClient = RedisClient.create(redisUrl);
    this.connection = redisClient.connect();
    this.reactiveCommands = connection.reactive();
    log.info("Connected to Redis at {}:{}", redisUri, redisPort);
  }

  public RedisReactiveCommands<String, String> getReactiveCommands() {
    return reactiveCommands;
  }

  @Override
  public void close() {
    if (connection != null) {
      connection.close();
    }
    if (redisClient != null) {
      redisClient.shutdown();
    }
    log.info("Redis connection closed.");
  }
}
