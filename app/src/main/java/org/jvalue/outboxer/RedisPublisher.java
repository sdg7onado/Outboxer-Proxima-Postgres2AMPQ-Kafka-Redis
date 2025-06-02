package org.jvalue.outboxer;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

@Slf4j
public class RedisPublisher implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>>, Closeable {
  private static final String REDIS_URL_CONFIG_NAME = "redis.url";
  private static final String REDIS_CHANNEL_CONFIG_NAME = "redis.channel";
  private static final String REDIS_RETRIES_CONFIG_NAME = "redis.retries";
  private static final String REDIS_RETRY_DELAY_MS_CONFIG_NAME = "redis.retry.delay.ms";

  private JedisPool jedisPool;
  private String channel;
  private int retries;
  private long retryDelayMs;

  public void init(Properties config) {
    String redisUrl = config.getProperty(REDIS_URL_CONFIG_NAME, "redis://localhost:6379");
    this.channel = config.getProperty(REDIS_CHANNEL_CONFIG_NAME, "debezium.events");
    this.retries = Integer.parseInt(config.getProperty(REDIS_RETRIES_CONFIG_NAME, "5"));
    this.retryDelayMs = Long.parseLong(config.getProperty(REDIS_RETRY_DELAY_MS_CONFIG_NAME, "1000"));
    this.jedisPool = new JedisPool(new JedisPoolConfig(), redisUrl);
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

    log.info("Publishing to Redis event {} with channel {} and payload {}", eventId, routingKey, payload);

    for (int i = 0; i <= retries; i++) {
      try (Jedis jedis = jedisPool.getResource()) {
        // Publish to Redis channel
        jedis.publish(channel, payload);
        // Optionally, store in key-value store
        jedis.set("event:" + eventId, payload);
        return;
      } catch (Exception e) {
        if (i >= retries) {
          throw new RuntimeException("Could not publish event to Redis after " + retries + " retries", e);
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
    if (jedisPool != null) {
      jedisPool.close();
    }
  }
}
