package org.jvalue.outboxer;

import io.debezium.engine.ChangeEvent;
import io.lettuce.core.RedisURI;
import lombok.extern.slf4j.Slf4j;
import org.jvalue.outboxer.util.ReactiveRedisConnectionManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedisPublisher implements Closeable {
  private static final String REDIS_URL_CONFIG_NAME = "redis.host";
  private static final String REDIS_CHANNEL_CONFIG_NAME = "redis.channel";
  private static final String REDIS_RETRIES_CONFIG_NAME = "redis.retries";
  private static final String REDIS_RETRY_DELAY_MS_CONFIG_NAME = "redis.retry.delay.ms";

  private ReactiveRedisConnectionManager redisManager;
  private String channel;
  private String deadLetterChannel;
  private int retries;
  private long retryDelayMs;

  public void init(Properties config) {
    String redisUrl = config.getProperty(REDIS_URL_CONFIG_NAME, "redis://127.0.0.1");
    int redisPort = Integer.parseInt((config.getProperty("redis.port", "6379")));
    this.channel = config.getProperty(REDIS_CHANNEL_CONFIG_NAME, "debezium.events");
    this.deadLetterChannel = channel + ".deadletter";
    this.retries = Integer.parseInt(config.getProperty(REDIS_RETRIES_CONFIG_NAME, "5"));
    this.retryDelayMs = Long.parseLong(config.getProperty(REDIS_RETRY_DELAY_MS_CONFIG_NAME, "1000"));

    this.redisManager = new ReactiveRedisConnectionManager(redisUrl, redisPort);
  }

  public Mono<Void> publishEvent(ChangeEvent<String, String> record) {
    String eventId = record.key();
    String payload = record.value();

    return redisManager.getReactiveCommands().publish(channel, payload)
        .doOnNext(res -> log.info("Published event {} to {}", eventId, channel))
        .then()
        .retryWhen(Retry.fixedDelay(retries, Duration.ofMillis(retryDelayMs))
            .doBeforeRetry(
                sig -> log.warn("Retrying publish for event {} attempt {}", eventId, sig.totalRetries() + 1)))
        .onErrorResume(e -> {
          log.error("Publishing to main channel failed for event {}, sending to dead-letter", eventId, e);
          return redisManager.getReactiveCommands().publish(deadLetterChannel, payload)
              .doOnNext(x -> log.info("Sent event {} to dead-letter channel", eventId))
              .then();
        });
  }

  // public Mono<Void> persistData(ChangeEvent<String, String> record) {
  //   String eventId = record.key();

  //   return Mono.fromCallable(() -> {
  //     JsonNode root = new ObjectMapper().readTree(record.value());
  //     return root.get("payload").asText();
  //   })
  //       .flatMap(payload -> redisManager.getReactiveCommands().set(eventId, payload)
  //           .doOnNext(res -> log.info("Stored event {} with result {}", eventId, res))
  //           .then()
  //           .retryWhen(Retry.fixedDelay(retries, Duration.ofMillis(retryDelayMs))
  //               .doBeforeRetry(
  //                   sig -> log.warn("Retrying persist for event {} attempt {}", eventId, sig.totalRetries() + 1)))
  //           .onErrorResume(e -> {
  //             log.error("Storing to Redis failed for event {}, sending to dead-letter", eventId, e);
  //             return redisManager.getReactiveCommands().set("dead-letter:" + eventId, payload)
  //                 .doOnNext(x -> log.info("Sent event {} to dead-letter key", eventId))
  //                 .then();
  //           }));
  // }

  public Mono<Void> persistData(ChangeEvent<String, String> record) {
    return Mono.fromCallable(() -> {
        ObjectMapper mapper = new ObjectMapper();

        // Extract eventId from key payload
        JsonNode keyNode = mapper.readTree(record.key());
        String eventId = keyNode.get("payload").asText();

        // Extract JSON payload from value payload
        JsonNode valueNode = mapper.readTree(record.value());
        String payloadJson = valueNode.get("payload").asText(); // still a JSON string

        return Tuples.of(eventId, payloadJson);
    })
    .flatMap(tuple -> {
        String eventId = tuple.getT1();
        String payloadJson = tuple.getT2();

        return redisManager.getReactiveCommands().set(eventId, payloadJson)
            .doOnNext(res -> log.info("Stored event {} with result {}", eventId, res))
            .then()
            .retryWhen(Retry.fixedDelay(retries, Duration.ofMillis(retryDelayMs))
                .doBeforeRetry(sig -> log.warn("Retrying persist for event {} attempt {}", eventId, sig.totalRetries() + 1)))
            .onErrorResume(e -> {
                log.error("Storing to Redis failed for event {}, sending to dead-letter", eventId, e);
                return redisManager.getReactiveCommands().set("dead-letter:" + eventId, payloadJson)
                    .doOnNext(x -> log.info("Sent event {} to dead-letter key", eventId))
                    .then();
            });
    });
}


  @Override
  public void close() throws IOException {
    if (redisManager != null) {
      redisManager.close();
    }
  }
}
