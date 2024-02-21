package org.jvalue.outboxer;

import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;

/**
 * This class contains the configuration definition for the {@link OutboxTableTransform}.
 */
public class OutboxTableTransformConfigDef {
  static final Field FIELD_EVENT_ID = Field.create("table.field.event.Id")
    .withDisplayName("Event ID field")
    .withType(ConfigDef.Type.STRING)
    .withDefault("Id")
    .withDescription("The column which contains the event Id within the outbox table");

  static final Field FIELD_EVENT_ROUTING_KEY = Field.create("table.field.event.AggregateName")
    .withDisplayName("Event routing key field")
    .withType(ConfigDef.Type.STRING)
    .withDefault("AggregateName")
    .withDescription("The column which contains the event routing key within the outbox table");

  static final Field FIELD_EVENT_AGGREGATEID = Field.create("table.field.event.AggregateId")
    .withDisplayName("Event AggregateId field")
    .withType(ConfigDef.Type.INT)
    .withDefault("AggregateId")
    .withDescription("The column which contains the event AggregateId within the outbox table");

  static final Field FIELD_EVENT_EVENTNAME = Field.create("table.field.event.EventName")
    .withDisplayName("Event EventName field")
    .withType(ConfigDef.Type.STRING)
    .withDefault("EventName")
    .withDescription("The column which contains the event EventName within the outbox table");

  static final Field FIELD_EVENT_PAYLOAD = Field.create("table.field.event.Payload")
    .withDisplayName("Event payload field")
    .withType(ConfigDef.Type.STRING)
    .withDefault("Payload")
    .withDescription("The column which contains the event Payload within the outbox table");

  static final Field FIELD_EVENT_CREATEDATE = Field.create("table.field.event.CreateDate")
    .withDisplayName("Event CreateDate field")
    .withType(ConfigDef.Type.STRING)
    .withDefault("CreateDate")
    .withDescription("The column which contains the event CreateDate within the outbox table");

  static final Field FIELD_EVENT_LASTEDITDATE = Field.create("table.field.event.LastEditDate")
    .withDisplayName("Event LastEditDate field")
    .withType(ConfigDef.Type.STRING)
    .withDefault("LastEditDate")
    .withDescription("The column which contains the event LastEditDate within the outbox table");

  static ConfigDef get() {
    var configDef = new ConfigDef();
    Field.group(configDef,
      "table",
                FIELD_EVENT_ID,
                FIELD_EVENT_ROUTING_KEY,
                FIELD_EVENT_AGGREGATEID,
                FIELD_EVENT_EVENTNAME,
                FIELD_EVENT_PAYLOAD,
                FIELD_EVENT_CREATEDATE,
                FIELD_EVENT_LASTEDITDATE);
    return configDef;
  }
}
