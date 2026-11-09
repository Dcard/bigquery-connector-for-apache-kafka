/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.convert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Storage Write API rows carry TIMESTAMP values as epoch microseconds, not strings. */
class StorageWriteApiTimestampConverterTest {

  private static final String FIELD = "ts";

  private static BigQuerySinkConfig config(boolean convertDebeziumTimestampToInteger) {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getVariableScaleDecimalHandlingMode())
        .thenReturn(BigQuerySinkConfig.DecimalHandlingMode.NUMERIC);
    when(config.getDecimalHandlingMode())
        .thenReturn(BigQuerySinkConfig.DecimalHandlingMode.NUMERIC);
    when(config.getShouldConvertDebeziumTimestampToInteger())
        .thenReturn(convertDebeziumTimestampToInteger);
    return config;
  }

  private static void initialize(boolean convertDebeziumTimestampToInteger) {
    BigQuerySinkConfig config = config(convertDebeziumTimestampToInteger);
    DebeziumLogicalConverters.initialize(config);
    KafkaLogicalConverters.initialize(config);
  }

  @AfterEach
  void tearDown() {
    // The registry is static and registerIfAbsent, so reset it for the next test
    DebeziumLogicalConverters.remove();
    KafkaLogicalConverters.remove();
  }

  private static Object convert(Schema fieldSchema, Object value, boolean useStorageWriteApi) {
    Schema schema = SchemaBuilder.struct().field(FIELD, fieldSchema).build();
    Struct struct = new Struct(schema).put(FIELD, value);
    SinkRecord record = new SinkRecord("topic", 0, null, null, schema, struct, 0);
    Map<String, Object> row =
        new BigQueryRecordConverter(false, useStorageWriteApi)
            .convertRecord(record, KafkaSchemaRecordType.VALUE);
    return row.get(FIELD);
  }

  private static long micros(String instant) {
    Instant i = Instant.parse(instant);
    return i.getEpochSecond() * 1_000_000L + i.getNano() / 1000;
  }

  static Stream<Arguments> timestamps() {
    return Stream.of(
        Arguments.of(
            "Debezium MicroTimestamp",
            io.debezium.time.MicroTimestamp.schema(),
            micros("2026-11-04T12:34:56.123456Z"),
            micros("2026-11-04T12:34:56.123456Z")),
        Arguments.of(
            "Debezium MicroTimestamp before 1970",
            io.debezium.time.MicroTimestamp.schema(),
            micros("1969-12-31T23:59:59.999999Z"),
            micros("1969-12-31T23:59:59.999999Z")),
        Arguments.of(
            "Debezium Timestamp (millis)",
            io.debezium.time.Timestamp.schema(),
            1611854944123L,
            micros("2021-01-28T17:29:04.123Z")),
        Arguments.of(
            "Kafka Timestamp",
            Timestamp.SCHEMA,
            new Date(1611854944123L),
            micros("2021-01-28T17:29:04.123Z")),
        Arguments.of(
            "Debezium ZonedTimestamp in UTC",
            io.debezium.time.ZonedTimestamp.schema(),
            "2026-11-04T12:34:56.123456Z",
            micros("2026-11-04T12:34:56.123456Z")),
        Arguments.of(
            "Debezium ZonedTimestamp with an offset",
            io.debezium.time.ZonedTimestamp.schema(),
            "2026-11-04T20:34:56.5+08:00",
            micros("2026-11-04T12:34:56.5Z")),
        Arguments.of(
            "Debezium ZonedTimestamp, nanos truncated to micros",
            io.debezium.time.ZonedTimestamp.schema(),
            "2026-11-04T12:34:56.123456789Z",
            micros("2026-11-04T12:34:56.123456Z")),
        Arguments.of(
            "Debezium ZonedTimestamp at BigQuery's earliest TIMESTAMP",
            io.debezium.time.ZonedTimestamp.schema(),
            "0001-01-01T00:00:00Z",
            micros("0001-01-01T00:00:00Z")),
        Arguments.of(
            "Debezium MicroTimestamp at BigQuery's latest TIMESTAMP",
            io.debezium.time.MicroTimestamp.schema(),
            micros("9999-12-31T23:59:59.999999Z"),
            micros("9999-12-31T23:59:59.999999Z")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("timestamps")
  void storageWriteApiUsesEpochMicros(String name, Schema schema, Object value, long expected) {
    initialize(false);
    assertEquals(expected, convert(schema, value, true));
  }

  static Stream<Arguments> outOfRange() {
    long earliest = micros("0001-01-01T00:00:00Z");
    long latest = micros("9999-12-31T23:59:59.999999Z");
    return Stream.of(
        // Postgres 0001-01-01 00:00:00+00 (Go's zero time), as Debezium formats it
        Arguments.of(
            "Debezium ZonedTimestamp in year 0",
            io.debezium.time.ZonedTimestamp.schema(),
            "0000-12-30T00:00:00Z",
            earliest),
        Arguments.of(
            "Debezium MicroTimestamp just before year 1",
            io.debezium.time.MicroTimestamp.schema(),
            earliest - 1,
            earliest),
        Arguments.of(
            "Debezium MicroTimestamp after year 9999",
            io.debezium.time.MicroTimestamp.schema(),
            latest + 1,
            latest),
        Arguments.of(
            "Debezium Timestamp whose micros overflow",
            io.debezium.time.Timestamp.schema(),
            Long.MIN_VALUE / 10,
            earliest),
        Arguments.of(
            "Kafka Timestamp whose micros overflow",
            Timestamp.SCHEMA,
            new Date(Long.MAX_VALUE / 10),
            latest),
        Arguments.of(
            "Debezium ZonedTimestamp in year +1000000",
            io.debezium.time.ZonedTimestamp.schema(),
            "+1000000-01-01T00:00:00Z",
            latest));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("outOfRange")
  void outOfRangeIsClamped(String name, Schema schema, Object value, long expected) {
    initialize(false);
    // BigQuery rejects values outside its range, which would fail the whole row
    assertEquals(expected, convert(schema, value, true));
  }

  @Test
  void unparseableFallsBackToTheLogicalConverter() {
    initialize(false);
    // The logical converter rejects it the same way as before
    assertThrows(
        java.time.format.DateTimeParseException.class,
        () -> convert(io.debezium.time.ZonedTimestamp.schema(), "not a timestamp", true));
  }

  @Test
  void insertAllKeepsTheStrings() {
    initialize(false);
    assertEquals(
        "2021-01-28 17:29:04.123",
        convert(io.debezium.time.Timestamp.schema(), 1611854944123L, false));
    assertEquals(
        "2026-11-04 12:34:56.123456Z",
        convert(io.debezium.time.ZonedTimestamp.schema(), "2026-11-04T12:34:56.123456Z", false));
  }

  @Test
  void debeziumTimestampAsIntegerStaysMillis() {
    // convertDebeziumTimestampToInteger makes the column INTEGER, so the value stays millis
    initialize(true);
    assertEquals(
        1611854944123L, convert(io.debezium.time.Timestamp.schema(), 1611854944123L, true));
  }

  @Test
  void nullStaysNull() {
    initialize(false);
    assertEquals(
        null, convert(io.debezium.time.ZonedTimestamp.builder().optional().build(), null, true));
  }
}
