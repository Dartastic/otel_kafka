// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_kafka/otel_kafka.dart';

/// Traces Kafka calls with OpenTelemetry.
///
/// `otel_kafka` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Kafka call in
/// [tracedKafkaProduce]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'kafka-example');

  // Wraps a Kafka produce in a PRODUCER span using OTel messaging semconv (messaging.system=kafka).
  final kafkaProduceId = await tracedKafkaProduce<String>(
    topic: 'orders',
    messageKey: 'orders/2026/inv-1042.pdf',
    payloadSize: 512,
    clientId: 'client_8f3a2c',
    invoke: () async {
      // Make the Kafka call here, then report what it returned.
      const id = 'kafka_8f3a2c';
      return const KafkaProduceResult(value: id, partition: 1, offset: 1);
    },
  );
  OTelLog.info('tracedKafkaProduce returned $kafkaProduceId');

  // Wraps the user's handling of one Kafka record in a CONSUMER span.
  final kafkaConsumeId = await tracedKafkaConsume<String>(
    topic: 'orders',
    consumerGroup: 'example',
    partition: 1,
    offset: 1,
    messageKey: 'orders/2026/inv-1042.pdf',
    invoke: () async {
      // Make the Kafka call here, then report what it returned.
      return 'kafka_8f3a2c';
    },
  );
  OTelLog.info('tracedKafkaConsume returned $kafkaConsumeId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutKafkaInstrumentationAsync.
  await runWithoutKafkaInstrumentationAsync(
    () => tracedKafkaProduce<String>(
      topic: 'orders',
      messageKey: 'orders/2026/inv-1042.pdf',
      payloadSize: 512,
      clientId: 'client_8f3a2c',
      invoke: () async {
        const id = 'kafka_8f3a2c';
        return const KafkaProduceResult(value: id, partition: 1, offset: 1);
      },
    ),
  );

  await OTel.shutdown();
}
