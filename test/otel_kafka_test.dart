// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_kafka/otel_kafka.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_kafka-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('produce records PRODUCER span with partition + offset', () async {
    await tracedKafkaProduce<String>(
      topic: 'orders',
      messageKey: 'user-1',
      payloadSize: 512,
      clientId: 'app-1',
      invoke: () async => const KafkaProduceResult(
        value: 'ok',
        partition: 3,
        offset: 999,
      ),
    );
    final a = attrsOf(spans.findSpanByName('publish orders')!);
    expect(a['messaging.system'], 'kafka');
    expect(a['messaging.destination.name'], 'orders');
    expect(a['messaging.kafka.message.key'], 'user-1');
    expect(a['messaging.kafka.destination.partition'], 3);
    expect(a['messaging.kafka.message.offset'], 999);
  });

  test('consume records CONSUMER span', () async {
    await tracedKafkaConsume<String>(
      topic: 'orders',
      consumerGroup: 'worker-1',
      partition: 3,
      offset: 999,
      invoke: () async => 'processed',
    );
    final a = attrsOf(spans.findSpanByName('receive orders')!);
    expect(a['messaging.kafka.consumer.group'], 'worker-1');
    expect(a['messaging.kafka.destination.partition'], 3);
    expect(a['messaging.kafka.message.offset'], 999);
  });

  test('suppression skips spans', () async {
    await runWithoutKafkaInstrumentationAsync(() async {
      await tracedKafkaProduce<String>(
        topic: 't',
        invoke: () async => const KafkaProduceResult(value: 'v'),
      );
    });
    expect(spans.findSpansStartingWith('publish '), isEmpty);
  });
}
