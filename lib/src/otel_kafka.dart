// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_kafka_suppression.dart';

const _tracerName = 'dartastic_otel_libs_kafka';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

/// Pair returned from `tracedKafkaProduce` callback.
class KafkaProduceResult<T> {
  const KafkaProduceResult({
    required this.value,
    this.partition,
    this.offset,
  });
  final T value;
  final int? partition;
  final int? offset;
}

/// Wraps a Kafka produce in a PRODUCER span using OTel messaging
/// semconv (messaging.system=kafka).
Future<T> tracedKafkaProduce<T>({
  required String topic,
  required Future<KafkaProduceResult<T>> Function() invoke,
  String? messageKey,
  int? payloadSize,
  String? clientId,
}) async {
  if (kafkaInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final m = <String, Object>{
    'messaging.system': 'kafka',
    'messaging.operation': 'publish',
    'messaging.destination.name': topic,
  };
  if (messageKey != null) m['messaging.kafka.message.key'] = messageKey;
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (clientId != null) m['messaging.kafka.client_id'] = clientId;

  final span = _tracer().startSpan(
    'publish $topic',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    final extras = <Attribute<Object>>[];
    if (r.partition != null) {
      extras.add(OTel.attributeInt(
        'messaging.kafka.destination.partition',
        r.partition!,
      ));
    }
    if (r.offset != null) {
      extras.add(OTel.attributeInt(
        'messaging.kafka.message.offset',
        r.offset!,
      ));
    }
    if (extras.isNotEmpty) span.addAttributes(OTel.attributes(extras));
    return r.value;
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps the user's handling of one Kafka record in a CONSUMER span.
Future<T> tracedKafkaConsume<T>({
  required String topic,
  required String consumerGroup,
  required Future<T> Function() invoke,
  int? partition,
  int? offset,
  String? messageKey,
}) async {
  if (kafkaInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'messaging.system': 'kafka',
    'messaging.operation': 'receive',
    'messaging.destination.name': topic,
    'messaging.kafka.consumer.group': consumerGroup,
  };
  if (partition != null) {
    m['messaging.kafka.destination.partition'] = partition;
  }
  if (offset != null) m['messaging.kafka.message.offset'] = offset;
  if (messageKey != null) m['messaging.kafka.message.key'] = messageKey;

  final span = _tracer().startSpan(
    'receive $topic',
    kind: SpanKind.consumer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
