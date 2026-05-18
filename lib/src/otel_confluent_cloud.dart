// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry_api/dartastic_opentelemetry_api.dart';

import 'otel_confluent_cloud_suppression.dart';

class ConfluentCloudResult {
  const ConfluentCloudResult({
    this.environment,
    this.clusterId,
    this.clusterName,
    this.topic,
    this.partition,
    this.offset,
    this.consumerGroup,
    this.schemaRegistry,
    this.region,
    this.statusCode,
    this.errorCode,
    this.errorMessage,
  });

  final String? environment;
  final String? clusterId;
  final String? clusterName;
  final String? topic;
  final int? partition;
  final int? offset;
  final String? consumerGroup;
  final String? schemaRegistry;
  final String? region;
  final int? statusCode;
  final String? errorCode;
  final String? errorMessage;
}

class ConfluentCloudCallResult<T> {
  const ConfluentCloudCallResult({required this.value, this.result});
  final T value;
  final ConfluentCloudResult? result;
}

Future<T> tracedConfluentCloud<T>({
  required String operation,
  required Future<ConfluentCloudCallResult<T>> Function() invoke,
  Tracer? tracer,
}) async {
  if (confluentCloudInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final t = tracer ?? OTel.tracerProvider().getTracer('otel_confluent_cloud');
  final span = t.startSpan(
    'confluentcloud $operation',
    kind: SpanKind.producer,
  );
  try {
    span.setStringAttribute('messaging.system', 'kafka');
    final r = await invoke();
    final res = r.result;
    if (res != null) {
      if (res.environment != null) {
        span.setStringAttribute('confluent.environment', res.environment!);
      }
      if (res.clusterId != null) {
        span.setStringAttribute('confluent.cluster.id', res.clusterId!);
      }
      if (res.clusterName != null) {
        span.setStringAttribute('confluent.cluster.name', res.clusterName!);
      }
      if (res.topic != null) {
        span.setStringAttribute('messaging.destination.name', res.topic!);
      }
      if (res.partition != null) {
        span.setIntAttribute(
          'messaging.kafka.destination.partition',
          res.partition!,
        );
      }
      if (res.offset != null) {
        span.setIntAttribute('messaging.kafka.message.offset', res.offset!);
      }
      if (res.consumerGroup != null) {
        span.setStringAttribute(
          'messaging.kafka.consumer.group',
          res.consumerGroup!,
        );
      }
      if (res.schemaRegistry != null) {
        span.setStringAttribute(
          'confluent.schema_registry',
          res.schemaRegistry!,
        );
      }
      if (res.region != null) {
        span.setStringAttribute('cloud.region', res.region!);
      }
      if (res.statusCode != null) {
        span.setIntAttribute('http.response.status_code', res.statusCode!);
      }
      if (res.errorCode != null) {
        span.setStringAttribute('confluentcloud.error.code', res.errorCode!);
      }
      final fail =
          res.errorCode != null ||
          (res.statusCode != null && res.statusCode! >= 400);
      if (fail) {
        span.setStringAttribute(
          ErrorResource.errorType.key,
          res.errorCode ?? 'confluent.error',
        );
        span.setStatus(SpanStatusCode.Error, res.errorMessage);
      }
    }
    return r.value;
  } catch (e, st) {
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
