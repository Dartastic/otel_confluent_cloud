// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry_api/dartastic_opentelemetry_api.dart';

import 'otel_confluent_cloud_suppression.dart';

class ConfluentCloudResult {
  /// Creates a result.
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

/// What a traced call returned: the value for the caller plus fields
/// recorded on the span.
class ConfluentCloudCallResult<T> {
  /// Creates a call result.
  const ConfluentCloudCallResult({required this.value, this.result});

  /// The value returned to the caller.
  final T value;
  final ConfluentCloudResult? result;
}

Future<T> tracedConfluentCloud<T>({
  required String operation,
  required Future<ConfluentCloudCallResult<T>> Function() invoke,
  APITracer? tracer,
}) async {
  if (confluentCloudInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final t =
      tracer ?? OTelAPI.tracerProvider().getTracer('otel_confluent_cloud');
  final span = t.startSpan(
    'confluentcloud $operation',
    kind: SpanKind.producer,
  );
  try {
    span.addAttributes(OTelAPI.attributes(
        [OTelAPI.attributeString('messaging.system', 'kafka')]));
    final r = await invoke();
    final res = r.result;
    if (res != null) {
      if (res.environment != null) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeString('confluent.environment', res.environment!)
        ]));
      }
      if (res.clusterId != null) {
        span.addAttributes(OTelAPI.attributes(
            [OTelAPI.attributeString('confluent.cluster.id', res.clusterId!)]));
      }
      if (res.clusterName != null) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeString('confluent.cluster.name', res.clusterName!)
        ]));
      }
      if (res.topic != null) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeString('messaging.destination.name', res.topic!)
        ]));
      }
      if (res.partition != null) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeInt(
            'messaging.kafka.destination.partition',
            res.partition!,
          )
        ]));
      }
      if (res.offset != null) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeInt('messaging.kafka.message.offset', res.offset!)
        ]));
      }
      if (res.consumerGroup != null) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeString(
            'messaging.kafka.consumer.group',
            res.consumerGroup!,
          )
        ]));
      }
      if (res.schemaRegistry != null) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeString(
            'confluent.schema_registry',
            res.schemaRegistry!,
          )
        ]));
      }
      if (res.region != null) {
        span.addAttributes(OTelAPI.attributes(
            [OTelAPI.attributeString('cloud.region', res.region!)]));
      }
      if (res.statusCode != null) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeInt('http.response.status_code', res.statusCode!)
        ]));
      }
      if (res.errorCode != null) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeString('confluentcloud.error.code', res.errorCode!)
        ]));
      }
      final fail = res.errorCode != null ||
          (res.statusCode != null && res.statusCode! >= 400);
      if (fail) {
        span.addAttributes(OTelAPI.attributes([
          OTelAPI.attributeString(
            ErrorAttributes.errorType.key,
            res.errorCode ?? 'confluent.error',
          )
        ]));
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
