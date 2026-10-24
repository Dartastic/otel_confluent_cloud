// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_confluent_cloud/otel_confluent_cloud.dart';

/// Traces Confluent Cloud calls with OpenTelemetry.
///
/// `otel_confluent_cloud` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Confluent Cloud call in
/// [tracedConfluentCloud]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'confluent-cloud-example');

  // Traces a Confluent Cloud call.
  final confluentCloudId = await tracedConfluentCloud<String>(
    operation: 'create',
    invoke: () async {
      // Make the Confluent Cloud call here, then report what it returned.
      const id = 'confluent_cloud_8f3a2c';
      return const ConfluentCloudCallResult(
          value: id,
          result: ConfluentCloudResult(
              environment: 'example',
              clusterId: 'cluster_8f3a2c',
              clusterName: 'example',
              topic: 'orders',
              partition: 1,
              offset: 1,
              consumerGroup: 'example',
              schemaRegistry: 'example',
              region: 'us-east-1',
              statusCode: 201,
              errorCode: 'example',
              errorMessage: 'example'));
    },
  );
  OTelLog.info('tracedConfluentCloud create returned $confluentCloudId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutConfluentCloudInstrumentationAsync.
  await runWithoutConfluentCloudInstrumentationAsync(
    () => tracedConfluentCloud<String>(
      operation: 'create',
      invoke: () async {
        const id = 'confluent_cloud_8f3a2c';
        return const ConfluentCloudCallResult(
            value: id,
            result: ConfluentCloudResult(
                environment: 'example',
                clusterId: 'cluster_8f3a2c',
                clusterName: 'example',
                topic: 'orders',
                partition: 1,
                offset: 1,
                consumerGroup: 'example',
                schemaRegistry: 'example',
                region: 'us-east-1',
                statusCode: 201,
                errorCode: 'example',
                errorMessage: 'example'));
      },
    ),
  );

  await OTel.shutdown();
}
