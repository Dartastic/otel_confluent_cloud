# otel_confluent_cloud

Dartastic OpenTelemetry bridge for Confluent Cloud (managed Kafka).

Wraps each call in a CLIENT-kind span with vendor-specific attributes following OpenTelemetry semantic conventions where applicable.

```dart
import 'package:otel_confluent_cloud/otel_confluent_cloud.dart';

await tracedConfluentCloud<void>(
  operation: 'someOp',
  invoke: () async {
    final r = await client.someOp();
    return ConfluentCloudCallResult(value: r, result: ConfluentCloudResult(statusCode: 200));
  },
);
```

Suppress instrumentation within a block:

```dart
await runWithoutConfluentCloudInstrumentationAsync(() async {
  await client.someInternalSync();
});
```
