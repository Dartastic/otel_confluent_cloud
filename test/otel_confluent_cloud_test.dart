// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:otel_confluent_cloud/otel_confluent_cloud.dart';
import 'package:test/test.dart';

void main() {
  group('otel_confluent_cloud', () {
    test('suppression toggles in zone', () async {
      expect(confluentCloudInstrumentationSuppressed(), isFalse);
      await runWithoutConfluentCloudInstrumentationAsync(() async {
        expect(confluentCloudInstrumentationSuppressed(), isTrue);
      });
      expect(confluentCloudInstrumentationSuppressed(), isFalse);
    });

    test('ConfluentCloudResult fields default null', () {
      const r = ConfluentCloudResult();
      expect(r.statusCode, isNull);
    });
  });
}
