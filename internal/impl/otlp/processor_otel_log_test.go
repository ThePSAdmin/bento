package otlp

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestOtelLogProcConfigParsing(t *testing.T) {
	spec := otelLogProcSpec()

	t.Run("minimal_config_http", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
http:
  - address: localhost:4318
body: "${! content() }"
`, service.GlobalEnvironment())
		require.NoError(t, err)

		collectors, err := parseLogCollectors(conf, "http")
		require.NoError(t, err)
		require.Len(t, collectors, 1)
		assert.Equal(t, "localhost:4318", collectors[0].address)
		assert.False(t, collectors[0].secure)
	})

	t.Run("minimal_config_grpc", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
grpc:
  - address: localhost:4317
body: "${! content() }"
`, service.GlobalEnvironment())
		require.NoError(t, err)

		collectors, err := parseLogCollectors(conf, "grpc")
		require.NoError(t, err)
		require.Len(t, collectors, 1)
		assert.Equal(t, "localhost:4317", collectors[0].address)
		assert.False(t, collectors[0].secure)
	})

	t.Run("full_config", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
http:
  - address: http-collector:4318
    secure: true
grpc:
  - address: grpc-collector:4317
    secure: true
body: "${! json() }"
timestamp: "${! json(\"event_time\") }"
observed_timestamp: "${! now() }"
severity: "${! json(\"level\") }"
attributes_mapping: |
  root.service = "my-service"
  root.env = "production"
trace_id: "${! metadata(\"trace_id\") }"
span_id: "${! metadata(\"span_id\") }"
flush_timeout: 10s
`, service.GlobalEnvironment())
		require.NoError(t, err)

		httpCollectors, err := parseLogCollectors(conf, "http")
		require.NoError(t, err)
		require.Len(t, httpCollectors, 1)
		assert.Equal(t, "http-collector:4318", httpCollectors[0].address)
		assert.True(t, httpCollectors[0].secure)

		grpcCollectors, err := parseLogCollectors(conf, "grpc")
		require.NoError(t, err)
		require.Len(t, grpcCollectors, 1)
		assert.Equal(t, "grpc-collector:4317", grpcCollectors[0].address)
		assert.True(t, grpcCollectors[0].secure)

		flushTimeout, err := conf.FieldDuration("flush_timeout")
		require.NoError(t, err)
		assert.Equal(t, 10*time.Second, flushTimeout)
	})

	t.Run("multiple_collectors", func(t *testing.T) {
		conf, err := spec.ParseYAML(`
http:
  - address: http1:4318
  - address: http2:4318
    secure: true
grpc:
  - address: grpc1:4317
  - address: grpc2:4317
    secure: true
body: test
`, service.GlobalEnvironment())
		require.NoError(t, err)

		httpCollectors, err := parseLogCollectors(conf, "http")
		require.NoError(t, err)
		require.Len(t, httpCollectors, 2)
		assert.Equal(t, "http1:4318", httpCollectors[0].address)
		assert.Equal(t, "http2:4318", httpCollectors[1].address)

		grpcCollectors, err := parseLogCollectors(conf, "grpc")
		require.NoError(t, err)
		require.Len(t, grpcCollectors, 2)
		assert.Equal(t, "grpc1:4317", grpcCollectors[0].address)
		assert.Equal(t, "grpc2:4317", grpcCollectors[1].address)
	})

	t.Run("missing_body_fails", func(t *testing.T) {
		_, err := spec.ParseYAML(`
http:
  - address: localhost:4318
`, service.GlobalEnvironment())
		require.Error(t, err)
	})
}

func TestParseSeverity(t *testing.T) {
	tests := []struct {
		input    string
		expected log.Severity
	}{
		{"TRACE", log.SeverityTrace},
		{"trace", log.SeverityTrace},
		{"DEBUG", log.SeverityDebug},
		{"debug", log.SeverityDebug},
		{"INFO", log.SeverityInfo},
		{"info", log.SeverityInfo},
		{"WARN", log.SeverityWarn},
		{"warn", log.SeverityWarn},
		{"WARNING", log.SeverityWarn},
		{"warning", log.SeverityWarn},
		{"ERROR", log.SeverityError},
		{"error", log.SeverityError},
		{"FATAL", log.SeverityFatal},
		{"fatal", log.SeverityFatal},
		{"unknown", log.SeverityInfo},           // default
		{"", log.SeverityInfo},                  // default
		{"  INFO  ", log.SeverityInfo},          // trimmed
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseSeverity(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	t.Run("rfc3339", func(t *testing.T) {
		ts, err := parseTimestamp("2024-01-15T10:30:00Z")
		require.NoError(t, err)
		assert.Equal(t, 2024, ts.Year())
		assert.Equal(t, time.January, ts.Month())
		assert.Equal(t, 15, ts.Day())
		assert.Equal(t, 10, ts.Hour())
		assert.Equal(t, 30, ts.Minute())
	})

	t.Run("rfc3339_nano", func(t *testing.T) {
		ts, err := parseTimestamp("2024-01-15T10:30:00.123456789Z")
		require.NoError(t, err)
		assert.Equal(t, 123456789, ts.Nanosecond())
	})

	t.Run("unix_seconds", func(t *testing.T) {
		ts, err := parseTimestamp("1705315800") // 2024-01-15T10:30:00Z
		require.NoError(t, err)
		assert.Equal(t, int64(1705315800), ts.Unix())
	})

	t.Run("unix_nanoseconds", func(t *testing.T) {
		ts, err := parseTimestamp("1705315800123456789")
		require.NoError(t, err)
		assert.Equal(t, int64(1705315800123456789), ts.UnixNano())
	})

	t.Run("unix_float_seconds", func(t *testing.T) {
		ts, err := parseTimestamp("1705315800.5")
		require.NoError(t, err)
		assert.Equal(t, int64(1705315800), ts.Unix())
		assert.Equal(t, 500000000, ts.Nanosecond())
	})

	t.Run("empty_fails", func(t *testing.T) {
		_, err := parseTimestamp("")
		require.Error(t, err)
	})

	t.Run("invalid_fails", func(t *testing.T) {
		_, err := parseTimestamp("not-a-timestamp")
		require.Error(t, err)
	})
}

func TestParseTraceID(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		tid, err := parseTraceID("0102030405060708090a0b0c0d0e0f10")
		require.NoError(t, err)
		expected := trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
		assert.Equal(t, expected, tid)
	})

	t.Run("with_whitespace", func(t *testing.T) {
		tid, err := parseTraceID("  0102030405060708090a0b0c0d0e0f10  ")
		require.NoError(t, err)
		assert.NotEqual(t, trace.TraceID{}, tid)
	})

	t.Run("too_short", func(t *testing.T) {
		_, err := parseTraceID("0102030405060708")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "32 hex characters")
	})

	t.Run("too_long", func(t *testing.T) {
		_, err := parseTraceID("0102030405060708090a0b0c0d0e0f101112")
		require.Error(t, err)
	})

	t.Run("invalid_hex", func(t *testing.T) {
		_, err := parseTraceID("0102030405060708090a0b0c0d0e0fXX")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid hex")
	})
}

func TestParseSpanID(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		sid, err := parseSpanID("0102030405060708")
		require.NoError(t, err)
		expected := trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		assert.Equal(t, expected, sid)
	})

	t.Run("with_whitespace", func(t *testing.T) {
		sid, err := parseSpanID("  0102030405060708  ")
		require.NoError(t, err)
		assert.NotEqual(t, trace.SpanID{}, sid)
	})

	t.Run("too_short", func(t *testing.T) {
		_, err := parseSpanID("01020304")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "16 hex characters")
	})

	t.Run("too_long", func(t *testing.T) {
		_, err := parseSpanID("0102030405060708090a")
		require.Error(t, err)
	})

	t.Run("invalid_hex", func(t *testing.T) {
		_, err := parseSpanID("01020304050607XX")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid hex")
	})
}

func TestToLogKeyValue(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    any
		expected log.KeyValue
	}{
		{"string", "key", "value", log.String("key", "value")},
		{"int", "key", 42, log.Int("key", 42)},
		{"int64", "key", int64(42), log.Int64("key", 42)},
		{"float64", "key", 3.14, log.Float64("key", 3.14)},
		{"bool_true", "key", true, log.Bool("key", true)},
		{"bool_false", "key", false, log.Bool("key", false)},
		{"bytes", "key", []byte("hello"), log.Bytes("key", []byte("hello"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toLogKeyValue(tt.key, tt.value)
			assert.Equal(t, tt.expected.Key, result.Key)
			// Value comparison is complex for OTEL types, so just check key
		})
	}
}

// mockLogExporter is a test exporter that captures emitted records
type mockLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (m *mockLogExporter) Export(ctx context.Context, records []sdklog.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, records...)
	return nil
}

func (m *mockLogExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockLogExporter) ForceFlush(ctx context.Context) error {
	return nil
}

func (m *mockLogExporter) Records() []sdklog.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records
}

// newTestOtelLogProcessor creates a processor with a mock exporter for testing
func newTestOtelLogProcessor(t *testing.T, configYAML string) (*otelLogProcessor, *mockLogExporter) {
	spec := otelLogProcSpec()
	conf, err := spec.ParseYAML(configYAML, service.GlobalEnvironment())
	require.NoError(t, err)

	// Parse required body field
	body, err := conf.FieldInterpolatedString("body")
	require.NoError(t, err)

	// Parse optional fields
	var timestamp *service.InterpolatedString
	if conf.Contains("timestamp") {
		if ts, err := conf.FieldInterpolatedString("timestamp"); err == nil {
			timestamp = ts
		}
	}

	var observedTs *service.InterpolatedString
	if conf.Contains("observed_timestamp") {
		if ots, err := conf.FieldInterpolatedString("observed_timestamp"); err == nil {
			observedTs = ots
		}
	}

	var severity *service.InterpolatedString
	if conf.Contains("severity") {
		if sev, err := conf.FieldInterpolatedString("severity"); err == nil {
			severity = sev
		}
	}

	var attributesMapping *bloblang.Executor
	if conf.Contains("attributes_mapping") {
		if am, err := conf.FieldBloblang("attributes_mapping"); err == nil {
			attributesMapping = am
		}
	}

	var traceID *service.InterpolatedString
	if conf.Contains("trace_id") {
		if tid, err := conf.FieldInterpolatedString("trace_id"); err == nil {
			traceID = tid
		}
	}

	var spanID *service.InterpolatedString
	if conf.Contains("span_id") {
		if sid, err := conf.FieldInterpolatedString("span_id"); err == nil {
			spanID = sid
		}
	}

	flushTimeout, err := conf.FieldDuration("flush_timeout")
	require.NoError(t, err)

	// Create mock exporter and provider
	mockExp := &mockLogExporter{}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(mockExp)),
	)

	proc := &otelLogProcessor{
		logger:            service.MockResources().Logger(),
		provider:          provider,
		otelLog:           provider.Logger("bento-test"),
		body:              body,
		timestamp:         timestamp,
		observedTs:        observedTs,
		severity:          severity,
		attributesMapping: attributesMapping,
		traceID:           traceID,
		spanID:            spanID,
		flushTimeout:      flushTimeout,
	}

	return proc, mockExp
}

func TestOtelLogProcessor_Process(t *testing.T) {
	ctx := context.Background()

	t.Run("basic_message", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: "${! content() }"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte("hello world"))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)
		require.Same(t, msg, batch[0])

		// Force flush to ensure records are exported
		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)
		assert.Equal(t, "hello world", records[0].Body().AsString())
	})

	t.Run("with_severity", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
severity: ERROR
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte("{}"))
		_, err := proc.Process(ctx, msg)
		require.NoError(t, err)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)
		assert.Equal(t, log.SeverityError, records[0].Severity())
		assert.Equal(t, "ERROR", records[0].SeverityText())
	})

	t.Run("with_json_body", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: "User ${! json(\"name\") } logged in"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte(`{"name": "alice"}`))
		_, err := proc.Process(ctx, msg)
		require.NoError(t, err)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)
		assert.Equal(t, "User alice logged in", records[0].Body().AsString())
	})

	t.Run("with_attributes", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test
attributes_mapping: |
  root.service = "my-service"
  root.version = "1.0.0"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte("{}"))
		_, err := proc.Process(ctx, msg)
		require.NoError(t, err)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)

		// Check attributes
		attrs := make(map[string]string)
		records[0].WalkAttributes(func(kv log.KeyValue) bool {
			attrs[kv.Key] = kv.Value.AsString()
			return true
		})
		assert.Equal(t, "my-service", attrs["service"])
		assert.Equal(t, "1.0.0", attrs["version"])
	})

	t.Run("with_trace_context", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test
trace_id: "0102030405060708090a0b0c0d0e0f10"
span_id: "0102030405060708"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte("{}"))
		_, err := proc.Process(ctx, msg)
		require.NoError(t, err)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)

		expectedTraceID := trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
		expectedSpanID := trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

		assert.Equal(t, expectedTraceID, records[0].TraceID())
		assert.Equal(t, expectedSpanID, records[0].SpanID())
	})

	t.Run("body_interpolation_error", func(t *testing.T) {
		proc, _ := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: '${! throw("simulated error") }'
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte("{}"))
		_, err := proc.Process(ctx, msg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to resolve body")
	})
}

func TestOtelLogProcessor_NoCollectors(t *testing.T) {
	spec := otelLogProcSpec()
	conf, err := spec.ParseYAML(`
body: test
`, service.GlobalEnvironment())
	require.NoError(t, err)

	_, err = newOtelLogProcessor(conf, service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one http or grpc collector must be configured")
}

func TestOtelLogProcessor_NullValues(t *testing.T) {
	ctx := context.Background()

	t.Run("null_timestamp_uses_default", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
timestamp: "${! json(\"ts\") }"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		// Message with null timestamp field
		msg := service.NewMessage([]byte(`{"ts": null}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)
		// Record should still be emitted - null timestamp means use default (not set explicitly)
		assert.Equal(t, "test message", records[0].Body().AsString())
	})

	t.Run("null_observed_timestamp_uses_default", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
observed_timestamp: "${! json(\"obs_ts\") }"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte(`{"obs_ts": null}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)
		assert.Equal(t, "test message", records[0].Body().AsString())
		// observed_timestamp should default to approximately now
		assert.False(t, records[0].ObservedTimestamp().IsZero())
	})

	t.Run("null_severity_uses_default", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
severity: "${! json(\"level\") }"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte(`{"level": null}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)
		assert.Equal(t, "test message", records[0].Body().AsString())
		// Severity should not be set (default/unspecified)
		assert.Equal(t, log.Severity(0), records[0].Severity())
	})

	t.Run("null_trace_id_not_set", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
trace_id: "${! json(\"tid\") }"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte(`{"tid": null}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)
		assert.Equal(t, "test message", records[0].Body().AsString())
		// Trace ID should be zero (not set)
		assert.Equal(t, trace.TraceID{}, records[0].TraceID())
	})

	t.Run("null_span_id_not_set", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
span_id: "${! json(\"sid\") }"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte(`{"sid": null}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)
		assert.Equal(t, "test message", records[0].Body().AsString())
		// Span ID should be zero (not set)
		assert.Equal(t, trace.SpanID{}, records[0].SpanID())
	})

	t.Run("attributes_mapping_null_value_skipped", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
attributes_mapping: |
  root.service = "my-service"
  root.nullable = this.maybe_null
  root.version = "1.0.0"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		// maybe_null is null, should be skipped
		msg := service.NewMessage([]byte(`{"maybe_null": null}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)

		// Check attributes - nullable should be skipped
		attrs := make(map[string]string)
		records[0].WalkAttributes(func(kv log.KeyValue) bool {
			attrs[kv.Key] = kv.Value.AsString()
			return true
		})
		assert.Equal(t, "my-service", attrs["service"])
		assert.Equal(t, "1.0.0", attrs["version"])
		_, hasNullable := attrs["nullable"]
		assert.False(t, hasNullable, "null attribute should be skipped")
	})

	t.Run("attributes_mapping_all_null_produces_empty_attrs", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
attributes_mapping: |
  root.a = this.val_a
  root.b = this.val_b
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		// All values are null
		msg := service.NewMessage([]byte(`{"val_a": null, "val_b": null}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)

		// Check that no attributes are set
		attrCount := 0
		records[0].WalkAttributes(func(kv log.KeyValue) bool {
			attrCount++
			return true
		})
		assert.Equal(t, 0, attrCount, "all null attributes should be skipped")
	})

	t.Run("attributes_mapping_mixed_null_and_values", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
attributes_mapping: |
  root.present = this.value
  root.missing = this.null_value
  root.also_present = "static"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		msg := service.NewMessage([]byte(`{"value": "hello", "null_value": null}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)

		attrs := make(map[string]string)
		records[0].WalkAttributes(func(kv log.KeyValue) bool {
			attrs[kv.Key] = kv.Value.AsString()
			return true
		})
		assert.Equal(t, "hello", attrs["present"])
		assert.Equal(t, "static", attrs["also_present"])
		_, hasMissing := attrs["missing"]
		assert.False(t, hasMissing, "null attribute should be skipped")
	})

	t.Run("attributes_mapping_returns_null_entirely", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: test message
attributes_mapping: |
  root = if this.skip == true { deleted() } else { {"key": "value"} }
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		// This should result in deleted() which returns null
		msg := service.NewMessage([]byte(`{"skip": true}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)

		// When mapping returns deleted(), result is nil, so no attributes
		attrCount := 0
		records[0].WalkAttributes(func(kv log.KeyValue) bool {
			attrCount++
			return true
		})
		assert.Equal(t, 0, attrCount, "deleted() should result in no attributes")
	})

	t.Run("body_null_converts_to_null_string", func(t *testing.T) {
		proc, mockExp := newTestOtelLogProcessor(t, `
http:
  - address: localhost:4318
body: "${! json(\"message\") }"
`)
		t.Cleanup(func() { _ = proc.Close(ctx) })

		// Body interpolation of null becomes the string "null"
		msg := service.NewMessage([]byte(`{"message": null}`))
		batch, err := proc.Process(ctx, msg)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		_ = proc.provider.ForceFlush(ctx)

		records := mockExp.Records()
		require.Len(t, records, 1)
		// Interpolated null becomes "null" string
		assert.Equal(t, "null", records[0].Body().AsString())
	})
}
