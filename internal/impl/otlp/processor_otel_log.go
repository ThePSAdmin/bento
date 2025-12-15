package otlp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

func otelLogProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Integration", "Observability").
		Version("1.0.0").
		Summary("Sends log records to an OpenTelemetry collector.").
		Description(`
This processor sends log records to an OpenTelemetry collector using the OTLP protocol.
All fields support Bloblang interpolation, allowing dynamic values based on message content.

The processor follows the [OpenTelemetry Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/) specification.
`).
		Field(service.NewObjectListField("http",
			service.NewStringField("address").
				Description("The endpoint of a collector to send log events to.").
				Example("localhost:4318"),
			service.NewBoolField("secure").
				Description("Connect to the collector over HTTPS.").
				Default(false),
		).Description("A list of HTTP collectors.").Default([]any{})).
		Field(service.NewObjectListField("grpc",
			service.NewStringField("address").
				Description("The endpoint of a collector to send log events to.").
				Example("localhost:4317"),
			service.NewBoolField("secure").
				Description("Connect to the collector with client transport security.").
				Default(false),
		).Description("A list of gRPC collectors.").Default([]any{})).
		Field(service.NewInterpolatedStringField("body").
			Description("The body/message of the log record. This field is required and supports interpolation.").
			Example("${! json() }").
			Example("${! content() }").
			Example("User ${! json(\"user.name\") } performed action ${! json(\"action\") }")).
		Field(service.NewInterpolatedStringField("timestamp").
			Description("The timestamp when the event occurred. Supports RFC3339 format or Unix timestamp (seconds or nanoseconds). If not set, the current time is used.").
			Example("${! json(\"event_time\") }").
			Example("${! metadata(\"timestamp\") }").
			Default("").
			Optional()).
		Field(service.NewInterpolatedStringField("observed_timestamp").
			Description("The timestamp when the log record was observed. If not set, the current time is used.").
			Default("").
			Optional()).
		Field(service.NewInterpolatedStringField("severity").
			Description("The severity level of the log record. Supported values: TRACE, DEBUG, INFO, WARN, ERROR, FATAL.").
			Example("${! json(\"level\") }").
			Example("INFO").
			Default("").
			Optional()).
		Field(service.NewBloblangField("attributes_mapping").
			Description("A Bloblang mapping that produces a flat object of key-value pairs to attach as attributes to the log record.").
			Example(`root.service = "my-service"
root.env = meta("env")
root.user_id = this.user.id`).
			Optional()).
		Field(service.NewInterpolatedStringField("trace_id").
			Description("A trace ID to associate with this log record for distributed tracing correlation. Should be a 32-character hex string.").
			Example("${! metadata(\"traceparent\").split(\"-\").index(1) }").
			Default("").
			Optional()).
		Field(service.NewInterpolatedStringField("span_id").
			Description("A span ID to associate with this log record. Should be a 16-character hex string.").
			Example("${! metadata(\"traceparent\").split(\"-\").index(2) }").
			Default("").
			Optional()).
		Field(service.NewDurationField("flush_timeout").
			Description("The maximum time to wait for pending log records to be exported when closing the processor.").
			Default("5s").
			Advanced())
}

type otelLogProcessor struct {
	logger *service.Logger

	// OTEL components
	provider *sdklog.LoggerProvider
	otelLog  log.Logger

	// Interpolated fields
	body       *service.InterpolatedString
	timestamp  *service.InterpolatedString
	observedTs *service.InterpolatedString
	severity   *service.InterpolatedString
	traceID    *service.InterpolatedString
	spanID     *service.InterpolatedString

	// Bloblang mapping for attributes
	attributesMapping *bloblang.Executor

	flushTimeout time.Duration
}

func init() {
	err := service.RegisterProcessor(
		"open_telemetry_log",
		otelLogProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newOtelLogProcessor(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

func newOtelLogProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*otelLogProcessor, error) {
	// Parse HTTP collectors
	httpCollectors, err := parseLogCollectors(conf, "http")
	if err != nil {
		return nil, err
	}

	// Parse gRPC collectors
	grpcCollectors, err := parseLogCollectors(conf, "grpc")
	if err != nil {
		return nil, err
	}

	if len(httpCollectors) == 0 && len(grpcCollectors) == 0 {
		return nil, errors.New("at least one http or grpc collector must be configured")
	}

	// Create log provider with exporters
	provider, err := createLogProvider(httpCollectors, grpcCollectors)
	if err != nil {
		return nil, err
	}

	// Parse required body field
	body, err := conf.FieldInterpolatedString("body")
	if err != nil {
		return nil, err
	}

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
	if err != nil {
		return nil, err
	}

	return &otelLogProcessor{
		logger:            mgr.Logger(),
		provider:          provider,
		otelLog:           provider.Logger("bento"),
		body:              body,
		timestamp:         timestamp,
		observedTs:        observedTs,
		severity:          severity,
		attributesMapping: attributesMapping,
		traceID:           traceID,
		spanID:            spanID,
		flushTimeout:      flushTimeout,
	}, nil
}

func (p *otelLogProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Resolve body (required)
	bodyStr, err := p.body.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve body: %w", err)
	}

	// Build log record
	var record log.Record
	record.SetBody(log.StringValue(bodyStr))

	// Handle timestamp
	now := time.Now()
	if p.timestamp != nil {
		tsStr, err := p.timestamp.TryString(msg)
		if err != nil {
			p.logger.Warnf("Failed to resolve timestamp: %v", err)
		} else if tsStr != "" {
			if ts, err := parseTimestamp(tsStr); err != nil {
				p.logger.Warnf("Failed to parse timestamp '%s': %v", tsStr, err)
			} else {
				record.SetTimestamp(ts)
			}
		}
	}

	// Handle observed timestamp (default to now)
	observedTs := now
	if p.observedTs != nil {
		otsStr, err := p.observedTs.TryString(msg)
		if err != nil {
			p.logger.Warnf("Failed to resolve observed_timestamp: %v", err)
		} else if otsStr != "" {
			if ots, err := parseTimestamp(otsStr); err != nil {
				p.logger.Warnf("Failed to parse observed_timestamp '%s': %v", otsStr, err)
			} else {
				observedTs = ots
			}
		}
	}
	record.SetObservedTimestamp(observedTs)

	// Handle severity
	if p.severity != nil {
		sevStr, err := p.severity.TryString(msg)
		if err != nil {
			p.logger.Warnf("Failed to resolve severity: %v", err)
		} else if sevStr != "" && sevStr != "null" {
			sev := parseSeverity(sevStr)
			record.SetSeverity(sev)
			record.SetSeverityText(sevStr)
		}
	}

	// Handle attributes mapping
	if p.attributesMapping != nil {
		attrs, err := p.resolveAttributes(msg)
		if err != nil {
			p.logger.Warnf("Failed to resolve attributes: %v", err)
		} else if len(attrs) > 0 {
			record.AddAttributes(attrs...)
		}
	}

	// Handle trace context (trace_id and span_id)
	// The OTEL SDK extracts trace context from the context, so we inject a SpanContext
	emitCtx := ctx
	var traceID trace.TraceID
	var spanID trace.SpanID
	var hasTraceContext bool

	if p.traceID != nil {
		tidStr, err := p.traceID.TryString(msg)
		if err != nil {
			p.logger.Warnf("Failed to resolve trace_id: %v", err)
		} else if tidStr != "" {
			if tid, err := parseTraceID(tidStr); err != nil {
				p.logger.Warnf("Failed to parse trace_id '%s': %v", tidStr, err)
			} else {
				traceID = tid
				hasTraceContext = true
			}
		}
	}

	if p.spanID != nil {
		sidStr, err := p.spanID.TryString(msg)
		if err != nil {
			p.logger.Warnf("Failed to resolve span_id: %v", err)
		} else if sidStr != "" {
			if sid, err := parseSpanID(sidStr); err != nil {
				p.logger.Warnf("Failed to parse span_id '%s': %v", sidStr, err)
			} else {
				spanID = sid
				hasTraceContext = true
			}
		}
	}

	// If we have trace context, inject it into the context
	if hasTraceContext {
		spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
			Remote:     true,
		})
		emitCtx = trace.ContextWithSpanContext(ctx, spanCtx)
	}

	// Emit the log record
	p.otelLog.Emit(emitCtx, record)

	// Pass message through unchanged
	return service.MessageBatch{msg}, nil
}

func (p *otelLogProcessor) Close(ctx context.Context) error {
	// Create a context with timeout for flushing
	shutdownCtx, cancel := context.WithTimeout(ctx, p.flushTimeout)
	defer cancel()

	if err := p.provider.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("failed to shutdown log provider: %w", err)
	}
	return nil
}

func (p *otelLogProcessor) resolveAttributes(msg *service.Message) ([]log.KeyValue, error) {
	result, err := msg.BloblangQuery(p.attributesMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to execute attributes mapping: %w", err)
	}

	if result == nil {
		return nil, nil
	}

	raw, err := result.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to get structured data from attributes: %w", err)
	}

	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("attributes mapping must produce an object, got: %T", raw)
	}

	attrs := make([]log.KeyValue, 0, len(obj))
	for k, v := range obj {
		// Skip null values
		if v == nil {
			continue
		}
		attrs = append(attrs, toLogKeyValue(k, v))
	}

	return attrs, nil
}

// toLogKeyValue converts a key-value pair to an OTEL log.KeyValue
func toLogKeyValue(key string, value any) log.KeyValue {
	switch v := value.(type) {
	case string:
		return log.String(key, v)
	case int:
		return log.Int(key, v)
	case int64:
		return log.Int64(key, v)
	case float64:
		return log.Float64(key, v)
	case bool:
		return log.Bool(key, v)
	case []byte:
		return log.Bytes(key, v)
	default:
		// For complex types, convert to string
		return log.String(key, fmt.Sprintf("%v", v))
	}
}

// parseLogCollectors parses collector configuration for the specified type (http/grpc)
func parseLogCollectors(conf *service.ParsedConfig, name string) ([]collector, error) {
	list, err := conf.FieldObjectList(name)
	if err != nil {
		return nil, err
	}
	collectors := make([]collector, 0, len(list))
	for _, pc := range list {
		address, err := pc.FieldString("address")
		if err != nil {
			return nil, err
		}
		if address == "" {
			return nil, errors.New("collector address must not be empty")
		}

		secure, err := pc.FieldBool("secure")
		if err != nil {
			return nil, err
		}

		collectors = append(collectors, collector{
			address: address,
			secure:  secure,
		})
	}
	return collectors, nil
}

// createLogProvider creates a LoggerProvider with the specified collectors
func createLogProvider(httpCollectors, grpcCollectors []collector) (*sdklog.LoggerProvider, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	var opts []sdklog.LoggerProviderOption

	// Add gRPC exporters
	for _, c := range grpcCollectors {
		clientOpts := []otlploggrpc.Option{
			otlploggrpc.WithEndpoint(c.address),
		}
		if !c.secure {
			clientOpts = append(clientOpts, otlploggrpc.WithInsecure())
		}

		exp, err := otlploggrpc.New(ctx, clientOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC log exporter for %s: %w", c.address, err)
		}
		opts = append(opts, sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
	}

	// Add HTTP exporters
	for _, c := range httpCollectors {
		clientOpts := []otlploghttp.Option{
			otlploghttp.WithEndpoint(c.address),
		}
		if !c.secure {
			clientOpts = append(clientOpts, otlploghttp.WithInsecure())
		}

		exp, err := otlploghttp.New(ctx, clientOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create HTTP log exporter for %s: %w", c.address, err)
		}
		opts = append(opts, sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
	}

	return sdklog.NewLoggerProvider(opts...), nil
}

// parseSeverity converts a severity string to an OTEL log.Severity
func parseSeverity(s string) log.Severity {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "TRACE":
		return log.SeverityTrace
	case "DEBUG":
		return log.SeverityDebug
	case "INFO":
		return log.SeverityInfo
	case "WARN", "WARNING":
		return log.SeverityWarn
	case "ERROR":
		return log.SeverityError
	case "FATAL":
		return log.SeverityFatal
	default:
		return log.SeverityInfo
	}
}

// parseTimestamp parses a timestamp string in RFC3339 or Unix format
func parseTimestamp(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty timestamp")
	}

	// Try RFC3339 first
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}

	// Try Unix timestamp (seconds)
	if secs, err := strconv.ParseInt(s, 10, 64); err == nil {
		// Heuristic: if the number is very large, it's likely nanoseconds
		if secs > 1e12 {
			return time.Unix(0, secs), nil
		}
		return time.Unix(secs, 0), nil
	}

	// Try Unix timestamp (float seconds)
	if secsFloat, err := strconv.ParseFloat(s, 64); err == nil {
		secs := int64(secsFloat)
		nsecs := int64((secsFloat - float64(secs)) * 1e9)
		return time.Unix(secs, nsecs), nil
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", s)
}

// parseTraceID parses a 32-character hex string into a trace ID
func parseTraceID(s string) (trace.TraceID, error) {
	s = strings.TrimSpace(s)
	if len(s) != 32 {
		return trace.TraceID{}, fmt.Errorf("trace ID must be 32 hex characters, got %d", len(s))
	}

	bytes, err := hex.DecodeString(s)
	if err != nil {
		return trace.TraceID{}, fmt.Errorf("invalid hex in trace ID: %w", err)
	}

	var tid trace.TraceID
	copy(tid[:], bytes)
	return tid, nil
}

// parseSpanID parses a 16-character hex string into a span ID
func parseSpanID(s string) (trace.SpanID, error) {
	s = strings.TrimSpace(s)
	if len(s) != 16 {
		return trace.SpanID{}, fmt.Errorf("span ID must be 16 hex characters, got %d", len(s))
	}

	bytes, err := hex.DecodeString(s)
	if err != nil {
		return trace.SpanID{}, fmt.Errorf("invalid hex in span ID: %w", err)
	}

	var sid trace.SpanID
	copy(sid[:], bytes)
	return sid, nil
}
