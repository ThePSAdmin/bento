# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bento is a high-performance, resilient stream processor written in Go that connects various data sources and sinks while performing transformations, enrichments, filtering, and hydration on message payloads. It's declarative, cloud-native, and can be deployed as a static binary, Docker image, or serverless function.

## Essential Commands

### Building
```bash
make                    # Build bento binary to ./target/bin/bento
make install           # Build and install to $GOPATH/bin
make serverless        # Build Lambda function (outputs .zip)
make docker            # Build Docker image
make TAGS=x_bento_extra  # Build with extra components (zmq4, etc. requiring C libs)
```

### Testing
```bash
make test                           # Run all tests (3m timeout)
make test-race                      # Run tests with race detector
go test -run TestSpecificTest ./... # Run specific test
go test -run 'TestIntegration/kafka' ./...  # Run specific integration tests

# After building, test config files
./target/bin/bento test ./config/test/...
./target/bin/bento template lint ./config/template_examples/*.yaml
```

### Linting and Formatting
```bash
make lint              # Run golangci-lint with auto-fix
make fmt               # Format code with gofmt and goimports
go vet ./...          # Run go vet
```

### Development Tools
```bash
make tools             # Build bento_docs_gen tool
make docs              # Generate documentation from code
make playground        # Build Bloblang WASM playground
```

## Architecture Overview

### Core Stream Pipeline

Bento's architecture follows a transaction-based message flow pattern:

```
Input → Buffer (optional) → Pipeline (processors) → Output
```

**Key File:** `internal/stream/type.go` - The `Type` struct orchestrates all four layers.

### Transaction-Based Semantics

Every message flows through the system wrapped in a `message.Transaction` containing:
- Message payload (immutable after creation)
- Acknowledgment callback function
- Enables at-least-once delivery guarantees

**Transaction Coordinator:** `internal/transaction/`

The system guarantees message delivery without disk-persisted state using in-process transaction acknowledgments. When a message is processed successfully through the entire pipeline, acknowledgments propagate back to the source.

### Plugin Architecture

All major components (inputs, outputs, processors, buffers, caches, rate limits) use a registration pattern. Components register themselves during package initialization:

```go
// Pattern used throughout codebase
service.RegisterProcessor(
    "processor_name",
    configSpec,           // Configuration specification
    constructorFunc,      // Factory function
)
```

**Global Environment:** `public/service/environment.go:710` - Central registry for all component types.

**Component Implementations:** `internal/impl/` - Contains 60+ directories organized by provider (aws, gcp, kafka, etc.) and `pure` for built-in components.

### Component Interfaces

**Input Components** (`internal/component/input/`)
- `Streamed`: Primary interface returning transaction channels
- `Async`: Single message async reads
- Handle connection management, retry logic, backoff

**Processor Components** (`internal/component/processor/`)
- `V1`: Core processor interface
- Transforms: 1 message → N messages or drop
- Bloblang mapping is implemented as a processor

**Output Components** (`internal/component/output/`)
- Write messages to external systems
- Support batching and retry logic
- Return errors to trigger transaction nacks

**Buffer Components** (`internal/component/buffer/`)
- Optional intermediate storage between input and pipeline
- Types: Memory, Window-based

### Configuration System

**Main Config File:** `public/service/config.go:744`

Bento uses YAML-based declarative configuration with:
- Environment variable interpolation
- Schema-driven validation via field specifications
- Auto-generated documentation from specs

**Standard Config Structure:**
```yaml
input:                  # Input source(s)
  type: kafka
  kafka: {...}

pipeline:
  processors: []       # Ordered transformations

output:                # Output sink(s)
  type: s3
  s3: {...}

buffer: {}            # Optional buffering (memory/system)

resources:            # Reusable components (caches, rate limits)
  caches:
    my_cache: {...}

metrics: {}           # Metrics configuration
tracer: {}            # Tracing configuration
http: {}              # HTTP API settings (default :4195)
```

**Programmatic API:** `public/service/stream_builder.go:1117` - The `StreamBuilder` type allows building streams programmatically instead of YAML (useful for embedding Bento in applications).

### Bloblang Mapping Language

**Implementation:** `internal/bloblang/`

Bento includes a custom DSL for transformations:
- Compiled to execution trees for performance
- Functions: `.length()`, `.filter()`, `.map()`, `.json()`, etc.
- Available in processors and metadata contexts

**Usage Pattern:**
```yaml
pipeline:
  processors:
    - mapping: |
        root.message = this
        root.timestamp = now()
        root.filtered = this.items.filter(item -> item.active)
```

## Directory Structure

### Public API (for plugin development)
- `public/service/` - Main service API for building plugins
- `public/components/` - All component implementations organized by category
- `public/components/all/` - Import this to load all components
- `public/bloblang/` - Bloblang language API

### Internal Implementation
- `internal/impl/pure/` - Built-in components (37KB of processor implementations)
- `internal/impl/{provider}/` - Provider-specific implementations (aws, gcp, kafka, etc.)
- `internal/component/` - Component interfaces (input, output, processor, buffer, cache, etc.)
- `internal/manager/` - Component lifecycle management and resource injection
- `internal/message/` - Message model and batch handling
- `internal/pipeline/` - Pipeline orchestration
- `internal/stream/` - Stream lifecycle management
- `internal/cli/` - CLI command implementations
- `internal/bundle/` - Component bundling and registration

### Entry Points
- `cmd/bento/main.go` - Main CLI entry (calls `service.RunCLI()`)
- `cmd/serverless/bento-lambda/` - AWS Lambda handler
- `cmd/tools/` - Development tools (docs generator, playground)

## Key Patterns and Conventions

### Component Construction Pattern

Every component constructor follows this signature pattern:

```go
func constructorFunc(conf *ParsedConfig, mgr *Resources) (Component, error)
```

This enables:
- Validated configuration access via `conf`
- Dependency injection (logger, metrics, tracer, caches, rate limits) via `mgr`
- Consistent error handling

### Graceful Shutdown

**Location:** `internal/stream/type.go` (Stop, StopGracefully, StopUnordered methods)

Three-phase shutdown pattern:
1. **Graceful**: Input stops consuming, components drain message queues
2. **Ordered**: Components close in dependency order
3. **Unordered/Forced**: All components close simultaneously

Use `github.com/Jeffail/shutdown` package for coordinating goroutine shutdowns.

### Message Immutability

Messages are treated as immutable after creation. Processors that modify messages create copies using `.Copy()` or `.ShallowCopy()` methods. This enables safe concurrent access and avoids defensive copying.

### Broker Patterns

**Fan-out/Fan-in:** `internal/impl/pure/output_broker.go` and `input_broker.go`

Multiple inputs and outputs can be composed with:
- Round-robin distribution
- Greedy (first-available) routing
- Fallback (try-next-on-failure) logic
- Dynamic routing via `switch` and `match` conditions

### Testing Components

Use test utilities in `internal/component/testutil/`:
- Mock resources and managers
- Test message generation
- Integration test helpers

**Pattern for testing processors:**
```go
// See examples in internal/impl/pure/*_test.go files
proc, err := newProcessor(conf, mgr)
msgs, res := proc.ProcessBatch(ctx, batch)
```

## Component Registration

All components must register during package `init()`:

**Processors:** `service.RegisterProcessor(name, spec, constructor)`
**Inputs:** `service.RegisterInput(name, spec, constructor)`
**Outputs:** `service.RegisterOutput(name, spec, constructor)`
**Buffers:** `service.RegisterBuffer(name, spec, constructor)`
**Caches:** `service.RegisterCache(name, spec, constructor)`
**Rate Limits:** `service.RegisterRateLimit(name, spec, constructor)`

**Example:** See `internal/impl/pure/processor_*.go` files for registration patterns.

To create a custom Bento distribution with your own plugins, see `resources/plugin_example/` for a complete example module.

## Configuration Interpolation

Bento supports environment variable interpolation in configs:

```yaml
input:
  kafka:
    addresses: [ "${KAFKA_BROKER:localhost:9092}" ]
    topics: [ "${TOPIC}" ]
```

**Syntax:**
- `${VAR}` - Required variable (error if missing)
- `${VAR:default}` - Optional with default value
- `${file:/path/to/file}` - Read from file

## Build Tags

**`x_bento_extra`**: Includes components requiring C libraries (zmq4, etc.)

```bash
make TAGS=x_bento_extra
go build -tags "x_bento_extra" ./cmd/bento
```

If these dependencies aren't present, builds fail with linker errors like `ld: library not found for -lzmq`.

## Health and Observability

### HTTP Endpoints (default :4195)
- `/ping` - Liveness probe (always 200)
- `/ready` - Readiness probe (200 when input and output connected, 503 otherwise)
- `/version` - Build version info
- `/metrics` - Prometheus metrics (if configured)

### Metrics

**Config:** `metrics` section in config (statsd, prometheus, json, cloudwatch, etc.)

**Emitted Metrics:**
- Input/output message counts and errors
- Processor execution times
- Buffer utilization
- Connection states

### Tracing

**Config:** `tracer` section in config (jaeger, datadog, etc.)

Bento emits OpenTelemetry tracing events showing message flow through processors.

## Working with Configs

### Linting Configs
```bash
./target/bin/bento lint ./config.yaml
./target/bin/bento lint --deprecated ./config.yaml  # Check for deprecated fields
```

### Testing Configs
Bento supports unit tests for configs in `_bento_test.yaml` files:

```yaml
# my_pipeline_test.yaml
tests:
  - name: "test message transformation"
    target_processors: '/pipeline/processors'
    input_batch:
      - content: '{"foo":"bar"}'
    output_batches:
      - - json_equals: {"foo":"bar","added":"field"}
```

Run with: `bento test ./config/...`

## Common Integration Patterns

### Message Brokers
Kafka, NATS, RabbitMQ, Redis Streams are commonly used. Each has provider-specific implementations in `internal/impl/{provider}/`.

### Cloud Storage
S3, GCS, Azure Blob Storage inputs/outputs support:
- Directory scanning
- Event-driven processing (SQS notifications for S3)
- Automatic compression/decompression

### Databases
SQL, MongoDB, DynamoDB, Cassandra, Elasticsearch outputs support:
- Batching for performance
- Interpolation for dynamic operations
- Processors for query building

### HTTP
- `http_server` input: Exposes HTTP endpoint for receiving messages
- `http_client` input: Polls HTTP endpoints
- `http_client` output: POSTs messages to HTTP endpoints
- Supports websockets, SSE, custom headers, auth

## Documentation Generation

Documentation is generated from component registration specs:

```bash
make tools                           # Build docs generator
./target/bin/tools/bento_docs_gen    # Generate markdown docs
```

Component specs define:
- Field descriptions and types
- Examples
- Categories (input, output, processor, etc.)
- Version and stability information

These specs power both runtime validation and documentation generation.
