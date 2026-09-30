#pragma once

#include "uuid.h"

#include <string>

namespace clickhouse::open_telemetry {

/// See https://www.w3.org/TR/trace-context/ for trace_flags definition
enum TraceFlags : uint8_t {
    TRACE_FLAG_NONE    = 0,
    TRACE_FLAG_SAMPLED = 1,
};

/// The runtime info we need to create new OpenTelemetry spans.
struct TracingContext {
    UUID trace_id{};
    uint64_t span_id = 0;
    std::string tracestate;
    uint8_t trace_flags = TRACE_FLAG_NONE;
};

}  // namespace clickhouse::open_telemetry
