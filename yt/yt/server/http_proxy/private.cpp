#include "private.h"

#include <yt/yt/server/lib/logging/category_registry.h>

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NHttpProxy {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

inline const auto HttpStructuredProxyLoggerSchema = New<TTableSchema>(std::vector{
    TColumnSchema("request_id", ESimpleLogicalValueType::String).SetRequired(true),
    TColumnSchema("command", ESimpleLogicalValueType::String).SetRequired(true),
    TColumnSchema("user", ESimpleLogicalValueType::String).SetRequired(true),
    TColumnSchema("authenticated_from", ESimpleLogicalValueType::String).SetRequired(false),
    TColumnSchema("token_hash", ESimpleLogicalValueType::String).SetRequired(false),
    TColumnSchema("parameters", ESimpleLogicalValueType::Any).SetRequired(true),
    TColumnSchema("correlation_id", ESimpleLogicalValueType::String).SetRequired(false),
    TColumnSchema("trace_id", ESimpleLogicalValueType::String).SetRequired(false),
    TColumnSchema("user_agent", ESimpleLogicalValueType::String).SetRequired(false),
    TColumnSchema("path", ESimpleLogicalValueType::Any).SetRequired(false),
    TColumnSchema("http_path", ESimpleLogicalValueType::String).SetRequired(true),
    TColumnSchema("method", ESimpleLogicalValueType::String).SetRequired(true),
    TColumnSchema("http_code", ESimpleLogicalValueType::Int64).SetRequired(true),
    TColumnSchema("error_code", ESimpleLogicalValueType::Int64).SetRequired(true),
    TColumnSchema("error", ESimpleLogicalValueType::Any).SetRequired(true),
    TColumnSchema("remote_address", ESimpleLogicalValueType::String).SetRequired(true),
    TColumnSchema("l7_request_id", ESimpleLogicalValueType::String).SetRequired(false),
    TColumnSchema("l7_real_ip", ESimpleLogicalValueType::String).SetRequired(false),
    TColumnSchema("duration", ESimpleLogicalValueType::Int64).SetRequired(true),
    TColumnSchema("cpu_time", ESimpleLogicalValueType::Int64).SetRequired(true),
    TColumnSchema("start_time", ESimpleLogicalValueType::String).SetRequired(true),
    TColumnSchema("in_bytes", ESimpleLogicalValueType::Int64).SetRequired(true),
    TColumnSchema("out_bytes", ESimpleLogicalValueType::Int64).SetRequired(true),
});

const auto HttpStructuredProxyLogger = NLogging::CreateSchemafulLogger("HttpStructuredProxy", HttpStructuredProxyLoggerSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
