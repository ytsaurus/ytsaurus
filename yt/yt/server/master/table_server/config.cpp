#include "config.h"

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_schema_memory_usage_to_log", &TThis::MaxSchemaMemoryUsageToLog)
        .GreaterThan(0)
        .Default(8_KB)
        .DontSerializeDefault();

    registrar.Parameter("make_schema_attribute_opaque", &TThis::MakeSchemaAttributeOpaque)
        .Default(true);
    registrar.Parameter("non_opaque_schema_attribute_user_whitelist", &TThis::NonOpaqueSchemaAttributeUserWhitelist)
        .Default()
        .DontSerializeDefault();

    registrar.Parameter("compact_table_cache_expiration_timeout", &TThis::CompactTableCacheExpirationTimeout)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Minutes(5));
}

void TTableManagerConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
