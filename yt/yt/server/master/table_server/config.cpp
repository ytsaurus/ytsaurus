#include "config.h"

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

void TYsonTableSchemaCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cache_table_schema_after_convertion_to_yson", &TThis::CacheTableSchemaAfterConvertionToYson)
        .Default(false);
}

void TDynamicTableManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_schema_memory_usage_to_log", &TThis::MaxSchemaMemoryUsageToLog)
        .GreaterThan(0)
        .Default(8_KB);

    registrar.Parameter("make_schema_attribute_opaque", &TThis::MakeSchemaAttributeOpaque)
        .Default(true);
    registrar.Parameter("non_opaque_schema_attribute_user_whitelist", &TThis::NonOpaqueSchemaAttributeUserWhitelist)
        .Default();

    registrar.Parameter("table_schema_cache", &TThis::TableSchemaCache)
        .DefaultNew();
    registrar.Parameter("yson_table_schema_cache", &TThis::YsonTableSchemaCache)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->TableSchemaCache->RefreshTime = TDuration::Seconds(15);
        config->TableSchemaCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(5);
        config->TableSchemaCache->ExpireAfterFailedUpdateTime = TDuration::Minutes(5);

        config->YsonTableSchemaCache->RefreshTime = TDuration::Seconds(15);
        config->YsonTableSchemaCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(5);
        config->YsonTableSchemaCache->ExpireAfterFailedUpdateTime = TDuration::Minutes(5);
    });
}

void TTableManagerConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
