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

    registrar.Parameter("table_schema_cache", &TThis::TableSchemaCache)
        .DefaultNew();
    registrar.Parameter("yson_table_schema_cache", &TThis::YsonTableSchemaCache)
        .DefaultNew();

    registrar.Parameter("cache_heavy_schema_on_creation", &TThis::CacheHeavySchemaOnCreation)
        .Default(false)
        .DontSerializeDefault();

    registrar.Preprocessor([] (TThis* config) {
        config->TableSchemaCache->ExpirationPeriod = TDuration::Seconds(10);
        config->TableSchemaCache->RefreshTime = std::nullopt;
        config->TableSchemaCache->ShardCount = 256;
        config->TableSchemaCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(5);
        config->TableSchemaCache->ExpireAfterFailedUpdateTime = TDuration::Minutes(5);
        config->TableSchemaCache->ExpireAfterAccessTime = TDuration::Minutes(5);

        config->YsonTableSchemaCache->ExpirationPeriod = TDuration::Seconds(10);
        config->YsonTableSchemaCache->RefreshTime = std::nullopt;
        config->YsonTableSchemaCache->ShardCount = 256;
        config->YsonTableSchemaCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(5);
        config->YsonTableSchemaCache->ExpireAfterFailedUpdateTime = TDuration::Minutes(5);
        config->YsonTableSchemaCache->ExpireAfterAccessTime = TDuration::Minutes(5);
    });
}

void TTableManagerConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
