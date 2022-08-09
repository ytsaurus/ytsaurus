#include "config.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

void TSecurityManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("user_cache", &TThis::UserCache)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->UserCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(60);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TStructuredLoggingTopicDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);

    registrar.Parameter("suppressed_methods", &TThis::SuppressedMethods)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TApiServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("client_cache", &TThis::ClientCache)
        .DefaultNew();
    registrar.Parameter("security_manager", &TThis::SecurityManager)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->ClientCache->Capacity = DefaultClientCacheCapacity;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TApiServiceDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("client_cache", &TThis::ClientCache)
        .DefaultNew();
    registrar.Parameter("verbose_logging", &TThis::VerboseLogging)
        .Default(false);
    registrar.Parameter("enable_modify_rows_request_reordering", &TThis::EnableModifyRowsRequestReordering)
        .Default(true);
    registrar.Parameter("force_tracing", &TThis::ForceTracing)
        .Default(false);
    registrar.Parameter("read_buffer_row_count", &TThis::ReadBufferRowCount)
        .Default(10000);
    registrar.Parameter("read_buffer_data_weight", &TThis::ReadBufferDataWeight)
        .Default(16_MB);
    registrar.Parameter("security_manager", &TThis::SecurityManager)
        .DefaultNew();
    registrar.Parameter("structured_logging_main_topic", &TThis::StructuredLoggingMainTopic)
        .DefaultNew();
    registrar.Parameter("structured_logging_error_topic", &TThis::StructuredLoggingErrorTopic)
        .DefaultNew();
    registrar.Parameter("structured_logging_max_request_byte_size", &TThis::StructuredLoggingMaxRequestByteSize)
        .Default(10_KB);
    registrar.Parameter("formats", &TThis::Formats)
        .Default();

    registrar.Preprocessor([] (TThis* config) {
        config->StructuredLoggingMainTopic->SuppressedMethods = THashSet<TString>{"ModifyRows", "BatchModifyRows", "LookupRows", "VersionedLookupRows"};
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
