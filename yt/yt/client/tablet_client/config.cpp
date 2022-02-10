#include "config.h"

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

void TTableMountCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reject_if_entry_is_requested_but_not_ready", &TThis::RejectIfEntryIsRequestedButNotReady)
        .Default(false);
}

TTableMountCacheConfigPtr TTableMountCacheConfig::ApplyDynamic(
    const TTableMountCacheDynamicConfigPtr& dynamicConfig)
{
    auto config = New<TTableMountCacheConfig>();

    config->ApplyDynamicInplace(dynamicConfig);

    config->RejectIfEntryIsRequestedButNotReady = dynamicConfig->RejectIfEntryIsRequestedButNotReady.value_or(
        RejectIfEntryIsRequestedButNotReady);

    config->Postprocess();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TTableMountCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reject_if_entry_is_requested_but_not_ready", &TThis::RejectIfEntryIsRequestedButNotReady)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TRemoteDynamicStoreReaderConfig::TRemoteDynamicStoreReaderConfig()
{
    RegisterParameter("client_read_timeout", ClientReadTimeout)
        .Default(TDuration::Seconds(20));
    RegisterParameter("server_read_timeout", ServerReadTimeout)
        .Default(TDuration::Seconds(20));
    RegisterParameter("client_write_timeout", ClientWriteTimeout)
        .Default(TDuration::Seconds(20));
    RegisterParameter("server_write_timeout", ServerWriteTimeout)
        .Default(TDuration::Seconds(20));
    RegisterParameter("max_rows_per_server_read", MaxRowsPerServerRead)
        .GreaterThan(0)
        .Default(1024);

    RegisterParameter("window_size", WindowSize)
        .Default(16_MB)
        .GreaterThan(0);

    RegisterParameter("streaming_subrequest_failure_probability", StreamingSubrequestFailureProbability)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

TRetryingRemoteDynamicStoreReaderConfig::TRetryingRemoteDynamicStoreReaderConfig()
{
    RegisterParameter("retry_count", RetryCount)
        .Default(10)
        .GreaterThan(0);
    RegisterParameter("locate_request_backoff_time", LocateRequestBackoffTime)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
