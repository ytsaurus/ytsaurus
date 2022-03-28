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
    auto mergedConfig = CloneYsonSerializable(MakeStrong(this));

    mergedConfig->ApplyDynamicInplace(dynamicConfig);

    mergedConfig->RejectIfEntryIsRequestedButNotReady = dynamicConfig->RejectIfEntryIsRequestedButNotReady.value_or(
        RejectIfEntryIsRequestedButNotReady);

    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TTableMountCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reject_if_entry_is_requested_but_not_ready", &TThis::RejectIfEntryIsRequestedButNotReady)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TRemoteDynamicStoreReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("client_read_timeout", &TThis::ClientReadTimeout)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("server_read_timeout", &TThis::ServerReadTimeout)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("client_write_timeout", &TThis::ClientWriteTimeout)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("server_write_timeout", &TThis::ServerWriteTimeout)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("max_rows_per_server_read", &TThis::MaxRowsPerServerRead)
        .GreaterThan(0)
        .Default(1024);

    registrar.Parameter("window_size", &TThis::WindowSize)
        .Default(16_MB)
        .GreaterThan(0);

    registrar.Parameter("streaming_subrequest_failure_probability", &TThis::StreamingSubrequestFailureProbability)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

void TRetryingRemoteDynamicStoreReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("retry_count", &TThis::RetryCount)
        .Default(10)
        .GreaterThan(0);
    registrar.Parameter("locate_request_backoff_time", &TThis::LocateRequestBackoffTime)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
