#pragma once

#include "public.h"

#include <yt/core/misc/config.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTableMountCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
    //! If entry is requested for the first time then allow only client who requested the entry to wait for it.
    bool RejectIfEntryIsRequestedButNotReady;


    TTableMountCacheConfig()
    {
        RegisterParameter("reject_if_entry_is_requested_but_not_ready", RejectIfEntryIsRequestedButNotReady)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteDynamicStoreReaderConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration ClientReadTimeout;
    TDuration ServerReadTimeout;
    TDuration ClientWriteTimeout;
    TDuration ServerWriteTimeout;

    ssize_t WindowSize;

    TRemoteDynamicStoreReaderConfig()
    {
        RegisterParameter("client_read_timeout", ClientReadTimeout)
            .Default(TDuration::Seconds(20));
        RegisterParameter("server_read_timeout", ServerReadTimeout)
            .Default(TDuration::Seconds(20));
        RegisterParameter("client_write_timeout", ClientWriteTimeout)
            .Default(TDuration::Seconds(20));
        RegisterParameter("server_write_timeout", ServerWriteTimeout)
            .Default(TDuration::Seconds(20));

        RegisterParameter("window_size", WindowSize)
            .Default(16_MB)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteDynamicStoreReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TRetryingRemoteDynamicStoreReaderConfig
    : public TRemoteDynamicStoreReaderConfig
{
public:
    //! Maximum number of locate requests.
    int RetryCount;

    //! Time to wait between making another locate request.
    TDuration LocateRequestBackoffTime;

    TRetryingRemoteDynamicStoreReaderConfig()
    {
        RegisterParameter("retry_count", RetryCount)
            .Default(10)
            .GreaterThan(0);
        RegisterParameter("locate_request_backoff_time", LocateRequestBackoffTime)
            .Default(TDuration::Seconds(10));
    }
};

DEFINE_REFCOUNTED_TYPE(TRetryingRemoteDynamicStoreReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
