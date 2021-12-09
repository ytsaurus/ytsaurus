#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTableMountCacheConfig
    : public TAsyncExpiringCacheConfig
{
public:
    //! If entry is requested for the first time then allow only client who requested the entry to wait for it.
    bool RejectIfEntryIsRequestedButNotReady;

    TTableMountCacheConfig();

    TTableMountCacheConfigPtr ApplyDynamic(const TTableMountCacheDynamicConfigPtr& dynamicConfig);
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableMountCacheDynamicConfig
    : public TAsyncExpiringCacheDynamicConfig
{
public:
    std::optional<bool> RejectIfEntryIsRequestedButNotReady;

    TTableMountCacheDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteDynamicStoreReaderConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration ClientReadTimeout;
    TDuration ServerReadTimeout;
    TDuration ClientWriteTimeout;
    TDuration ServerWriteTimeout;
    i64 MaxRowsPerServerRead;

    ssize_t WindowSize;

    // Testing option.
    double StreamingSubrequestFailureProbability;

    TRemoteDynamicStoreReaderConfig();
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

    TRetryingRemoteDynamicStoreReaderConfig();
};

DEFINE_REFCOUNTED_TYPE(TRetryingRemoteDynamicStoreReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
