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

    TTableMountCacheConfigPtr ApplyDynamic(const TTableMountCacheDynamicConfigPtr& dynamicConfig);

    REGISTER_YSON_STRUCT(TTableMountCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableMountCacheDynamicConfig
    : public TAsyncExpiringCacheDynamicConfig
{
public:
    std::optional<bool> RejectIfEntryIsRequestedButNotReady;

    REGISTER_YSON_STRUCT(TTableMountCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteDynamicStoreReaderConfig
    : public virtual NYTree::TYsonStruct
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

    REGISTER_YSON_STRUCT(TRemoteDynamicStoreReaderConfig);

    static void Register(TRegistrar registrar);
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

    REGISTER_YSON_STRUCT(TRetryingRemoteDynamicStoreReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRetryingRemoteDynamicStoreReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
