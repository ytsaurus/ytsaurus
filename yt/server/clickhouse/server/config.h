#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/ytlib/api/native/config.h>

#include <yt/core/concurrency/config.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TNativeClientCacheConfig
    : public virtual NYTree::TYsonSerializable
    , public TAsyncExpiringCacheConfig
{
public:
    TNativeClientCacheConfig() = default;
};

DEFINE_REFCOUNTED_TYPE(TNativeClientCacheConfig);

////////////////////////////////////////////////////////////////////////////////

class TConfig
    : public TServerConfig
{
public:
    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    TNativeClientCacheConfigPtr ClientCache;

    // Controls incoming bandwidth used by scan jobs
    NConcurrency::TThroughputThrottlerConfigPtr ScanThrottler;

    TConfig();
};

DEFINE_REFCOUNTED_TYPE(TConfig);

}   // namespace NClickHouse
}   // namespace NYT
