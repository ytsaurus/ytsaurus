#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

struct TNodeDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
    TDuration SyncPeriod;

    //! TTL for GetClusterMeta request.
    TDuration ExpireAfterSuccessfulUpdateTime;
    TDuration ExpireAfterFailedUpdateTime;

    std::optional<int> CacheStickyGroupSize;

    REGISTER_YSON_STRUCT(TNodeDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
