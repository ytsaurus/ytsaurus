#pragma once

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TNodeDirectorySynchronizerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration SyncPeriod;

    //! TTL for GetClusterMeta request.
    TDuration ExpireAfterSuccessfulUpdateTime;
    TDuration ExpireAfterFailedUpdateTime;

    TNodeDirectorySynchronizerConfig();
};

DEFINE_REFCOUNTED_TYPE(TNodeDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
