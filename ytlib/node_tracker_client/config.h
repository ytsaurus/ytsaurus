#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

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

    TNodeDirectorySynchronizerConfig()
    {
        RegisterParameter("sync_period", SyncPeriod)
            .Default(TDuration::Seconds(15));

        RegisterParameter("expire_after_successful_update_time", ExpireAfterSuccessfulUpdateTime)
            .Alias("success_expiration_time")
            .Default(TDuration::Seconds(15));
        RegisterParameter("expire_after_failed_update_time", ExpireAfterFailedUpdateTime)
            .Alias("failure_expiration_time")
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
