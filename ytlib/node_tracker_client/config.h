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

    TNodeDirectorySynchronizerConfig()
    {
        RegisterParameter("sync_period", SyncPeriod)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeDirectorySynchronizerConfig)


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
