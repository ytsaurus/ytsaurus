#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TChaosCellDirectorySynchronizerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    //! Splay for directory updates period.
    TDuration SyncPeriodSplay;

    TChaosCellDirectorySynchronizerConfig();
};

DEFINE_REFCOUNTED_TYPE(TChaosCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
