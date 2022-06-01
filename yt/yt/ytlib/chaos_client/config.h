#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TChaosCellDirectorySynchronizerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Interval between consequent directory updates.
    TDuration SyncPeriod;

    //! Splay for directory updates period.
    TDuration SyncPeriodSplay;

    REGISTER_YSON_STRUCT(TChaosCellDirectorySynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosCellDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
