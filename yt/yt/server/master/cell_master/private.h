#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqResetDynamicallyPropagatedMasterCells;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAutomatonThreadBucket,
    (Gossips)
    (ChunkMaintenance)
    (Transactions)
);

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, CellMasterLogger, "Master");
YT_DEFINE_LEAKY_GLOBAL(const NProfiling::TProfiler, CellMasterProfiler, "/master");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
