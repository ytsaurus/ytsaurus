#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TReplicatedTableData;
struct TReplicaData;
struct TTableCollocationData;

DECLARE_REFCOUNTED_STRUCT(IReplicatedTableTracker)
DECLARE_REFCOUNTED_STRUCT(IReplicatedTableTrackerHost)

DECLARE_REFCOUNTED_CLASS(TDynamicReplicatedTableTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
