#pragma once

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/core/misc/public.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTabletNode::NProto {

class TOriginatorTablet;

} // namespace NYT::NTableNode::NProto

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TReplicatedTableData;
struct TReplicaData;
struct TTableCollocationData;

using NTabletClient::TTabletId;

DECLARE_REFCOUNTED_STRUCT(IReplicatedTableTracker)
DECLARE_REFCOUNTED_STRUCT(IReplicatedTableTrackerHost)

DECLARE_REFCOUNTED_STRUCT(TDynamicReplicatedTableTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
