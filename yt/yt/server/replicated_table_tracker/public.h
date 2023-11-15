#pragma once

#include <yt/yt/ytlib/replicated_table_tracker_client/public.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

using TGetStateUpdatesRsp = TIntrusivePtr<NRpc::TTypedClientResponse<
    NReplicatedTableTrackerClient::NProto::TRspGetTrackerStateUpdates>>;

using TApplyChangeModeCommandsRsp = TIntrusivePtr<NRpc::TTypedClientResponse<
    NReplicatedTableTrackerClient::NProto::TRspApplyChangeReplicaModeCommands>>;

using TComputeLagTimesRsp = TIntrusivePtr<NRpc::TTypedClientResponse<
    NReplicatedTableTrackerClient::NProto::TRspComputeReplicaLagTimes>>;

struct IBootstrap;

DECLARE_REFCOUNTED_CLASS(TReplicatedTableTrackerServerConfig)

DECLARE_REFCOUNTED_CLASS(TReplicatedTableTrackerHost)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
