#pragma once

#include <yt/yt/client/scheduler/private.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TNodeYsonList = std::vector<std::pair<NNodeTrackerClient::TNodeId, NYson::TYsonString>>;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, NodeShardLogger, "NodeShard");

static constexpr int CypressNodeLimit = 1'000'000;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnutilizedResourceReason,
    (Unknown)
    (Timeout)
    (Throttling)
    (FinishedJobs)
    (NodeHasWaitingAllocations)
    (AcceptableFragmentation)
    (ExcessiveFragmentation)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
