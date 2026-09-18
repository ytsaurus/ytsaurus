#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqRemoveNode;

using TReqRegisterNode = NNodeTrackerClient::NProto::TReqRegisterNode;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(INodeDisposalManager)

DECLARE_REFCOUNTED_CLASS(TNodeDiscoveryManager)

using TNodeList = TCompactVector<TNode*, NChunkClient::TypicalReplicaCount>;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, NodeTrackerServerStructuredLogger, "NodeTrackerServerStructured");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, NodeTrackerServerLogger, "NodeTrackerServer");
YT_DEFINE_LEAKY_GLOBAL(const NProfiling::TProfiler, NodeTrackerProfiler, "/node_tracker");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
