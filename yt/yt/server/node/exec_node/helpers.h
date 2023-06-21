#pragma once

#include "private.h"

#include <yt/yt/server/node/data_node/artifact.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TFetchedArtifactKey
{
    NHydra::TRevision ContentRevision;
    std::optional<NDataNode::TArtifactKey> ArtifactKey;
};

TFetchedArtifactKey FetchLayerArtifactKeyIfRevisionChanged(
    const NYPath::TYPath& path,
    NHydra::TRevision contentRevision,
    IBootstrap const* bootstrap,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TControllerAgentDescriptor> TryParseControllerAgentDescriptor(
    const NScheduler::NProto::NNode::TControllerAgentDescriptor& proto,
    const NNodeTrackerClient::TNetworkPreferenceList& localNetworks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
