#pragma once

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/data_node/artifact.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_descriptor.pb.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

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

struct TControllerAgentDescriptor
{
    TString Address;
    NScheduler::TIncarnationId IncarnationId;

    bool operator==(const TControllerAgentDescriptor& other) const noexcept;
    bool operator!=(const TControllerAgentDescriptor& other) const noexcept;

    explicit operator bool() const noexcept;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TControllerAgentDescriptor& controllerAgentDescriptor,
    TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TControllerAgentDescriptor> TryParseControllerAgentDescriptor(
    const NControllerAgent::NProto::TControllerAgentDescriptor& proto,
    const NNodeTrackerClient::TNetworkPreferenceList& localNetworks);

////////////////////////////////////////////////////////////////////////////////

void SetNodeInfoToRequest(
    NNodeTrackerClient::TNodeId nodeId,
    const NNodeTrackerClient::TNodeDescriptor& nodeDescriptor,
    const auto& request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

template <>
struct THash<NYT::NExecNode::TControllerAgentDescriptor>
{
    size_t operator () (const NYT::NExecNode::TControllerAgentDescriptor& descriptor) const;
};

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
