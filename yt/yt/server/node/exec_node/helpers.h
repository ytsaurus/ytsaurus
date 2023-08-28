#pragma once

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/data_node/artifact.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>

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
    const NScheduler::NProto::NNode::TControllerAgentDescriptor& proto,
    const NNodeTrackerClient::TNetworkPreferenceList& localNetworks);

////////////////////////////////////////////////////////////////////////////////

void SetNodeInfoToRequest(
    IBootstrap const* bootstrap,
    const auto& request)
{
    request->set_node_id(NYT::ToProto<ui32>(bootstrap->GetNodeId()));
    ToProto(request->mutable_node_descriptor(), bootstrap->GetLocalDescriptor());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

template <>
struct THash<NYT::NExecNode::TControllerAgentDescriptor>
{
    size_t operator () (const NYT::NExecNode::TControllerAgentDescriptor& descriptor) const;
};
