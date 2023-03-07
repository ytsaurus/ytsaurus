#pragma once

#include "public.h"
#include "artifact.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/client/api/public.h>

#include <yt/client/hydra/public.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/logging/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TFetchedArtifactKey
{
    NHydra::TRevision ContentRevision;
    std::optional<TArtifactKey> ArtifactKey;
};

TFetchedArtifactKey FetchLayerArtifactKeyIfRevisionChanged(
    const NYPath::TYPath& path,
    NHydra::TRevision contentRevision,
    NClusterNode::TBootstrap const* bootstrap,
    NApi::EMasterChannelKind masterChannelKind,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode