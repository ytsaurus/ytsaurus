#pragma once

#include "artifact.h"
#include "artifact_cache.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IVolumeArtifact)

struct IVolumeArtifact
    : public TRefCounted
{
    virtual const std::string& GetFileName() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IVolumeArtifactCache)

struct IVolumeArtifactCache
    : public TRefCounted
{
    virtual TFuture<IVolumeArtifactPtr> DownloadArtifact(
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IVolumeArtifactCachePtr CreateVolumeArtifactCacheAdapter(TArtifactCachePtr artifactCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
