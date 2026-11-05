#pragma once

#include "artifact.h"
#include "artifact_cache.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IVolumeArtifact
    : public TRefCounted
{
    virtual const std::string& GetFileName() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IVolumeArtifact)

////////////////////////////////////////////////////////////////////////////////

struct IVolumeArtifactCache
    : public TRefCounted
{
    virtual TFuture<IVolumeArtifactPtr> DownloadArtifact(
        const TArtifactKey& key,
        const TArtifactDownloadContext& artifactDownloadContext,
        TNonNullPtr<ECacheEventType> cacheEventType) = 0;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IVolumeArtifactCache)

////////////////////////////////////////////////////////////////////////////////

IVolumeArtifactCachePtr CreateVolumeArtifactCacheAdapter(TArtifactCachePtr artifactCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
