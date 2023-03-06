#pragma once

#include "public.h"

#include "chunk_cache.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IVolumeArtifact
    : public TRefCounted
{
    virtual TString GetFileName() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeArtifact)

////////////////////////////////////////////////////////////////////////////////

class IVolumeChunkCache
    : public TRefCounted
{
public:
    virtual TFuture<IVolumeArtifactPtr> DownloadArtifact(
        const NDataNode::TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeChunkCache)

////////////////////////////////////////////////////////////////////////////////

IVolumeChunkCachePtr CreateVolumeChunkCacheAdapter(TChunkCachePtr chunkCache);

////////////////////////////////////////////////////////////////////////////////

struct IVolume
    : public virtual TRefCounted
{
    virtual const TString& GetPath() const = 0;
    virtual TFuture<void> Remove() = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolume)

////////////////////////////////////////////////////////////////////////////////

//! Creates volumes from different layers.
//! Useful for creation of rootfs volumes.
struct IVolumeManager
    : public virtual TRefCounted
{
    virtual TFuture<IVolumePtr> PrepareVolume(
        const std::vector<NDataNode::TArtifactKey>& layers,
        const TArtifactDownloadOptions& downloadOptions,
        const TUserSandboxOptions& options) = 0;

    virtual bool IsLayerCached(const NDataNode::TArtifactKey& artifactKey) const = 0;

    virtual void BuildOrchidYson(NYTree::TFluentMap fluent) const = 0;

    virtual void ClearCaches() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeManager)

//! Porto volume manager initialization invokes a number of async operations
//! including population of on-disk and tmpfs layer cache.
TFuture<IVolumeManagerPtr> CreatePortoVolumeManager(
    NDataNode::TDataNodeConfigPtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfig,
    IVolumeChunkCachePtr chunkCache,
    IInvokerPtr controlInvoker,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
