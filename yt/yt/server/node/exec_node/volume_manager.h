#pragma once

#include "artifact_cache.h"
#include "preparation_options.h"
#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IVolumeArtifact
    : public TRefCounted
{
    virtual const std::string& GetFileName() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeArtifact)

////////////////////////////////////////////////////////////////////////////////

struct IVolumeArtifactCache
    : public TRefCounted
{
    virtual TFuture<IVolumeArtifactPtr> DownloadArtifact(
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeArtifactCache)

////////////////////////////////////////////////////////////////////////////////

IVolumeArtifactCachePtr CreateVolumeArtifactCacheAdapter(TArtifactCachePtr artifactCache);

////////////////////////////////////////////////////////////////////////////////

using TVolumeId = TGuid;

////////////////////////////////////////////////////////////////////////////////

struct IVolume
    : public virtual TRefCounted
{
    //! Get unique volume id.
    virtual const TVolumeId& GetId() const = 0;
    //! Get absolute path to volume mount point.
    virtual const std::string& GetPath() const = 0;
    //! Overlayfs stores its upper/work directories in root volume.
    virtual bool IsRootVolume() const = 0;
    //! Link volume mount point to target.
    virtual TFuture<void> Link(
        TGuid tag,
        const TString& target) = 0;
    //! Remove volume and links where it points to.
    virtual TFuture<void> Remove() = 0;

    virtual bool IsCached() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolume)

////////////////////////////////////////////////////////////////////////////////

//! Creates volumes from different layers.
//! Useful for creation of rootfs volumes.
struct IVolumeManager
    : public virtual TRefCounted
{
    //! Prepare root overlayfs volume.
    virtual TFuture<IVolumePtr> PrepareVolume(
        const std::vector<TArtifactKey>& artifactKeys,
        const TVolumePreparationOptions& options) = 0;

    //! Prepare tmpfs volumes.
    virtual TFuture<std::vector<TTmpfsVolumeResult>> PrepareTmpfsVolumes(
        const std::optional<TString>& sandboxPath,
        const std::vector<TTmpfsVolumeParams>& volumes) = 0;

    virtual TFuture<IVolumePtr> RbindRootVolume(
        const IVolumePtr& volume,
        const TString& slotPath) = 0;

    //! Link tmpfs volumes into destination directory.
    virtual TFuture<void> LinkTmpfsVolumes(
        const TString& destinationDirectory,
        const std::vector<TTmpfsVolumeResult>& volumes) = 0;

    virtual bool IsLayerCached(const TArtifactKey& artifactKey) const = 0;

    virtual void BuildOrchid(NYTree::TFluentAny fluent) const = 0;

    virtual void ClearCaches() const = 0;

    virtual void MarkLayersAsNotRemovable() const = 0;

    virtual TFuture<void> GetVolumeReleaseEvent() = 0;

    virtual TFuture<void> DisableLayerCache(const TError& reason) = 0;

    virtual bool IsEnabled() const = 0;

    virtual void OnDynamicConfigChanged(
        const TVolumeManagerDynamicConfigPtr& oldConfig,
        const TVolumeManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IVolumeManager)

//! Porto volume manager initialization invokes a number of async operations
//! including population of on-disk and tmpfs layer cache.
TFuture<IVolumeManagerPtr> CreatePortoVolumeManager(
    NDataNode::TDataNodeConfigPtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    IVolumeArtifactCachePtr artifactCache,
    IInvokerPtr controlInvoker,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

TFuture<IVolumeManagerPtr> CreateSimpleVolumeManager(
    const std::vector<TSlotLocationConfigPtr>& locations,
    IInvokerPtr invoker,
    bool detachedTmpfsUmount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
