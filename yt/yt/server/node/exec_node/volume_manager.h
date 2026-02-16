#pragma once

#include "public.h"
#include "preparation_options.h"
#include "volume_artifact.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

//! This class can create root volume as well as tmpfs volumes.
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

    //! TODO(yuryalekeev): Remove this method after we get rid of rbind volume.
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
