#pragma once

#include "private.h"
#include "volume.h"
#include "volume_counters.h"

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLayerLocation)
DECLARE_REFCOUNTED_CLASS(TLayer)

struct TFetchedArtifactKey;

////////////////////////////////////////////////////////////////////////////////

class TTmpfsLayerCache
    : public TRefCounted
{
public:
    using TAbsorbLayerCallback = TCallback<TFuture<TLayerPtr>(
        const TArtifactKey& artifactKey,
        const TArtifactDownloadOptions& downloadOptions,
        TGuid tag,
        TLayerLocationPtr targetLocation)>;

    TTmpfsLayerCache(
        IBootstrap* bootstrap,
        NDataNode::TTmpfsLayerCacheConfigPtr config,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        IInvokerPtr controlInvoker,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        const TString& cacheName,
        NContainers::IPortoExecutorPtr portoExecutor,
        TAbsorbLayerCallback absorbLayer);

    TLayerPtr FindLayer(const TArtifactKey& artifactKey);

    TFuture<void> Initialize();

    TFuture<void> Disable(const TError& error, bool persistentDisable = false);

    void BuildOrchid(NYTree::TFluentMap fluent) const;

    const TLayerLocationPtr& GetLocation() const;

private:
    const NDataNode::TTmpfsLayerCacheConfigPtr Config_;
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const IInvokerPtr ControlInvoker_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;
    const TString CacheName_;
    IBootstrap* const Bootstrap_;
    NContainers::IPortoExecutorPtr PortoExecutor_;
    TAbsorbLayerCallback AbsorbLayer_;

    TLayerLocationPtr TmpfsLocation_;

    THashMap<NYPath::TYPath, TFetchedArtifactKey> CachedLayerDescriptors_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, DataSpinLock_);
    THashMap<TArtifactKey, TLayerPtr> CachedLayers_;
    NConcurrency::TPeriodicExecutorPtr LayerUpdateExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, AlertSpinLock_);
    TError Alert_;

    TPromise<void> Initialized_ = NewPromise<void>();

    const NProfiling::TProfiler Profiler_;

    NProfiling::TCounter HitCounter_;
    NProfiling::TGauge UpdateFailedCounter_;

    TTmpfsLayerCacheCounters TmpfsLayerCacheCounters_;

    void PopulateTmpfsAlert(std::vector<TError>* errors);

    void SetAlert(const TError& error);

    void UpdateLayers();
};

DECLARE_REFCOUNTED_CLASS(TTmpfsLayerCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
