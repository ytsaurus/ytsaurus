#pragma once

#include "public.h"
#include "artifact.h"
#include "porto_volume.h"
#include "tmpfs_layer_cache.h"
#include "volume.h"
#include "volume_artifact.h"
#include "volume_options.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TLayerLocationPtr DoPickLocation(
    const std::vector<TLayerLocationPtr>& locations,
    std::function<bool(const TLayerLocationPtr&, const TLayerLocationPtr&)> isBetter);

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
using TAsyncMapValueBase = TAsyncCacheValueBase<TKey, TValue>;

// NB(pogorelov): It is pretty dirty map.
// The cache shard capacity is calculated to be 1,
// so since we have the weight of each element equal to 2,
// we get that the cache does not work as a cache, but works as a ValueMap.
template <class TKey, class TValue>
class TAsyncMapBase
    : public TAsyncSlruCacheBase<TKey, TValue>
{
    using TBase = TAsyncSlruCacheBase<TKey, TValue>;
public:
    TAsyncMapBase(const NProfiling::TProfiler& profiler = {});

private:
    i64 GetWeight(const typename TBase::TValuePtr& /*value*/) const override;
};

////////////////////////////////////////////////////////////////////////////////

//! This class caches volumes.
template <typename TKey>
class TVolumeCacheBase
    : public TAsyncMapBase<TKey, TCachedVolume<TKey>>
{
public:
    TVolumeCacheBase(
        const NProfiling::TProfiler& profiler,
        IBootstrap* const bootstrap,
        std::vector<TLayerLocationPtr> layerLocations);

    bool IsEnabled() const;

protected:
    IBootstrap* const Bootstrap_;
    const std::vector<TLayerLocationPtr> LayerLocations_;

    TLayerLocationPtr PickLocation();

    void OnAdded(const TIntrusivePtr<TCachedVolume<TKey>>& volume) override;

    void OnRemoved(const TIntrusivePtr<TCachedVolume<TKey>>& volume) override;

    void OnWeightUpdated(i64 weightDelta) override;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSquashFSVolumeCache)

//! This class creates squashfs volumes generated from cypress files (layers).
class TSquashFSVolumeCache
    : public TVolumeCacheBase<TArtifactKey>
{
public:
    TSquashFSVolumeCache(
        IBootstrap* const bootstrap,
        std::vector<TLayerLocationPtr> layerLocations,
        IVolumeArtifactCachePtr artifactCache);

    //! Get squashfs volume from cache or download and mount it.
    TFuture<IVolumePtr> GetOrCreateVolume(
        TGuid tag,
        TPrepareSquashFSVolumeOptions options);

private:
    const IVolumeArtifactCachePtr ArtifactCache_;

    TFuture<TSquashFSVolumePtr> DownloadAndPrepareVolume(
        const TArtifactKey& artifactKey,
        const TArtifactDownloadOptions& downloadOptions,
        TGuid tag);

    TSquashFSVolumePtr CreateSquashFSVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        NProfiling::TEventTimerGuard volumeCreateTimeGuard,
        const TArtifactKey& artifactKey,
        IVolumeArtifactPtr artifact);
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNbdVolumeFactory)

//! This class creates NBD volumes.
class TNbdVolumeFactory
    : public TVolumeCacheBase<TString>
{
public:
    using TVolumePtr = TIntrusivePtr<TCachedVolume<TString>>;

    TNbdVolumeFactory(
        IBootstrap* const bootstrap,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        std::vector<TLayerLocationPtr> layerLocations);

    TFuture<IVolumePtr> GetOrCreateVolume(
        TGuid tag,
        TPrepareRONbdVolumeOptions options);

    TFuture<IVolumePtr> GetOrCreateVolume(
        TGuid tag,
        TPrepareRWNbdVolumeOptions options);

private:
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, InsertLock_);

    static void ValidatePrepareRONbdVolumeOptions(const TPrepareRONbdVolumeOptions& options);
    static void ValidatePrepareRWNbdVolumeOptions(const TPrepareRWNbdVolumeOptions& options);

    TInsertCookie GetInsertCookie(const TString& deviceId, const NNbd::INbdServerPtr& nbdServer);

    //! Make callback that subscribes job for NBD device errors.
    TExtendedCallback<TVolumePtr(const TErrorOr<TVolumePtr>&)> MakeJobSubscriberForDeviceErrors(
        TJobId jobId,
        const TString& deviceId,
        const NNbd::INbdServerPtr& nbdServer,
        const NLogging::TLogger& Logger);

    // RO volumes start here.

    NNbd::IImageReaderPtr CreateArtifactReader(
        const NLogging::TLogger& Logger,
        const TArtifactKey& artifactKey);

    TFuture<NNbd::IBlockDevicePtr> CreateRONbdDevice(
        TGuid tag,
        TPrepareRONbdVolumeOptions options);

    TFuture<TRONbdVolumePtr> CreateRONbdVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TCreateNbdVolumeOptions options);

    //! Create RO NBD volume. The order of creation is as follows:
    //! 1. Create RO NBD device.
    //! 2. Register RO NBD device with NBD server.
    //! 3. Create RO NBD porto volume connected to RO NBD device.
    TFuture<TRONbdVolumePtr> PrepareRONbdVolume(
        TGuid tag,
        TPrepareRONbdVolumeOptions options);

    // RW volumes start here.

    TFuture<NNbd::IBlockDevicePtr> CreateRWNbdDevice(
        TGuid tag,
        TPrepareRWNbdVolumeOptions options);

    TFuture<TRWNbdVolumePtr> CreateRWNbdVolume(
        TGuid tag,
        NProfiling::TTagSet tagSet,
        TCreateNbdVolumeOptions options);

    //! Create RW NBD volume. The order of creation is as follows:
    //! 1. Create RW NBD device.
    //! 2. Register RW NBD device with NBD server.
    //! 3. Create RW NBD porto volume connected to RW NBD device.
    TFuture<TRWNbdVolumePtr> PrepareRWNbdVolume(
        TGuid tag,
        TPrepareRWNbdVolumeOptions options);

    TFuture<std::vector<std::string>> FindDataNodesWithMedium(
        const NYT::NChunkClient::TSessionId& sessionId,
        const TPrepareRWNbdVolumeOptions& options);

    //! Open NBD session on data node that can host NBD disk.
    std::optional<std::tuple<NRpc::IChannelPtr, NYT::NChunkClient::TSessionId>> TryOpenNbdSession(
        NYT::NChunkClient::TSessionId sessionId,
        std::vector<std::string> addresses,
        TPrepareRWNbdVolumeOptions options);

    //! Find data node suitable to host NBD disk and open NBD session.
    TFuture<std::optional<std::tuple<NRpc::IChannelPtr, NYT::NChunkClient::TSessionId>>> PrepareNbdSession(
        const TPrepareRWNbdVolumeOptions& options);
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLayerCache)

//! This class caches layers (tar archives) extracted from cypress files.
class TLayerCache
    : public TAsyncSlruCacheBase<TArtifactKey, TLayer>
{
public:
    TLayerCache(
        const NDataNode::TVolumeManagerConfigPtr& config,
        const NClusterNode::TClusterNodeDynamicConfigManagerPtr& dynamicConfigManager,
        std::vector<TLayerLocationPtr> layerLocations,
        NContainers::IPortoExecutorPtr tmpfsExecutor,
        IVolumeArtifactCachePtr artifactCache,
        IInvokerPtr controlInvoker,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        IBootstrap* bootstrap);

    //! Get tar archive (tar layer) from cache or download and extract it.
    TFuture<TLayerPtr> GetOrCreateLayer(
        TGuid tag,
        TPrepareLayerOptions options);

    TFuture<void> Initialize();

    bool IsEnabled() const;

    TLayerLocationPtr PickLocation();

    void PopulateAlerts(std::vector<TError>* alerts);

    TFuture<void> Disable(const TError& reason);

    TFuture<void> GetVolumeReleaseEvent();

    bool IsLayerCached(const TArtifactKey& artifactKey);

    void Touch(const TLayerPtr& layer);

    void BuildOrchid(NYTree::TFluentAny fluent) const;

    void OnDynamicConfigChanged(
        const TLayerCacheDynamicConfigPtr& oldConfig,
        const TLayerCacheDynamicConfigPtr& newConfig);

private:
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const IVolumeArtifactCachePtr ArtifactCache_;
    const IInvokerPtr ControlInvoker_;
    const std::vector<TLayerLocationPtr> LayerLocations_;
    const NContainers::IPortoExecutorPtr TmpfsExecutor_;

    NConcurrency::TAsyncSemaphorePtr Semaphore_;

    TTmpfsLayerCachePtr RegularTmpfsLayerCache_;
    TTmpfsLayerCachePtr NirvanaTmpfsLayerCache_;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    static TSlruCacheConfigPtr CreateCacheConfig(
        const NDataNode::TVolumeManagerConfigPtr& config,
        const std::vector<TLayerLocationPtr>& layerLocations);

    i64 GetWeight(const TLayerPtr& layer) const override;

    void OnAdded(const TLayerPtr& layer) override;

    void OnRemoved(const TLayerPtr& layer) override;

    void OnWeightUpdated(i64 weightDelta) override;

    void ProfileLocation(const TLayerLocationPtr& location);

    TLayerPtr FindLayerInTmpfs(const TArtifactKey& artifactKey, const TGuid& tag = TGuid());

    TFuture<TLayerPtr> DownloadAndImportLayer(
        const TArtifactKey& artifactKey,
        const TArtifactDownloadOptions& downloadOptions,
        TGuid tag,
        TLayerLocationPtr location);

    TLayerLocationPtr PickLocation() const;

    void OnProfiling();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

#define VOLUME_CACHE_INL_H_
#include "volume_cache-inl.h"
#undef VOLUME_CACHE_INL_H_
