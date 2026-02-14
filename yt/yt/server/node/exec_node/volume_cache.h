#pragma once

#include "public.h"
#include "artifact.h"
#include "volume.h"
#include "porto_volume.h"
#include "preparation_options.h"
#include "volume_options.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TLayerLocationPtr DoPickLocation(
    const std::vector<TLayerLocationPtr> locations,
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

//! This class caches volumes generated from cypress files (layers).
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
    const IBootstrap* const Bootstrap_;
    const std::vector<TLayerLocationPtr> LayerLocations_;

    TLayerLocationPtr PickLocation();

    void OnAdded(const TIntrusivePtr<TCachedVolume<TKey>>& volume) override;

    void OnRemoved(const TIntrusivePtr<TCachedVolume<TKey>>& volume) override;

    void OnWeightUpdated(i64 weightDelta) override;
};

DEFINE_REFCOUNTED_TYPE(TVolumeCacheBase<TArtifactKey>)

////////////////////////////////////////////////////////////////////////////////

//! This class caches volumes generated from cypress files (layers).
class TSquashFSVolumeCache
    : public TVolumeCacheBase<TArtifactKey>
{
public:
    TSquashFSVolumeCache(
        IBootstrap* const bootstrap,
        std::vector<TLayerLocationPtr> layerLocations,
        IVolumeArtifactCachePtr artifactCache);

    TFuture<IVolumePtr> GetOrCreateVolume(
        TGuid tag,
        const TArtifactKey& artifactKey,
        const TArtifactDownloadOptions& downloadOptions);

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

DECLARE_REFCOUNTED_CLASS(TSquashFSVolumeCache)

////////////////////////////////////////////////////////////////////////////////

//! This class caches volumes generated from cypress files (layers).
class TRONbdVolumeCache
    : public TVolumeCacheBase<TString>
{
public:
    using TVolumePtr = TIntrusivePtr<TCachedVolume<TString>>;

    TRONbdVolumeCache(
        IBootstrap* const bootstrap,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        std::vector<TLayerLocationPtr> layerLocations);

    TFuture<IVolumePtr> GetOrCreateVolume(
        TGuid tag,
        TPrepareRONbdVolumeOptions options);

private:
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, InsertLock_);

    static void ValidatePrepareNbdVolumeOptions(const TPrepareRONbdVolumeOptions& options);

    TInsertCookie GetInsertCookie(const TString& deviceId, const NNbd::INbdServerPtr& nbdServer);

    //! Make callback that subscribes job for NBD device errors.
    TExtendedCallback<TVolumePtr(const TErrorOr<TVolumePtr>&)> MakeJobSubscriberForDeviceErrors(
        TJobId jobId,
        const TString& deviceId,
        const NNbd::INbdServerPtr& nbdServer,
        const NLogging::TLogger& Logger);

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
};

DECLARE_REFCOUNTED_CLASS(TRONbdVolumeCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

#define VOLUME_CACHE_INL_H_
#include "volume_cache-inl.h"
#undef VOLUME_CACHE_INL_H_
