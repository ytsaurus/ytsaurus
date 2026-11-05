#pragma once

#include "public.h"
#include "layer.h"
#include "overlay_data.h"

#include <yt/yt/server/node/exec_node/artifact.h>
#include <yt/yt/server/node/exec_node/volume.h>
#include <yt/yt/server/node/exec_node/volume_artifact.h>

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TPortoVolumeBase
    : public IVolume
{
public:
    TPortoVolumeBase(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation);

    const TVolumeId& GetId() const override final;

    const std::string& GetPath() const override final;

    TFuture<void> Link(
        TGuid tag,
        const std::string& target) override final;

    TFuture<void> Unlink() override final;

    TFuture<void> Remove() override final;

    bool IsCached() const override;

protected:
    const NProfiling::TTagSet TagSet_;
    const TVolumeMeta VolumeMeta_;
    const TLayerLocationPtr LayerLocation_;

    const TPromise<void> RemovePromise_ = NewPromise<void>();
    std::atomic<bool> RemovalRequested_ = false;

    static TFuture<void> DoRemoveVolumeCommon(
        const std::string& volumeType,
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        TCallback<TFuture<void>(const NLogging::TLogger&)> postRemovalCleanup = {});

    void SetRemoveCallback(TCallback<TFuture<void>()> callback);

private:
    NConcurrency::TAsyncReaderWriterLock Lock_;
    std::vector<std::string> Targets_;

    TCallback<TFuture<void>(const std::vector<std::string>&)> RemoveCallback_;

    static TFuture<void> UnlinkTargets(TLayerLocationPtr location, const std::string& source, const std::vector<std::string>& targets);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TKey>
class TCachedVolume
    : public TPortoVolumeBase
    , public TAsyncCacheValueBase<TKey, TCachedVolume<TKey>>
{
public:
    TCachedVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation,
        const TKey& key);

    bool IsCached() const override final;
};

////////////////////////////////////////////////////////////////////////////////

class TSquashFSVolume
    : public TCachedVolume<TArtifactKey>
{
public:
    TSquashFSVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        IVolumeArtifactPtr artifact,
        TLayerLocationPtr location,
        const TArtifactKey& artifactKey);

    ~TSquashFSVolume() override;

private:
    // We store chunk cache artifact here to make sure that SquashFS file outlives SquashFS volume.
    const IVolumeArtifactPtr Artifact_;

    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta);
};

DEFINE_REFCOUNTED_TYPE(TSquashFSVolume)

////////////////////////////////////////////////////////////////////////////////

class TRWNbdVolume
    : public TPortoVolumeBase
{
public:
    TRWNbdVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation,
        std::string nbdDeviceId,
        NNbd::INbdServerPtr nbdServer);

    ~TRWNbdVolume() override;

private:
    const std::string NbdDeviceId_;
    const NNbd::INbdServerPtr NbdServer_;

    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        std::string nbdDeviceId,
        NNbd::INbdServerPtr nbdServer);
};

DEFINE_REFCOUNTED_TYPE(TRWNbdVolume)

////////////////////////////////////////////////////////////////////////////////

class TRONbdVolume
    : public TCachedVolume<std::string>
{
public:
    TRONbdVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation,
        std::string nbdDeviceId,
        NNbd::INbdServerPtr nbdServer);

    ~TRONbdVolume() override;

private:
    const std::string NbdDeviceId_;
    const NNbd::INbdServerPtr NbdServer_;

    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        std::string nbdDeviceId,
        NNbd::INbdServerPtr nbdServer);
};

DEFINE_REFCOUNTED_TYPE(TRONbdVolume)

////////////////////////////////////////////////////////////////////////////////

class TOverlayVolume
    : public TPortoVolumeBase
{
public:
    TOverlayVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr location,
        std::vector<TOverlayData> overlayDataArray,
        IVolumePtr volumeForUpperLayer);

    ~TOverlayVolume() override;

private:
    // Holds volumes and layers (so that they are not destroyed) while they are needed.
    const std::vector<TOverlayData> OverlayDataArray_;

    IVolumePtr VolumeForUpperLayer_;

    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        std::vector<TOverlayData> overlayDataArray,
        IVolumePtr volumeForUpperLayer);
};

DEFINE_REFCOUNTED_TYPE(TOverlayVolume)

////////////////////////////////////////////////////////////////////////////////

class TTmpfsVolume
    : public TPortoVolumeBase
{
public:
    TTmpfsVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr location);

    ~TTmpfsVolume() override;

private:
    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta);
};

DEFINE_REFCOUNTED_TYPE(TTmpfsVolume)

////////////////////////////////////////////////////////////////////////////////

class TLoopVolume
    : public TPortoVolumeBase
{
public:
    TLoopVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr location);

    ~TLoopVolume() override;

private:
    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta);
};

DEFINE_REFCOUNTED_TYPE(TLoopVolume)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

#define PORTO_VOLUME_INL_H_
#include "porto_volume-inl.h"
#undef PORTO_VOLUME_INL_H_
