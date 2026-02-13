#pragma once

#include "public.h"
#include "volume.h"
#include "artifact.h"

#include <yt/yt/server/lib/nbd/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

// Forward declarations.
class TLayerLocation;
DECLARE_REFCOUNTED_CLASS(TLayerLocation)

class TVolumeProfilerCounters;

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
        const TString& target) override final;

    TFuture<void> Remove() override final;

    bool IsCached() const override;

protected:
    const NProfiling::TTagSet TagSet_;
    const TVolumeMeta VolumeMeta_;
    const TLayerLocationPtr LayerLocation_;

    TPromise<void> RemovePromise_ = NewPromise<void>();
    std::atomic<bool> RemovalRequested_{false};

    static TFuture<void> DoRemoveVolumeCommon(
        const TString& volumeType,
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        TCallback<TFuture<void>(const NLogging::TLogger&)> postRemovalCleanup = {});

    void SetRemoveCallback(TCallback<TFuture<void>()> callback);

private:
    NConcurrency::TAsyncReaderWriterLock Lock_;
    std::vector<TString> Targets_;

    TCallback<TFuture<void>(const std::vector<TString>&)> RemoveCallback_;

    static TFuture<void> UnlinkTargets(TLayerLocationPtr location, TString source, std::vector<TString> targets);
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
        const TKey& key)
        : TPortoVolumeBase(
            std::move(tagSet),
            std::move(volumeMeta),
            std::move(layerLocation))
        , TAsyncCacheValueBase<TKey, TCachedVolume<TKey>>(key)
    { }

    bool IsCached() const override final
    {
        return true;
    }

    bool IsRootVolume() const override final
    {
        return false;
    }
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

DECLARE_REFCOUNTED_CLASS(TSquashFSVolume)

////////////////////////////////////////////////////////////////////////////////

class TRWNbdVolume
    : public TPortoVolumeBase
{
public:
    TRWNbdVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation,
        TString nbdDeviceId,
        NNbd::INbdServerPtr nbdServer);

    ~TRWNbdVolume() override;

    bool IsRootVolume() const override final;

private:
    const TString NbdDeviceId_;
    const NNbd::INbdServerPtr NbdServer_;

    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        TString nbdDeviceId,
        NNbd::INbdServerPtr nbdServer);
};

DECLARE_REFCOUNTED_CLASS(TRWNbdVolume)

////////////////////////////////////////////////////////////////////////////////

class TRONbdVolume
    : public TCachedVolume<TString>
{
public:
    TRONbdVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr layerLocation,
        TString nbdDeviceId,
        NNbd::INbdServerPtr nbdServer);

    ~TRONbdVolume() override;

private:
    const TString NbdDeviceId_;
    const NNbd::INbdServerPtr NbdServer_;

    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        TString nbdDeviceId,
        NNbd::INbdServerPtr nbdServer);
};

DECLARE_REFCOUNTED_CLASS(TRONbdVolume)

////////////////////////////////////////////////////////////////////////////////

class TOverlayVolume
    : public TPortoVolumeBase
{
public:
    TOverlayVolume(
        NProfiling::TTagSet tagSet,
        TVolumeMeta volumeMeta,
        TLayerLocationPtr location,
        std::vector<TOverlayData> overlayDataArray);

    ~TOverlayVolume() override;

    bool IsRootVolume() const override final;

private:
    // Holds volumes and layers (so that they are not destroyed) while they are needed.
    const std::vector<TOverlayData> OverlayDataArray_;

    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta,
        std::vector<TOverlayData> overlayDataArray);
};

DECLARE_REFCOUNTED_CLASS(TOverlayVolume)

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

    bool IsRootVolume() const override final;

private:
    static TFuture<void> DoRemove(
        NProfiling::TTagSet tagSet,
        TLayerLocationPtr location,
        TVolumeMeta volumeMeta);
};

DECLARE_REFCOUNTED_CLASS(TTmpfsVolume)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
