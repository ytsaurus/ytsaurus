#include "porto_volume.h"
#include "private.h"
#include "layer_location.h"
#include "volume_counters.h"

#include <yt/yt/server/lib/nbd/server.h>
#include <yt/yt/server/lib/nbd/block_device.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NLogging;
using namespace NNbd;

////////////////////////////////////////////////////////////////////////////////

TPortoVolumeBase::TPortoVolumeBase(
    TTagSet tagSet,
    TVolumeMeta volumeMeta,
    TLayerLocationPtr layerLocation)
    : TagSet_(std::move(tagSet))
    , VolumeMeta_(std::move(volumeMeta))
    , LayerLocation_(std::move(layerLocation))
{ }

const TVolumeId& TPortoVolumeBase::GetId() const
{
    return VolumeMeta_.Id;
}

const std::string& TPortoVolumeBase::GetPath() const
{
    return VolumeMeta_.MountPath;
}

TFuture<void> TPortoVolumeBase::Link(
    TGuid tag,
    const TString& target)
{
    return TAsyncLockWriterGuard::Acquire(&Lock_)
        .AsUnique().Apply(BIND([tag, target, this, this_ = MakeStrong(this)] (
            TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>&& guard)
        {
            // Targets_ is protected with guard.
            Y_UNUSED(guard);

            Targets_.push_back(target);

            // TODO(dgolear): Switch to std::string.
            auto source = TString(GetPath());
            return LayerLocation_->LinkVolume(tag, source, target);
        }));
}

TFuture<void> TPortoVolumeBase::Remove()
{
    if (!RemovalRequested_.exchange(true)) {
        TAsyncLockWriterGuard::Acquire(&Lock_).AsUnique().Subscribe(BIND(
            [
                removalCallback = RemoveCallback_,
                removePromise = RemovePromise_,
                targets = Targets_,
                volumePath = VolumeMeta_.MountPath
            ] (TErrorOr<TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>>&& guardOrError) mutable {
                auto Logger = ExecNodeLogger()
                    .WithTag("VolumePath: %v", volumePath);

                YT_LOG_FATAL_UNLESS(guardOrError.IsOK(), guardOrError, "Failed to acquire lock (VolumePath: %v)", volumePath);

                auto guard = std::move(guardOrError.Value());
                auto removeFuture = removalCallback(targets).Apply(BIND(
                    [guard = std::move(guard), volumePath = std::move(volumePath)] (const TError& error) {
                        auto Logger = ExecNodeLogger()
                            .WithTag("VolumePath: %v", volumePath);

                        if (!error.IsOK()) {
                            YT_LOG_ERROR(
                                error,
                                "Failed to remove volume (VolumePath: %v)",
                                volumePath);
                        }
                        return error;
                    }));
                removePromise.SetFrom(removeFuture);
            }));
    }

    return RemovePromise_;
}

bool TPortoVolumeBase::IsCached() const
{
    return false;
}

TFuture<void> TPortoVolumeBase::DoRemoveVolumeCommon(
    const TString& volumeType,
    TTagSet tagSet,
    TLayerLocationPtr location,
    TVolumeMeta volumeMeta,
    TCallback<TFuture<void>(const TLogger&)> postRemovalCleanup)
{
    TEventTimerGuard volumeRemoveTimeGuard(
        TVolumeProfilerCounters::Get()->GetTimer(tagSet, "/remove_time"));

    const auto& volumeId = volumeMeta.Id;
    const auto& volumePath = volumeMeta.MountPath;

    auto Logger = ExecNodeLogger()
        .WithTag("VolumeType: %v, VolumeId: %v, VolumePath: %v",
            volumeType,
            volumeId,
            volumePath);

    YT_LOG_DEBUG("Removing volume");

    return location->RemoveVolume(tagSet, volumeId)
        .Apply(BIND(
            [
                Logger,
                cleanup = std::move(postRemovalCleanup),
                volumeRemoveTimeGuard = std::move(volumeRemoveTimeGuard)
            ] (const TError& error) {
                if (!error.IsOK()) {
                    YT_LOG_WARNING(error, "Failed to remove volume");
                } else {
                    YT_LOG_DEBUG("Removed volume");
                }

                // Perform post-removal cleanup if provided (e.g., device finalization, overlay cleanup)
                if (cleanup) {
                    return cleanup(Logger);
                }
                return VoidFuture;
            }))
        .ToUncancelable();
}

void TPortoVolumeBase::SetRemoveCallback(TCallback<TFuture<void>()> callback)
{
    // Unlink targets prior to removing volume.
    RemoveCallback_ = BIND(
        [
            location = LayerLocation_,
            volumePath = VolumeMeta_.MountPath,
            callback = std::move(callback)
        ] (const std::vector<TString>& targets) {
            return UnlinkTargets(location, volumePath, targets)
                .AsUnique().Apply(BIND([volumePath, callback = std::move(callback)] (TError&& error) {
                    auto Logger = ExecNodeLogger()
                        .WithTag("VolumePath: %v", volumePath);

                    if (!error.IsOK()) {
                        YT_LOG_WARNING(error, "Failed to unlink targets (VolumePath: %v)",
                            volumePath);
                    } else {
                        YT_LOG_DEBUG("Unlinked targets (VolumePath: %v)",
                            volumePath);
                    }
                    // Now remove the actual volume.
                    return callback();
                }));
        });
}

TFuture<void> TPortoVolumeBase::UnlinkTargets(TLayerLocationPtr location, TString source, std::vector<TString> targets)
{
    auto Logger = ExecNodeLogger()
        .WithTag("VolumePath: %v", source);

    YT_LOG_DEBUG("Unlinking targets (VolumePath: %v, Targets: %v)",
        source,
        targets);

    if (targets.empty()) {
        return OKFuture;
    }

    std::vector<TFuture<void>> futures;
    futures.reserve(targets.size());
    for (const auto& target : targets) {
        futures.emplace_back(location->UnlinkVolume(source, target));
    }

    return AllSucceeded(std::move(futures))
        .ToUncancelable();
}

////////////////////////////////////////////////////////////////////////////////

TSquashFSVolume::TSquashFSVolume(
    TTagSet tagSet,
    TVolumeMeta volumeMeta,
    IVolumeArtifactPtr artifact,
    TLayerLocationPtr location,
    const TArtifactKey& artifactKey)
    : TCachedVolume(
        std::move(tagSet),
        std::move(volumeMeta),
        std::move(location),
        artifactKey)
    , Artifact_(std::move(artifact))
{
    SetRemoveCallback(BIND(
        &TSquashFSVolume::DoRemove,
        TagSet_,
        LayerLocation_,
        VolumeMeta_));
}

TSquashFSVolume::~TSquashFSVolume()
{
    YT_UNUSED_FUTURE(Remove());
}

TFuture<void> TSquashFSVolume::DoRemove(
    TTagSet tagSet,
    TLayerLocationPtr location,
    TVolumeMeta volumeMeta)
{
    return DoRemoveVolumeCommon(
        "SquashFS",
        std::move(tagSet),
        std::move(location),
        std::move(volumeMeta));
}

DEFINE_REFCOUNTED_TYPE(TSquashFSVolume)

////////////////////////////////////////////////////////////////////////////////

TRWNbdVolume::TRWNbdVolume(
    TTagSet tagSet,
    TVolumeMeta volumeMeta,
    TLayerLocationPtr layerLocation,
    TString nbdDeviceId,
    INbdServerPtr nbdServer)
    : TPortoVolumeBase(
        std::move(tagSet),
        std::move(volumeMeta),
        std::move(layerLocation))
    , NbdDeviceId_(std::move(nbdDeviceId))
    , NbdServer_(std::move(nbdServer))
{
    SetRemoveCallback(BIND(
        &TRWNbdVolume::DoRemove,
        TagSet_,
        LayerLocation_,
        VolumeMeta_,
        NbdDeviceId_,
        NbdServer_));
}

TRWNbdVolume::~TRWNbdVolume()
{
    YT_UNUSED_FUTURE(Remove());
}

bool TRWNbdVolume::IsRootVolume() const
{
    return true;
}

TFuture<void> TRWNbdVolume::DoRemove(
    TTagSet tagSet,
    TLayerLocationPtr location,
    TVolumeMeta volumeMeta,
    TString nbdDeviceId,
    INbdServerPtr nbdServer)
{
    // First, unregister device. At this point device is removed from the
    // server but it remains in existing device connections.
    auto device = nbdServer->TryUnregisterDevice(nbdDeviceId);

    // Second, remove volume. At this point all device connections are going
    // be terminated.
    auto postRemovalCleanup = BIND_NO_PROPAGATE(
        [device = std::move(device)] (const TLogger& Logger) -> TFuture<void> {
            if (device) {
                YT_LOG_DEBUG("Finalizing RW NBD device");
                return device->Finalize();
            } else {
                YT_LOG_WARNING("Failed to finalize device; unknown device");
                return VoidFuture;
            }
        })
        .AsyncVia(nbdServer->GetInvoker());

    return DoRemoveVolumeCommon(
        "RW NBD",
        std::move(tagSet),
        std::move(location),
        std::move(volumeMeta),
        std::move(postRemovalCleanup));
}

DEFINE_REFCOUNTED_TYPE(TRWNbdVolume)

////////////////////////////////////////////////////////////////////////////////

TRONbdVolume::TRONbdVolume(
    TTagSet tagSet,
    TVolumeMeta volumeMeta,
    TLayerLocationPtr layerLocation,
    TString nbdDeviceId,
    INbdServerPtr nbdServer)
    : TCachedVolume(
        std::move(tagSet),
        std::move(volumeMeta),
        std::move(layerLocation),
        nbdDeviceId)
    , NbdDeviceId_(std::move(nbdDeviceId))
    , NbdServer_(std::move(nbdServer))
{
    SetRemoveCallback(BIND(
        &TRONbdVolume::DoRemove,
        TagSet_,
        LayerLocation_,
        VolumeMeta_,
        NbdDeviceId_,
        NbdServer_));
}

TRONbdVolume::~TRONbdVolume()
{
    YT_UNUSED_FUTURE(Remove());
}

TFuture<void> TRONbdVolume::DoRemove(
    TTagSet tagSet,
    TLayerLocationPtr location,
    TVolumeMeta volumeMeta,
    TString nbdDeviceId,
    INbdServerPtr nbdServer)
{
    // First, unregister device. At this point device is removed from the
    // server but it remains in existing device connections.
    auto device = nbdServer->TryUnregisterDevice(nbdDeviceId);

    // Second, remove volume. At this point all device connections are going
    // be terminated.
    auto postRemovalCleanup = BIND_NO_PROPAGATE(
        [device = std::move(device)] (const TLogger& Logger) -> TFuture<void> {
            if (device) {
                YT_LOG_DEBUG("Finalizing RO NBD device");
                return device->Finalize();
            } else {
                YT_LOG_WARNING("Failed to finalize device; unknown device");
                return VoidFuture;
            }
        })
        .AsyncVia(nbdServer->GetInvoker());

    return DoRemoveVolumeCommon(
        "RO NBD",
        std::move(tagSet),
        std::move(location),
        std::move(volumeMeta),
        std::move(postRemovalCleanup));
}

DEFINE_REFCOUNTED_TYPE(TRONbdVolume)

////////////////////////////////////////////////////////////////////////////////

TOverlayVolume::TOverlayVolume(
    TTagSet tagSet,
    TVolumeMeta volumeMeta,
    TLayerLocationPtr location,
    std::vector<TOverlayData> overlayDataArray)
    : TPortoVolumeBase(
        std::move(tagSet),
        std::move(volumeMeta),
        std::move(location))
    , OverlayDataArray_(std::move(overlayDataArray))
{
    SetRemoveCallback(BIND(
        &TOverlayVolume::DoRemove,
        TagSet_,
        LayerLocation_,
        VolumeMeta_,
        OverlayDataArray_));
}

TOverlayVolume::~TOverlayVolume()
{
    YT_UNUSED_FUTURE(Remove());
}

bool TOverlayVolume::IsRootVolume() const
{
    return false;
}

TFuture<void> TOverlayVolume::DoRemove(
    TTagSet tagSet,
    TLayerLocationPtr location,
    TVolumeMeta volumeMeta,
    std::vector<TOverlayData> overlayDataArray)
{
    // At first remove overlay volume, then remove constituent volumes and layers.
    auto postRemovalCleanup = BIND_NO_PROPAGATE([overlayDataArray = std::move(overlayDataArray)] (const TLogger&) mutable -> TFuture<void> {
        std::vector<TFuture<void>> futures;
        futures.reserve(overlayDataArray.size());
        for (auto& overlayData : overlayDataArray) {
            futures.push_back(overlayData.Remove());
        }
        return AllSucceeded(std::move(futures));
    });

    return DoRemoveVolumeCommon(
        "Overlay",
        std::move(tagSet),
        std::move(location),
        std::move(volumeMeta),
        std::move(postRemovalCleanup));
}

DEFINE_REFCOUNTED_TYPE(TOverlayVolume)

////////////////////////////////////////////////////////////////////////////////

TTmpfsVolume::TTmpfsVolume(
    TTagSet tagSet,
    TVolumeMeta volumeMeta,
    TLayerLocationPtr location)
    : TPortoVolumeBase(
        std::move(tagSet),
        std::move(volumeMeta),
        std::move(location))
{
    SetRemoveCallback(BIND(
        &TTmpfsVolume::DoRemove,
        TagSet_,
        LayerLocation_,
        VolumeMeta_));
}

TTmpfsVolume::~TTmpfsVolume()
{
    YT_UNUSED_FUTURE(Remove());
}

bool TTmpfsVolume::IsRootVolume() const
{
    return false;
}

TFuture<void> TTmpfsVolume::DoRemove(
    TTagSet tagSet,
    TLayerLocationPtr location,
    TVolumeMeta volumeMeta)
{
    return DoRemoveVolumeCommon(
        "Tmpfs",
        std::move(tagSet),
        std::move(location),
        std::move(volumeMeta));
}

DEFINE_REFCOUNTED_TYPE(TTmpfsVolume)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
