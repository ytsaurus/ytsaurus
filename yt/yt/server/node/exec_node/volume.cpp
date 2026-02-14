#include "volume.h"

#include "layer_location.h"
#include "private.h"
#include "volume_counters.h"

#include <yt/yt/server/tools/tools.h>
#include <yt/yt/server/tools/proc.h>

#include <yt/yt/core/actions/bind.h>

#include <util/digest/city.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IVolume)

////////////////////////////////////////////////////////////////////////////////

TLayer::TLayer(const TLayerMeta& layerMeta, const TArtifactKey& artifactKey, const TLayerLocationPtr& layerLocation)
    : TAsyncCacheValueBase<TArtifactKey, TLayer>(artifactKey)
    , LayerMeta_(layerMeta)
    , Location_(layerLocation)
{ }

TLayer::~TLayer()
{
    auto removalNeeded = IsLayerRemovalNeeded_;
    YT_LOG_INFO(
        "Layer is destroyed (LayerId: %v, LayerPath: %v, RemovalNeeded: %v)",
        LayerMeta_.Id,
        LayerMeta_.Path,
        removalNeeded);

    if (removalNeeded) {
        Location_->RemoveLayer(LayerMeta_.Id)
            .Subscribe(BIND([layerId = LayerMeta_.Id] (const TError& result) {
                YT_LOG_ERROR_IF(!result.IsOK(), result, "Failed to remove layer (LayerId: %v)", layerId);
            }));
    }
}

const TString& TLayer::GetCypressPath() const
{
    return GetKey().data_source().path();
}

const std::string& TLayer::GetPath() const
{
    return LayerMeta_.Path;
}

i64 TLayer::GetSize() const
{
    return LayerMeta_.size();
}

const TLayerMeta& TLayer::GetMeta() const
{
    return LayerMeta_;
}

void TLayer::IncreaseHitCount()
{
    HitCount_.fetch_add(1);
}

int TLayer::GetHitCount() const
{
    return HitCount_.load();
}

void TLayer::SetLayerRemovalNotNeeded()
{
    IsLayerRemovalNeeded_ = false;
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TLayer)

////////////////////////////////////////////////////////////////////////////////

const std::string& TOverlayData::GetPath() const
{
    if (std::holds_alternative<TLayerPtr>(Variant_)) {
        return std::get<TLayerPtr>(Variant_)->GetPath();
    }

    return std::get<IVolumePtr>(Variant_)->GetPath();
}

TFuture<void> TOverlayData::Remove()
{
    if (IsLayer()) {
        return OKFuture;
    }

    const auto& self = GetVolume();
    if (self->IsCached()) {
        return OKFuture;
    }

    return self->Remove();
}

////////////////////////////////////////////////////////////////////////////////

TSimpleTmpfsVolume::TSimpleTmpfsVolume(
    TTagSet tagSet,
    const std::string& path,
    IInvokerPtr invoker,
    bool detachUnmount)
    : TagSet_(std::move(tagSet))
    , Path_(path)
    , VolumeId_(
        [&path] {
            auto [low, high] = CityHash128(path.c_str(), path.size());
            return TGuid(low, high);
        }())
    , Invoker_(std::move(invoker))
    , DetachUnmount_(detachUnmount)
{ }

TSimpleTmpfsVolume::~TSimpleTmpfsVolume()
{
    YT_UNUSED_FUTURE(Remove());
}

bool TSimpleTmpfsVolume::IsCached() const
{
    return false;
}

TFuture<void> TSimpleTmpfsVolume::Link(
    TGuid,
    const TString&)
{
    // Simple volume is created inside sandbox, so we don't need to link it.
    YT_UNIMPLEMENTED("Link is not implemented for SimpleTmpfsVolume");
}

TFuture<void> TSimpleTmpfsVolume::Remove()
{
    if (RemoveFuture_) {
        return RemoveFuture_;
    }

    TEventTimerGuard volumeRemoveTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(TagSet_, "/remove_time"));

    const auto volumeType = EVolumeType::Tmpfs;
    const auto& volumeId = VolumeId_;
    const auto& volumePath = Path_;

    auto Logger = ExecNodeLogger()
        .WithTag("VolumeType: %v, VolumeId: %v, VolumePath: %v",
            volumeType,
            volumeId,
            volumePath);

    RemoveFuture_ = BIND(
        [
            tagSet = TagSet_,
            Logger,
            this,
            this_ = MakeStrong(this)
        ] {
            try {
                RunTool<TRemoveDirContentAsRootTool>(Path_);

                auto config = New<TUmountConfig>();
                config->Path = Path_;
                config->Detach = DetachUnmount_;
                RunTool<TUmountAsRootTool>(config);

                TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
                    .Update(VolumeCounters().Decrement(tagSet));
                TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/removed").Increment(1);
            } catch (const std::exception& ex) {
                TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/remove_errors").Increment(1);

                YT_LOG_ERROR(
                    ex,
                    "Failed to remove volume");

                THROW_ERROR_EXCEPTION("Failed to remove volume")
                    << ex;
            }
        })
        .AsyncVia(Invoker_)
        .Run()
        .ToUncancelable();

    return RemoveFuture_;
}

const TVolumeId& TSimpleTmpfsVolume::GetId() const
{
    return VolumeId_;
}

const std::string& TSimpleTmpfsVolume::GetPath() const
{
    return Path_;
}

bool TSimpleTmpfsVolume::IsRootVolume() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TSimpleTmpfsVolume)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
