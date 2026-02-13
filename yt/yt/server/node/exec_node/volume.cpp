#include "volume.h"

#include "layer_location.h"

#include <yt/yt/core/actions/bind.h>

namespace NYT::NExecNode {

using namespace NConcurrency;

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

} // namespace NYT::NExecNode
