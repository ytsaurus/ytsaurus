#include "overlay_data.h"

#include "layer.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TOverlayData::TOverlayData(TLayerPtr layer)
    : Variant_(std::move(layer))
{ }

TOverlayData::TOverlayData(IVolumePtr volume)
    : Variant_(std::move(volume))
{ }

const std::string& TOverlayData::GetPath() const
{
    if (std::holds_alternative<TLayerPtr>(Variant_)) {
        return std::get<TLayerPtr>(Variant_)->GetPath();
    }

    return std::get<IVolumePtr>(Variant_)->GetPath();
}

bool TOverlayData::IsLayer() const
{
    return std::holds_alternative<TLayerPtr>(Variant_);
}

const TLayerPtr& TOverlayData::GetLayer() const
{
    return std::get<TLayerPtr>(Variant_);
}

bool TOverlayData::IsVolume() const
{
    return !IsLayer();
}

const IVolumePtr& TOverlayData::GetVolume() const
{
    return std::get<IVolumePtr>(Variant_);
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
