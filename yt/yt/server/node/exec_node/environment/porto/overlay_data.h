#pragma once

#include "public.h"

#include <yt/yt/server/node/exec_node/volume.h>

#include <variant>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TOverlayData
{
public:
    TOverlayData() = default;

    explicit TOverlayData(TLayerPtr layer);

    explicit TOverlayData(IVolumePtr volume);

    const std::string& GetPath() const;

    bool IsLayer() const;

    const TLayerPtr& GetLayer() const;

    bool IsVolume() const;

    const IVolumePtr& GetVolume() const;

    TFuture<void> Remove();

private:
    std::variant<TLayerPtr, IVolumePtr> Variant_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
