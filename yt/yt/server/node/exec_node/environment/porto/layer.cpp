#include "layer.h"

#include "layer_location.h"

#include <yt/yt/server/node/exec_node/private.h>

#include <yt/yt/core/actions/bind.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TLayer::TLayer(
    const TLayerMeta& layerMeta,
    const TArtifactKey& artifactKey,
    const TLayerLocationPtr& layerLocation)
    : TAsyncCacheValueBase<TArtifactKey, TLayer>(artifactKey)
    , LayerMeta_(layerMeta)
    , Location_(layerLocation)
{ }

TLayer::~TLayer()
{
    auto removalNeeded = IsLayerRemovalNeeded_;
    YT_TLOG_INFO("Layer is destroyed")
        .With("LayerId", LayerMeta_.Id)
        .With("LayerPath", LayerMeta_.Path)
        .With("RemovalNeeded", removalNeeded);

    if (removalNeeded) {
        Location_->RemoveLayer(LayerMeta_.Id)
            .Subscribe(BIND([layerId = LayerMeta_.Id] (const TError& result) {
                YT_TLOG_ERROR_IF(!result.IsOK(), "Failed to remove layer")
                    .With("LayerId", layerId)
                    .With(result);
            }));
    }
}

const NYPath::TYPath& TLayer::GetCypressPath() const
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

} // namespace NYT::NExecNode
