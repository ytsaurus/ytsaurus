#pragma once

#include "public.h"
#include "volume_meta.h"

#include <yt/yt/server/node/exec_node/artifact.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TLayer
    : public TAsyncCacheValueBase<TArtifactKey, TLayer>
{
public:
    TLayer(
        const TLayerMeta& layerMeta,
        const TArtifactKey& artifactKey,
        const TLayerLocationPtr& layerLocation);

    ~TLayer();

    const NYPath::TYPath& GetCypressPath() const;

    const std::string& GetPath() const;

    i64 GetSize() const;

    const TLayerMeta& GetMeta() const;

    void IncreaseHitCount();

    int GetHitCount() const;

    void SetLayerRemovalNotNeeded();

private:
    const TLayerMeta LayerMeta_;
    const TLayerLocationPtr Location_;
    std::atomic<int> HitCount_;

    // If the slot manager is disabled, the layers that are currently in the layer cache
    // do not need to be removed from the porto. If you delete them, firstly, in this
    // case they will need to be re-imported into the porto, and secondly, then there
    // may be a problem when inserting the same layers when starting a new volume manager.
    // Namely, a layer object that is already in the new cache may be deleted from the old cache,
    // in which case the layer object in the new cache will be corrupted.
    bool IsLayerRemovalNeeded_ = true;
};

DEFINE_REFCOUNTED_TYPE(TLayer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
