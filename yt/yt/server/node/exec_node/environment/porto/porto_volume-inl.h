#ifndef PORTO_VOLUME_INL_H_
#error "Direct inclusion of this file is not allowed, include porto_volume.h"
// For the sake of sane code completion.
#include "porto_volume.h"
#endif

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

template <typename TKey>
TCachedVolume<TKey>::TCachedVolume(
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

template <typename TKey>
bool TCachedVolume<TKey>::IsCached() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
