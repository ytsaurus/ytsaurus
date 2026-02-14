#ifndef VOLUME_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include volume_cache.h"
// For the sake of sane code completion.
#include "volume_cache.h"
#endif

#include "layer_location.h"
#include "private.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TAsyncMapBase<TKey, TValue>::TAsyncMapBase(const NProfiling::TProfiler& profiler)
    : TBase(TSlruCacheConfig::CreateWithCapacity(0, /*shardCount*/ 1), profiler)
{ }

template <class TKey, class TValue>
i64 TAsyncMapBase<TKey, TValue>::GetWeight(const typename TBase::TValuePtr& /*value*/) const
{
    return 2;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TKey>
TVolumeCacheBase<TKey>::TVolumeCacheBase(
    const NProfiling::TProfiler& profiler,
    IBootstrap* const bootstrap,
    std::vector<TLayerLocationPtr> layerLocations)
    : TAsyncMapBase<TKey, TCachedVolume<TKey>>(profiler)
    , Bootstrap_(bootstrap)
    , LayerLocations_(std::move(layerLocations))
{ }

template <typename TKey>
bool TVolumeCacheBase<TKey>::IsEnabled() const
{
    for (const auto& location : LayerLocations_) {
        if (location->IsEnabled()) {
            return true;
        }
    }

    return false;
}

template <typename TKey>
TLayerLocationPtr TVolumeCacheBase<TKey>::PickLocation()
{
    return DoPickLocation(LayerLocations_, [] (const TLayerLocationPtr& candidate, const TLayerLocationPtr& current) {
        return candidate->GetVolumeCount() < current->GetVolumeCount();
    });
}

template <typename TKey>
void TVolumeCacheBase<TKey>::OnAdded(const TIntrusivePtr<TCachedVolume<TKey>>& volume)
{
    const auto Logger = ExecNodeLogger;
    YT_LOG_DEBUG("Volume added to cache (VolumeId: %v)",
        volume->GetId());
}

template <typename TKey>
void TVolumeCacheBase<TKey>::OnRemoved(const TIntrusivePtr<TCachedVolume<TKey>>& volume)
{
    const auto Logger = ExecNodeLogger;
    YT_LOG_DEBUG("Volume removed from cache (VolumeId: %v)",
        volume->GetId());
}

template <typename TKey>
void TVolumeCacheBase<TKey>::OnWeightUpdated(i64 weightDelta)
{
    const auto Logger = ExecNodeLogger;
    YT_LOG_DEBUG("Volume cache total weight updated (WeightDelta: %v)",
        weightDelta);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
