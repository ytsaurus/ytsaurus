#ifndef STATE_CACHE_INL_H_
    #error "Direct inclusion of this file is not allowed, include state_cache.h"
    // For the sake of sane code completion.
    #include "state_cache.h"
#endif

////////////////////////////////////////////////////////////////////////////////

inline size_t THash<NYT::NFlow::TStateCacheKey>::operator()(const NYT::NFlow::TStateCacheKey& key) const
{
    // Preserve the hash of (jobId, key, name), including its shard assignment.
    return key.Namespace->Hash ^ IntHash(THash<std::optional<NYT::NFlow::TKey>>()(key.Key));
}

////////////////////////////////////////////////////////////////////////////////
