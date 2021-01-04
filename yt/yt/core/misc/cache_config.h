#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSlruCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! The maximum number of weight units cached items are allowed to occupy.
    //! Zero means that no items are cached.
    i64 Capacity;

    //! The fraction of total capacity given to the younger segment.
    double YoungerSizeFraction;

    //! Number of shards.
    int ShardCount;

    //! Capacity of internal buffer used to amortize and de-contend touch operations.
    int TouchBufferCapacity;

    explicit TSlruCacheConfig(i64 capacity = 0);
};

DEFINE_REFCOUNTED_TYPE(TSlruCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlruCacheDynamicConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! The maximum number of weight units cached items are allowed to occupy.
    //! Zero means that no items are cached.
    std::optional<i64> Capacity;

    //! The fraction of total capacity given to the younger segment.
    std::optional<double> YoungerSizeFraction;

    TSlruCacheDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TSlruCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

//! Cache which removes entries after a while.
/*!
 * TAsyncExpiringCache acts like a proxy between a client and a remote service:
 * requests are sent to the service and responses are saved in the cache as entries.
 * Next time the client makes a request, the response can be taken from the cache
 * unless it is expired.
 *
 * An entry is considered expired if at least one of the following conditions is true:
 * 1) last access was more than ExpireAfterAccessTime ago,
 * 2) last update was more than ExpireAfter*UpdateTime ago.
 *
 * To avoid client awaiting time on subsequent requests and keep the response
 * up to date, the cache updates entries in the background:
 * If request was successful, the cache performs the same request after RefreshTime
 * and updates the entry.
 * If request was unsuccessful, the entry (which contains error response) will be expired
 * after ExpireAfterFailedUpdateTime.
 */
class TAsyncExpiringCacheConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Time since last finished Get() after which an entry is removed.
    TDuration ExpireAfterAccessTime;

    //! Time since last update, if succeeded, after which an entry is removed.
    TDuration ExpireAfterSuccessfulUpdateTime;

    //! Time since last update, if it failed, after which an entry is removed.
    TDuration ExpireAfterFailedUpdateTime;

    //! Time before next (background) update.
    std::optional<TDuration> RefreshTime;

    //! If set to true, cache will invoke DoGetMany once instead of DoGet on every entry during an update.
    bool BatchUpdate;

    TAsyncExpiringCacheConfig();
};

DEFINE_REFCOUNTED_TYPE(TAsyncExpiringCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
