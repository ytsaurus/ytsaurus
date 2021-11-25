#include "chaos_cache.h"

#include "private.h"

#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/concurrency/spinlock.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/string.h>
#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/server/lib/chaos_cache/config.h>

#include <util/digest/multi.h>

namespace NYT::NMasterCache {

using namespace NConcurrency;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NObjectClient;
using namespace NChaosClient;
using namespace NChaosCache;

////////////////////////////////////////////////////////////////////////////////

const auto static& Logger = MasterCacheLogger;

////////////////////////////////////////////////////////////////////////////////

TChaosCacheKey::TChaosCacheKey(
    TString user,
    TReplicationCardToken replicationCardToken,
    bool requestCoordinators,
    bool requestProgress,
    bool requestHistory)
    : User(std::move(user))
    , ReplicationCardToken(std::move(replicationCardToken))
    , RequestCoordinators(requestCoordinators)
    , RequestProgress(requestProgress)
    , RequestHistory(requestHistory)
{ }

TChaosCacheKey::operator size_t() const
{
    return MultiHash(
        User,
        ReplicationCardToken,
        RequestCoordinators,
        RequestProgress,
        RequestHistory);
}

bool TChaosCacheKey::operator == (const TChaosCacheKey& other) const
{
    return
        User == other.User &&
        ReplicationCardToken == other.ReplicationCardToken &&
        RequestCoordinators == other.RequestCoordinators &&
        RequestProgress == other.RequestProgress &&
        RequestHistory == other.RequestHistory;
}

void FormatValue(TStringBuilderBase* builder, const TChaosCacheKey& key, TStringBuf /*format*/)
{
    builder->AppendFormat("{User:%v Token:%v Coordinators:%v Progress:%v History:%v}",
        key.User,
        key.ReplicationCardToken,
        key.RequestCoordinators,
        key.RequestProgress,
        key.RequestHistory);
}

TString ToString(const TChaosCacheKey& key)
{
    return ToStringViaBuilder(key);
}

////////////////////////////////////////////////////////////////////////////////

TChaosCacheEntry::TChaosCacheEntry(
    const TChaosCacheKey& key,
    TInstant timestamp,
    TErrorOr<NChaosClient::TReplicationCardPtr> replicationCard)
    : TAsyncCacheValueBase(key)
    , Timestamp_(timestamp)
    , ReplicationCard_(replicationCard)
{ }

bool TChaosCacheEntry::GetSuccess() const
{
    return ReplicationCard_.IsOK();
}

////////////////////////////////////////////////////////////////////////////////

TCacheProfilingCounters::TCacheProfilingCounters(const NProfiling::TProfiler& profiler)
    : HitRequestCount(profiler.Counter("/hit_request_count"))
    , HitResponseBytes(profiler.Counter("/hit_response_bytes"))
    , MissRequestCount(profiler.Counter("/miss_request_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

TChaosCache::TChaosCache(
    TChaosCacheConfigPtr config,
    const NProfiling::TProfiler& profiler)
    : TAsyncSlruCacheBase(config)
    , Profiler_(profiler)
{ }

TChaosCache::TCookie TChaosCache::BeginLookup(
    TRequestId requestId,
    const TChaosCacheKey& key,
    TDuration successExpirationTime,
    TDuration failureExpirationTime)
{
    auto entry = Find(key);
    bool cacheHit = false;
    if (entry) {
        if (IsExpired(entry, successExpirationTime, failureExpirationTime)) {
            YT_LOG_DEBUG("Cache entry expired (RequestId: %v, Key: %v, Success: %v)",
                requestId,
                key,
                entry->GetSuccess());

            TryRemoveValue(entry);

        } else {
            cacheHit = true;
            YT_LOG_DEBUG("Cache hit (RequestId: %v, Key: %v, Success: %v)",
                requestId,
                key,
                entry->GetSuccess());
        }
    }

    auto counters = GetProfilingCounters(key.User);
    if (cacheHit) {
        counters->HitRequestCount.Increment();
        counters->HitResponseBytes.Increment(entry->GetTotalSpace());
    } else {
        counters->MissRequestCount.Increment();
    }

    return BeginInsert(key);
}

void TChaosCache::EndLookup(
    NRpc::TRequestId requestId,
    TCookie cookie,
    TErrorOr<NChaosClient::TReplicationCardPtr> replicationCard)
{
    const auto& key = cookie.GetKey();

    YT_LOG_DEBUG("Cache population request succeeded (RequestId: %v, Key: %v, Success: %v)",
        requestId,
        key,
        replicationCard.IsOK());

    auto entry = New<TChaosCacheEntry>(
        key,
        TInstant::Now(),
        replicationCard);

    cookie.EndInsert(entry);
}

TCacheProfilingCountersPtr TChaosCache::GetProfilingCounters(const TString& user)
{
    {
        auto guard = ReaderGuard(Lock_);
        if (auto it = UserToCounters_.find(user)) {
            return it->second;
        }
    }

    auto counters = New<TCacheProfilingCounters>(Profiler_
        .WithTag("user", user));

    {
        auto guard = WriterGuard(Lock_);
        auto [it, inserted] = UserToCounters_.emplace(user, std::move(counters));
        return it->second;
    }
}

bool TChaosCache::IsResurrectionSupported() const
{
    return false;
}

void TChaosCache::OnAdded(const TChaosCacheEntryPtr& entry)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TAsyncSlruCacheBase::OnAdded(entry);

    const auto& key = entry->GetKey();
    YT_LOG_DEBUG("Cache entry added (Key: %v, Success: %v, TotalSpace: %v)",
        key,
        entry->GetSuccess(),
        entry->GetTotalSpace());
}

void TChaosCache::OnRemoved(const TChaosCacheEntryPtr& entry)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TAsyncSlruCacheBase::OnRemoved(entry);

    const auto& key = entry->GetKey();
    YT_LOG_DEBUG("Cache entry removed (Key: %v, Success: %v, TotalSpace: %v)",
        key,
        entry->GetSuccess(),
        entry->GetTotalSpace());
}

i64 TChaosCache::GetWeight(const TChaosCacheEntryPtr& entry) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return entry->GetTotalSpace();
}

bool TChaosCache::IsExpired(
    const TChaosCacheEntryPtr& entry,
    TDuration successExpirationTime,
    TDuration failureExpirationTime)
{
    return
        TInstant::Now() > entry->GetTimestamp() +
        (entry->GetSuccess() ? successExpirationTime : failureExpirationTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
