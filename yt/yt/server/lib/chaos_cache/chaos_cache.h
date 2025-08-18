#pragma once

#include "public.h"

#include <yt/yt/server/lib/chaos_cache/public.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

struct TChaosCacheKey
{
    NChaosClient::TReplicationCardId CardId;
    NChaosClient::TReplicationCardFetchOptions FetchOptions;

    operator size_t() const;
    bool operator == (const TChaosCacheKey& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TChaosCacheKey& key, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TChaosCacheEntry)

class TChaosCacheEntry
    : public TAsyncCacheValueBase<TChaosCacheKey, TChaosCacheEntry>
{
public:
    TChaosCacheEntry(
        const TChaosCacheKey& key,
        TInstant timestamp,
        TErrorOr<NChaosClient::TReplicationCardPtr> replicationCard);

    DEFINE_BYVAL_RO_PROPERTY(i64, TotalSpace);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, Timestamp);
    DEFINE_BYVAL_RO_PROPERTY(TErrorOr<NChaosClient::TReplicationCardPtr>, ReplicationCard);

    bool GetSuccess() const;
};

DEFINE_REFCOUNTED_TYPE(TChaosCacheEntry)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCacheProfilingCounters)

struct TCacheProfilingCounters
    : public TRefCounted
{
    explicit TCacheProfilingCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter HitRequestCount;
    NProfiling::TCounter HitResponseBytes;
    NProfiling::TCounter MissRequestCount;
};

DEFINE_REFCOUNTED_TYPE(TCacheProfilingCounters)

////////////////////////////////////////////////////////////////////////////////

class TChaosCache
    : public TAsyncSlruCacheBase<TChaosCacheKey, TChaosCacheEntry>
{
public:
    TChaosCache(
        NChaosCache::TChaosCacheConfigPtr config,
        const NProfiling::TProfiler& profiler,
        const NLogging::TLogger& logger);

    using TCookie = TAsyncSlruCacheBase<TChaosCacheKey, TChaosCacheEntry>::TInsertCookie;
    TCookie BeginLookup(
        NRpc::TRequestId requestId,
        const TChaosCacheKey& key,
        TDuration successExpirationTime,
        TDuration failureExpirationTime,
        NChaosClient::TReplicationEra refreshEra,
        const std::string& user);

    void EndLookup(
        NRpc::TRequestId requestId,
        TCookie cookie,
        TErrorOr<NChaosClient::TReplicationCardPtr> replicationCard);

private:
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);

    THashMap<TString, TCacheProfilingCountersPtr> UserToCounters_;

    TCacheProfilingCountersPtr GetProfilingCounters(const std::string& user);

    bool IsResurrectionSupported() const override;

    void OnAdded(const TChaosCacheEntryPtr& entry) override;
    void OnRemoved(const TChaosCacheEntryPtr& entry) override;
    i64 GetWeight(const TChaosCacheEntryPtr& entry) const override;

    static bool IsExpired(
        const TChaosCacheEntryPtr& entry,
        TDuration successExpirationTime,
        TDuration failureExpirationTime);
};

DEFINE_REFCOUNTED_TYPE(TChaosCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
