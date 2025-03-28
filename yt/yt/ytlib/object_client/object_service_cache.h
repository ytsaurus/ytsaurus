#pragma once

#include "private.h"

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/adjusted_exponential_moving_average.h>

#include <yt/yt/client/hydra/public.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/ymath.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

struct TObjectServiceCacheKey
{
    TCellTag CellTag = InvalidCellTag;
    std::string User;
    NYPath::TYPath Path;
    TString Service;
    TString Method;
    TSharedRef RequestBody;
    size_t RequestBodyHash;
    bool SuppressUpstreamSync = false;
    bool SuppressTransactionCoordinatorSync = false;

    TObjectServiceCacheKey() = default;

    TObjectServiceCacheKey(
        TCellTag cellTag,
        const std::string& user,
        NYPath::TYPath path,
        TString service,
        TString method,
        TSharedRef requestBody,
        bool suppressUpstreamSync,
        bool suppressTransactionCoordinatorSync);

    operator size_t() const;
    bool operator == (const TObjectServiceCacheKey& other) const;

    i64 ComputeExtraSpace() const;
};

void FormatValue(TStringBuilderBase* builder, const TObjectServiceCacheKey& key, TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceCacheEntry
    : public TAsyncCacheValueBase<TObjectServiceCacheKey, TObjectServiceCacheEntry>
{
public:
    TObjectServiceCacheEntry(
        const TObjectServiceCacheKey& key,
        bool success,
        NHydra::TRevision revision,
        TInstant timestamp,
        TSharedRefArray responseMessage,
        TDuration aggregationPeriod,
        const TObjectServiceCacheEntryPtr& expiredEntry);

    DEFINE_BYVAL_RO_PROPERTY(bool, Success);
    DEFINE_BYVAL_RO_PROPERTY(TSharedRefArray, ResponseMessage);
    DEFINE_BYVAL_RO_PROPERTY(i64, TotalSpace);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, Timestamp);
    DEFINE_BYVAL_RO_PROPERTY(NHydra::TRevision, Revision);

    i64 GetByteRate() const;
    i64 GetTotalByteRate() const;

private:
    friend class TObjectServiceCache;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ByteRateAggregatorLock_);
    mutable TAverageAdjustedExponentialMovingAverage ByteRateAggregator_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TotalByteRateAggregatorLock_);
    mutable TAverageAdjustedExponentialMovingAverage TotalByteRateAggregator_;

    i64 ComputeExtraSpace() const;

    void UpdateByteRate();
    void UpdateTotalByteRate(int stickyGroupSize);
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceCacheEntry)

////////////////////////////////////////////////////////////////////////////////

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

class TObjectServiceCache
    : public TRefCounted
{
public:
    using TUnderlyingCookie = TAsyncSlruCacheBase<TObjectServiceCacheKey, TObjectServiceCacheEntry>::TInsertCookie;

    class TCookie
        : public TUnderlyingCookie
    {
    public:
        TCookie() = default;
        TCookie(TUnderlyingCookie&& underlyingCookie, TObjectServiceCacheEntryPtr expiredEntry);

        TCookie(TCookie&& other);
        TCookie(const TCookie& other) = delete;

        TCookie& operator=(TCookie&& other);
        TCookie& operator=(const TCookie& other) = delete;

        const TObjectServiceCacheEntryPtr& ExpiredEntry() const;

    private:
        TObjectServiceCacheEntryPtr ExpiredEntry_;
    };

    TObjectServiceCache(
        TObjectServiceCacheConfigPtr config,
        IMemoryUsageTrackerPtr memoryTracker,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler);

    TCookie BeginLookup(
        NRpc::TRequestId requestId,
        const TObjectServiceCacheKey& key,
        TDuration expireAfterSuccessfulUpdateTime,
        TDuration expireAfterFailedUpdateTime,
        TDuration successStalenessBound,
        NHydra::TRevision refreshRevision);

    void EndLookup(
        NRpc::TRequestId requestId,
        TCookie cookie,
        const TSharedRefArray& responseMessage,
        NHydra::TRevision revision,
        bool success);

    void UpdateAdvisedEntryStickyGroupSize(const TObjectServiceCacheEntryPtr& entry, int currentSize);
    int GetAdvisedEntryStickyGroupSize(const TObjectServiceCacheEntryPtr& entry);

    NYTree::IYPathServicePtr GetOrchidService();

    void Reconfigure(const TObjectServiceCacheDynamicConfigPtr& config);

private:
    class TCache
        : public TMemoryTrackingAsyncSlruCacheBase<TObjectServiceCacheKey, TObjectServiceCacheEntry>
    {
    public:
        TCache(
            TObjectServiceCache* owner,
            TObjectServiceCacheConfigPtr config,
            IMemoryUsageTrackerPtr memoryTracker,
            const NProfiling::TProfiler& profiler);

    private:
        TWeakPtr<TObjectServiceCache> const Owner_;

        bool IsResurrectionSupported() const override;
        void OnAdded(const TObjectServiceCacheEntryPtr& entry) override;
        void OnRemoved(const TObjectServiceCacheEntryPtr& entry) override;
        i64 GetWeight(const TObjectServiceCacheEntryPtr& entry) const override;
    };

    const TIntrusivePtr<TCache> Cache_;

    const TObjectServiceCacheConfigPtr Config_;
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler_;

    std::atomic<i64> EntryByteRateLimit_;
    std::atomic<i64> TopEntryByteRateThreshold_;
    std::atomic<TDuration> AggregationPeriod_;
    std::atomic<int> MinAdvisedStickyGroupSize_;
    std::atomic<int> MaxAdvisedStickyGroupSize_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);

    using TProfilingCountersKey = std::tuple<std::string, TString>;
    THashMap<TProfilingCountersKey, TCacheProfilingCountersPtr> KeyToCounters_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ExpiredEntriesLock_);
    THashMap<TObjectServiceCacheKey, TObjectServiceCacheEntryPtr> ExpiredEntries_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, TopEntriesLock_);
    THashMap<TObjectServiceCacheKey, TObjectServiceCacheEntryPtr> TopEntries_;

    TCacheProfilingCountersPtr GetProfilingCounters(const std::string& user, const TString& method);

    void MaybeEraseTopEntry(const TObjectServiceCacheKey& key);

    void OnAdded(const TObjectServiceCacheEntryPtr& entry);
    void OnRemoved(const TObjectServiceCacheEntryPtr& entry);

    static bool IsExpired(
        const TObjectServiceCacheEntryPtr& entry,
        TDuration expireAfterSuccessfulUpdateTime,
        TDuration expireAfterFailedUpdateTime);

    void TouchEntry(const TObjectServiceCacheEntryPtr& entry, bool forceRenewTop = false);
    void DoBuildOrchid(NYson::IYsonConsumer* consumer);
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
