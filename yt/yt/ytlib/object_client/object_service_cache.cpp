#include "config.h"

#include "object_service_cache.h"
#include "object_service_proxy.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/throttling_channel.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NObjectClient {

using namespace NConcurrency;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TCachedObjectServiceResponseTag
{ };

////////////////////////////////////////////////////////////////////////////////

TObjectServiceCacheKey::TObjectServiceCacheKey(
    TCellTag cellTag,
    TString user,
    TYPath path,
    TString service,
    TString method,
    TSharedRef requestBody,
    bool suppressUpstreamSync,
    bool suppressTransactionCoordinatorSync)
    : CellTag(std::move(cellTag))
    , User(std::move(user))
    , Path(std::move(path))
    , Service(std::move(service))
    , Method(std::move(method))
    , RequestBody(std::move(requestBody))
    , RequestBodyHash(GetChecksum(RequestBody))
    , SuppressUpstreamSync(suppressUpstreamSync)
    , SuppressTransactionCoordinatorSync(suppressTransactionCoordinatorSync)
{ }

TObjectServiceCacheKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, CellTag);
    HashCombine(result, User);
    HashCombine(result, Path);
    HashCombine(result, Service);
    HashCombine(result, Method);
    HashCombine(result, RequestBodyHash);
    HashCombine(result, SuppressUpstreamSync);
    HashCombine(result, SuppressTransactionCoordinatorSync);
    return result;
}

bool TObjectServiceCacheKey::operator == (const TObjectServiceCacheKey& other) const
{
    return
        CellTag == other.CellTag &&
        User == other.User &&
        Path == other.Path &&
        Service == other.Service &&
        Method == other.Method &&
        RequestBodyHash == other.RequestBodyHash &&
        TRef::AreBitwiseEqual(RequestBody, other.RequestBody) &&
        SuppressUpstreamSync == other.SuppressUpstreamSync &&
        SuppressTransactionCoordinatorSync == other.SuppressTransactionCoordinatorSync;
}

i64 TObjectServiceCacheKey::ComputeExtraSpace() const
{
    return
        User.length() +
        Path.length() +
        Service.length() +
        Method.length() +
        RequestBody.Size();
}

void FormatValue(TStringBuilderBase* builder, const TObjectServiceCacheKey& key, TStringBuf /*format*/)
{
    builder->AppendFormat("{%v %v %v.%v %v %zx %v %v}",
        key.CellTag,
        key.User,
        key.Service,
        key.Method,
        key.Path,
        key.RequestBodyHash,
        key.SuppressUpstreamSync,
        key.SuppressTransactionCoordinatorSync);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceCacheEntry::TObjectServiceCacheEntry(
    const TObjectServiceCacheKey& key,
    bool success,
    NHydra::TRevision revision,
    TInstant timestamp,
    TSharedRefArray responseMessage,
    TAverageHistoricUsageAggregator byteRateAggregator)
    : TAsyncCacheValueBase(key)
    , Success_(success)
    , ResponseMessage_(std::move(responseMessage))
    , Timestamp_(timestamp)
    , Revision_(revision)
    , ByteRateAggregator_(std::move(byteRateAggregator))
{
    TotalSpace_ = sizeof(*this) + ComputeExtraSpace();
}

void TObjectServiceCacheEntry::UpdateByteRateOnRequest()
{
    auto guard = Guard(Lock_);
    ByteRateAggregator_.UpdateAt(GetInstant(), TotalSpace_);
}

double TObjectServiceCacheEntry::GetByteRate() const
{
    auto guard = Guard(Lock_);
    return ByteRateAggregator_.GetHistoricUsage();
}

TAverageHistoricUsageAggregator TObjectServiceCacheEntry::GetByteRateAggregator() const
{
    auto guard = Guard(Lock_);
    return ByteRateAggregator_;
}

i64 TObjectServiceCacheEntry::ComputeExtraSpace() const
{
    return
        ResponseMessage_.ByteSize() +
        GetKey().ComputeExtraSpace();
}

////////////////////////////////////////////////////////////////////////////////

TCacheProfilingCounters::TCacheProfilingCounters(const NProfiling::TProfiler& profiler)
    : HitRequestCount(profiler.Counter("/hit_request_count"))
    , HitResponseBytes(profiler.Counter("/hit_response_bytes"))
    , MissRequestCount(profiler.Counter("/miss_request_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

TObjectServiceCache::TCookie::TCookie(TUnderlyingCookie&& underlyingCookie, TObjectServiceCacheEntryPtr expiredEntry)
    : TUnderlyingCookie(std::move(underlyingCookie))
    , ExpiredEntry_(std::move(expiredEntry))
{ }

const TObjectServiceCacheEntryPtr& TObjectServiceCache::TCookie::ExpiredEntry() const
{
    return ExpiredEntry_;
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceCache::TObjectServiceCache(
    TObjectServiceCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryTracker,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : TMemoryTrackingAsyncSlruCacheBase(
        config,
        std::move(memoryTracker),
        profiler)
    , Config_(std::move(config))
    , Logger(logger)
    , Profiler_(profiler.WithSparse())
    , TopEntryByteRateThreshold_(Config_->TopEntryByteRateThreshold)
{ }

TObjectServiceCache::TCookie TObjectServiceCache::BeginLookup(
    TRequestId requestId,
    const TObjectServiceCacheKey& key,
    TDuration expireAfterSuccessfulUpdateTime,
    TDuration expireAfterFailedUpdateTime,
    TDuration successStalenessBound,
    NHydra::TRevision refreshRevision)
{
    auto entry = Find(key);
    auto tryRemove = [&] () {
        {
            auto guard = WriterGuard(ExpiredEntriesLock_);
            ExpiredEntries_.emplace(key, entry);
        }

        TryRemoveValue(entry);
    };

    bool cacheHit = false;
    if (entry) {
        if (refreshRevision && entry->GetRevision() != NHydra::NullRevision && entry->GetRevision() <= refreshRevision) {
            YT_LOG_DEBUG("Cache entry refresh requested (RequestId: %v, Key: %v, Revision: %x, Success: %v)",
                requestId,
                key,
                entry->GetRevision(),
                entry->GetSuccess());

            tryRemove();
        } else if (IsExpired(entry, expireAfterSuccessfulUpdateTime, expireAfterFailedUpdateTime)) {
            YT_LOG_DEBUG("Cache entry expired (RequestId: %v, Key: %v, Revision: %x, Success: %v)",
                requestId,
                key,
                entry->GetRevision(),
                entry->GetSuccess());

            tryRemove();
        } else {
            cacheHit = true;
            YT_LOG_DEBUG("Cache hit (RequestId: %v, Key: %v, Revision: %x, Success: %v)",
                requestId,
                key,
                entry->GetRevision(),
                entry->GetSuccess());
        }

        TouchEntry(entry);
    } else {
        auto guard = ReaderGuard(ExpiredEntriesLock_);

        if (auto it = ExpiredEntries_.find(key); it != ExpiredEntries_.end()) {
            entry = it->second;
            TouchEntry(entry);
        }
    }

    auto counters = GetProfilingCounters(key.User, key.Method);
    if (cacheHit) {
        counters->HitRequestCount.Increment();
        counters->HitResponseBytes.Increment(entry->GetTotalSpace());
    } else {
        counters->MissRequestCount.Increment();
    }

    auto underlyingCookie = BeginInsert(key);

    if (underlyingCookie.GetValue().IsSet()) {
        // Do not return stale response, when actual one is available.
        entry = nullptr;
    } else if (entry && entry->GetSuccess()) {
        // Verify stale response validity.
        if ((TInstant::Now() > entry->GetTimestamp() + successStalenessBound) ||
            (refreshRevision && entry->GetRevision() != NHydra::NullRevision && entry->GetRevision() <= refreshRevision))
        {
            entry = nullptr;
        }
    } else {
        // Do not return stale errors.
        entry = nullptr;
    }

    return TCookie(std::move(underlyingCookie), std::move(entry));
}

void TObjectServiceCache::EndLookup(
    NRpc::TRequestId requestId,
    TCookie cookie,
    const TSharedRefArray& responseMessage,
    NHydra::TRevision revision,
    bool success)
{
    const auto& key = cookie.GetKey();

    YT_LOG_DEBUG("Cache population request succeeded (RequestId: %v, Key: %v, Revision: %x, Success: %v)",
        requestId,
        key,
        revision,
        success);

    std::optional<TAverageHistoricUsageAggregator> byteRateAggregator;
    {
        auto guard = WriterGuard(ExpiredEntriesLock_);

        if (auto it = ExpiredEntries_.find(key); it != ExpiredEntries_.end()) {
            const auto& expiredEntry = it->second;
            byteRateAggregator.emplace(expiredEntry->GetByteRateAggregator());
            ExpiredEntries_.erase(it);
        }
    }
    MaybeEraseTopEntry(key);

    if (!byteRateAggregator) {
        byteRateAggregator.emplace();
        byteRateAggregator->UpdateParameters(THistoricUsageAggregationParameters(
            EHistoricUsageAggregationMode::ExponentialMovingAverage,
            1.0 / 60.0));
    }

    auto entry = New<TObjectServiceCacheEntry>(
        key,
        success,
        revision,
        TInstant::Now(),
        TSharedRefArray::MakeCopy(responseMessage, GetRefCountedTypeCookie<TCachedObjectServiceResponseTag>()),
        *byteRateAggregator);
    TouchEntry(entry, /*forceRenewTop*/ true);

    cookie.EndInsert(entry);
}

IYPathServicePtr TObjectServiceCache::GetOrchidService()
{
    auto producer = BIND(&TObjectServiceCache::DoBuildOrchid, MakeStrong(this));
    return IYPathService::FromProducer(producer);
}

void TObjectServiceCache::Configure(const TObjectServiceCacheDynamicConfigPtr& config)
{
    TMemoryTrackingAsyncSlruCacheBase::Reconfigure(config);
    TopEntryByteRateThreshold_.store(config->TopEntryByteRateThreshold.value_or(
        Config_->TopEntryByteRateThreshold));
}

TCacheProfilingCountersPtr TObjectServiceCache::GetProfilingCounters(const TString& user, const TString& method)
{
    auto key = std::make_tuple(user, method);

    {
        auto guard = ReaderGuard(Lock_);
        if (auto it = KeyToCounters_.find(key)) {
            return it->second;
        }
    }

    auto counters = New<TCacheProfilingCounters>(Profiler_
        .WithTag("user", user)
        .WithTag("method", method));

    {
        auto guard = WriterGuard(Lock_);
        auto [it, inserted] = KeyToCounters_.emplace(key, std::move(counters));
        return it->second;
    }
}

bool TObjectServiceCache::IsResurrectionSupported() const
{
    return false;
}

void TObjectServiceCache::OnAdded(const TObjectServiceCacheEntryPtr& entry)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TMemoryTrackingAsyncSlruCacheBase::OnAdded(entry);

    const auto& key = entry->GetKey();
    YT_LOG_DEBUG("Cache entry added (Key: %v, Revision: %x, Success: %v, TotalSpace: %v)",
        key,
        entry->GetRevision(),
        entry->GetSuccess(),
        entry->GetTotalSpace());
}

void TObjectServiceCache::OnRemoved(const TObjectServiceCacheEntryPtr& entry)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TMemoryTrackingAsyncSlruCacheBase::OnRemoved(entry);

    const auto& key = entry->GetKey();
    YT_LOG_DEBUG("Cache entry removed (Key: %v, Revision: %x, Success: %v, TotalSpace: %v)",
        key,
        entry->GetRevision(),
        entry->GetSuccess(),
        entry->GetTotalSpace());

    auto guard = ReaderGuard(ExpiredEntriesLock_);

    if (!ExpiredEntries_.contains(key)) {
        MaybeEraseTopEntry(key);
    }
}

void TObjectServiceCache::MaybeEraseTopEntry(const TObjectServiceCacheKey& key)
{
    auto guard = WriterGuard(TopEntriesLock_);
    if (TopEntries_.erase(key) > 0) {
        YT_LOG_DEBUG("Removed entry from top (Key: %v)", key);
    }
}

i64 TObjectServiceCache::GetWeight(const TObjectServiceCacheEntryPtr& entry) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return entry->GetTotalSpace();
}

bool TObjectServiceCache::IsExpired(
    const TObjectServiceCacheEntryPtr& entry,
    TDuration expireAfterSuccessfulUpdateTime,
    TDuration expireAfterFailedUpdateTime)
{
    return
        TInstant::Now() > entry->GetTimestamp() +
        (entry->GetSuccess() ? expireAfterSuccessfulUpdateTime : expireAfterFailedUpdateTime);
}

void TObjectServiceCache::TouchEntry(const TObjectServiceCacheEntryPtr& entry, bool forceRenewTop)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& key = entry->GetKey();

    auto previous = entry->GetByteRate();
    entry->UpdateByteRateOnRequest();
    auto current = entry->GetByteRate();

    auto topEntryByteRateThreshold = TopEntryByteRateThreshold_.load();
    if ((previous < topEntryByteRateThreshold && current >= topEntryByteRateThreshold) || forceRenewTop) {
        auto guard = WriterGuard(TopEntriesLock_);

        if (entry->GetByteRate() >= topEntryByteRateThreshold) {
            if (TopEntries_.emplace(key, entry).second) {
                YT_LOG_DEBUG("Added entry to top (Key: %v, ByteRate: %v -> %v)",
                    key,
                    previous,
                    current);
            }
        }
    }

    if ((previous >= topEntryByteRateThreshold && current < topEntryByteRateThreshold) || forceRenewTop) {
        auto guard = WriterGuard(TopEntriesLock_);

        if (entry->GetByteRate() < topEntryByteRateThreshold) {
            if (TopEntries_.erase(key) > 0) {
                YT_LOG_DEBUG("Removed entry from top (Key: %v, ByteRate: %v -> %v)",
                    key,
                    previous,
                    current);
            }
        }
    }
}

void TObjectServiceCache::DoBuildOrchid(IYsonConsumer* consumer)
{
    struct TFrozenEntry
    {
        TObjectServiceCacheKey Key;
        double ByteRate;
    };

    std::vector<TFrozenEntry> top;
    {
        auto guard = ReaderGuard(TopEntriesLock_);
        for (const auto& [key, entry] : TopEntries_) {
            top.push_back({
                .Key = key,
                .ByteRate = entry->GetByteRate()
            });
        }
    }

    std::sort(top.begin(), top.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.ByteRate > rhs.ByteRate;
    });

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("top_requests")
                .DoListFor(top, [&] (auto fluent, const auto& item) {
                    fluent
                        .Item().BeginMap()
                            .Item("cell_tag").Value(item.Key.CellTag)
                            .Item("user").Value(item.Key.User)
                            .Item("service").Value(item.Key.Service)
                            .Item("method").Value(item.Key.Method)
                            .Item("path").Value(item.Key.Path)
                            .Item("request_body_hash").Value(item.Key.RequestBodyHash)
                            .Item("byte_rate").Value(item.ByteRate)
                        .EndMap();
                })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
