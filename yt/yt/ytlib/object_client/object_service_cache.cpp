#include "config.h"

#include "object_service_cache.h"
#include "object_service_proxy.h"

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/string.h>
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

TObjectServiceCacheKey::TObjectServiceCacheKey(
    TCellTag cellTag,
    TString user,
    TYPath path,
    TString service,
    TString method,
    TSharedRef requestBody)
    : CellTag(std::move(cellTag))
    , User(std::move(user))
    , Path(std::move(path))
    , Service(std::move(service))
    , Method(std::move(method))
    , RequestBody(std::move(requestBody))
    , RequestBodyHash(GetChecksum(RequestBody))
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
        TRef::AreBitwiseEqual(RequestBody, other.RequestBody);
}

void FormatValue(TStringBuilderBase* builder, const TObjectServiceCacheKey& key, TStringBuf /*format*/)
{
    builder->AppendFormat("{%v %v %v.%v %v %x}",
        key.CellTag,
        key.User,
        key.Service,
        key.Method,
        key.Path,
        key.RequestBodyHash);
}

////////////////////////////////////////////////////////////////////////////////

TObjectServiceCacheEntry::TObjectServiceCacheEntry(
    const TObjectServiceCacheKey& key,
    bool success,
    NHydra::TRevision revision,
    TInstant timestamp,
    TSharedRefArray responseMessage,
    double byteRate,
    TInstant lastUpdateTime)
    : TAsyncCacheValueBase(key)
    , Success_(success)
    , ResponseMessage_(std::move(responseMessage))
    , TotalSpace_(GetByteSize(ResponseMessage_))
    , Timestamp_(timestamp)
    , Revision_(revision)
    , ByteRate_(byteRate)
    , LastUpdateTime_(lastUpdateTime)
{ }

void TObjectServiceCacheEntry::IncrementRate()
{
    auto guard = Guard(Lock_);

    auto now = TInstant::Now();
    auto sinceLast = (now - LastUpdateTime_).SecondsFloat();
    auto w = Exp2(-2. * sinceLast);

    if (sinceLast > 0.01) {
        auto average = TotalSpace_ * 1. / sinceLast;
        ByteRate_ = w * ByteRate_ + (1 - w) * average;
    } else {
        constexpr auto c = 1.386; // 2 * ln2
        ByteRate_ = w * ByteRate_ + (1 - sinceLast * c / 2) * c * TotalSpace_;
    }
    LastUpdateTime_ = now;
}

double TObjectServiceCacheEntry::GetByteRate() const
{
    return ByteRate_;
}

TInstant TObjectServiceCacheEntry::GetLastUpdateTime() const
{
    return LastUpdateTime_;
}

////////////////////////////////////////////////////////////////////////////////

TCacheProfilingCounters::TCacheProfilingCounters(const NProfiling::TProfiler& profiler)
    : HitRequestCount(profiler.Counter("/hit_request_count"))
    , HitResponseBytes(profiler.Counter("/hit_response_bytes"))
    , MissRequestCount(profiler.Counter("/miss_request_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

TObjectServiceCache::TObjectServiceCache(
    TObjectServiceCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryTracker,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : TMemoryTrackingAsyncSlruCacheBase(
        config,
        std::move(memoryTracker))
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
    NHydra::TRevision refreshRevision)
{
    auto entry = Find(key);
    auto tryRemove = [&] () {
        {
            auto guard = WriterGuard(ExpiredEntriesLock_);
            ExpiredEntries_.emplace(key, entry);
        }

        TryRemove(entry);
    };

    bool cacheHit = false;
    if (entry) {
        if (refreshRevision && entry->GetRevision() != NHydra::NullRevision && entry->GetRevision() <= refreshRevision) {
            YT_LOG_DEBUG("Cache entry refresh requested (RequestId: %v, Key: %v, Revision: %llx, Success: %v)",
                requestId,
                key,
                entry->GetRevision(),
                entry->GetSuccess());

            tryRemove();
        } else if (IsExpired(entry, expireAfterSuccessfulUpdateTime, expireAfterFailedUpdateTime)) {
            YT_LOG_DEBUG("Cache entry expired (RequestId: %v, Key: %v, Revision: %llx, Success: %v)",
                requestId,
                key,
                entry->GetRevision(),
                entry->GetSuccess());

            tryRemove();
        } else {
            cacheHit = true;
            YT_LOG_DEBUG("Cache hit (RequestId: %v, Key: %v, Revision: %llx, Success: %v)",
                requestId,
                key,
                entry->GetRevision(),
                entry->GetSuccess());
        }

        TouchEntry(entry);
    } else {
        auto guard = ReaderGuard(ExpiredEntriesLock_);

        if (auto it = ExpiredEntries_.find(key); it != ExpiredEntries_.end()) {
            TouchEntry(it->second);
        }
    }

    auto counters = GetProfilingCounters(key.User, key.Method);
    if (cacheHit) {
        counters->HitRequestCount.Increment();
        counters->HitResponseBytes.Increment(entry->GetTotalSpace());
    } else {
        counters->MissRequestCount.Increment();
    }

    return BeginInsert(key);
}

void TObjectServiceCache::EndLookup(
    NRpc::TRequestId requestId,
    TCookie cookie,
    const TSharedRefArray& responseMessage,
    NHydra::TRevision revision,
    bool success)
{
    const auto& key = cookie.GetKey();

    YT_LOG_DEBUG("Cache population request succeeded (RequestId: %v, Key: %v, Revision: %llx, Success: %v)",
        requestId,
        key,
        revision,
        success);

    auto rate = 0.0;
    auto lastUpdateTime = TInstant::Now();
    {
        auto guard = WriterGuard(ExpiredEntriesLock_);

        if (auto it = ExpiredEntries_.find(key); it != ExpiredEntries_.end()) {
            const auto& expiredEntry = it->second;
            rate = expiredEntry->GetByteRate();
            lastUpdateTime = expiredEntry->GetLastUpdateTime();
            ExpiredEntries_.erase(it);
        }
    }

    auto entry = New<TObjectServiceCacheEntry>(
        key,
        success,
        revision,
        TInstant::Now(),
        responseMessage,
        rate,
        lastUpdateTime);
    TouchEntry(entry);

    cookie.EndInsert(entry);
}

IYPathServicePtr TObjectServiceCache::GetOrchidService()
{
    auto producer = BIND(&TObjectServiceCache::DoBuildOrchid, MakeStrong(this));
    return IYPathService::FromProducer(producer);
}

void TObjectServiceCache::Reconfigure(const TObjectServiceCacheDynamicConfigPtr& config)
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

    TAsyncSlruCacheBase::OnAdded(entry);

    const auto& key = entry->GetKey();
    YT_LOG_DEBUG("Cache entry added (Key: %v, Revision: %llx, Success: %v, TotalSpace: %v)",
        key,
        entry->GetRevision(),
        entry->GetSuccess(),
        entry->GetTotalSpace());
}

void TObjectServiceCache::OnRemoved(const TObjectServiceCacheEntryPtr& entry)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TAsyncSlruCacheBase::OnRemoved(entry);

    const auto& key = entry->GetKey();
    YT_LOG_DEBUG("Cache entry removed (Key: %v, Revision: %llx, Success: %v, TotalSpace: %v)",
        key,
        entry->GetRevision(),
        entry->GetSuccess(),
        entry->GetTotalSpace());

    auto guard = ReaderGuard(ExpiredEntriesLock_);

    if (!ExpiredEntries_.contains(key)) {
        auto guard = WriterGuard(TopEntriesLock_);
        if (TopEntries_.erase(key) > 0) {
            YT_LOG_DEBUG("Removed entry from top (Key: %v)", key);
        }
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

void TObjectServiceCache::TouchEntry(const TObjectServiceCacheEntryPtr& entry)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& key = entry->GetKey();

    auto previous = entry->GetByteRate();
    entry->IncrementRate();
    auto current = entry->GetByteRate();

    auto topEntryByteRateThreshold = TopEntryByteRateThreshold_.load();
    if (previous < topEntryByteRateThreshold && current >= topEntryByteRateThreshold) {
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

    if (previous >= topEntryByteRateThreshold && current < topEntryByteRateThreshold) {
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
    std::vector<std::pair<TObjectServiceCacheKey, TObjectServiceCacheEntryPtr>> top;
    {
        auto guard = ReaderGuard(TopEntriesLock_);
        top = {TopEntries_.begin(), TopEntries_.end()};
    }

    std::sort(top.begin(), top.end(), [] (const auto& rhs, const auto& lhs) {
        return rhs.second->GetByteRate() > lhs.second->GetByteRate();
    });

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("top_requests")
                .DoListFor(top, [&] (auto fluent, const auto& item) {
                    const auto& [key, entry] = item;
                    fluent
                        .Item().BeginMap()
                            .Item("cell_tag").Value(key.CellTag)
                            .Item("user").Value(key.User)
                            .Item("service").Value(key.Service)
                            .Item("method").Value(key.Method)
                            .Item("path").Value(key.Path)
                            .Item("request_body_hash").Value(key.RequestBodyHash)
                            .Item("byte_rate").Value(entry->GetByteRate())
                        .EndMap();
                })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
