#include "config.h"

#include "object_service_cache.h"

#include <yt/core/profiling/profile_manager.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/string.h>
#include <yt/core/misc/checksum.h>

#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/throttling_channel.h>

#include <yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NObjectServer {

using namespace NConcurrency;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NObjectClient;

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
    TSharedRefArray responseMessage)
    : TAsyncCacheValueBase(key)
    , Success_(success)
    , ResponseMessage_(std::move(responseMessage))
    , TotalSpace_(GetByteSize(ResponseMessage_))
    , Timestamp_(timestamp)
    , Revision_(revision)
{ }

////////////////////////////////////////////////////////////////////////////////

TCacheProfilingCounters::TCacheProfilingCounters(const NProfiling::TTagIdList& tagIds)
    : HitRequestCount("/hit_request_count", tagIds)
    , HitResponseBytes("/hit_response_bytes", tagIds)
    , MissRequestCount("/miss_request_count", tagIds)
{ }

////////////////////////////////////////////////////////////////////////////////

TObjectServiceCache::TObjectServiceCache(
    TObjectServiceCacheConfigPtr config,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : TAsyncSlruCacheBase(config)
    , Logger(logger)
    , Profiler_(profiler)
{ }

TObjectServiceCache::TCookie TObjectServiceCache::BeginLookup(
    TRequestId requestId,
    const TObjectServiceCacheKey& key,
    TDuration successExpirationTime,
    TDuration failureExpirationTime,
    NHydra::TRevision refreshRevision)
{
    auto entry = Find(key);
    bool cacheHit = false;
    if (entry) {
        if (refreshRevision && entry->GetRevision() != NHydra::NullRevision && entry->GetRevision() <= refreshRevision) {
            YT_LOG_DEBUG("Cache entry refresh requested (RequestId: %v, Key: %v, Revision: %llx, Success: %v)",
                requestId,
                key,
                entry->GetRevision(),
                entry->GetSuccess());

            TryRemove(entry);

        } else if (IsExpired(entry, successExpirationTime, failureExpirationTime)) {
            YT_LOG_DEBUG("Cache entry expired (RequestId: %v, Key: %v, Revision: %llx, Success: %v)",
                requestId,
                key,
                entry->GetRevision(),
                entry->GetSuccess());

            TryRemove(entry);

        } else {
            cacheHit = true;
            YT_LOG_DEBUG("Cache hit (RequestId: %v, Key: %v, Revision: %llx, Success: %v)",
                requestId,
                key,
                entry->GetRevision(),
                entry->GetSuccess());
        }
    }

    auto counters = GetProfilingCounters(key.User, key.Method);
    if (cacheHit) {
        Profiler_.Increment(counters->HitRequestCount);
        Profiler_.Increment(counters->HitResponseBytes, entry->GetTotalSpace());
    } else {
        Profiler_.Increment(counters->MissRequestCount);
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

    auto entry = New<TObjectServiceCacheEntry>(
        key,
        success,
        revision,
        TInstant::Now(),
        responseMessage);

    cookie.EndInsert(entry);
}

TCacheProfilingCountersPtr TObjectServiceCache::GetProfilingCounters(const TString& user, const TString& method)
{
    auto key = std::make_tuple(user, method);

    {
        NConcurrency::TReaderGuard guard(Lock_);
        if (auto it = KeyToCounters_.find(key)) {
            return it->second;
        }
    }

    NProfiling::TTagIdList tagIds{
        NProfiling::TProfileManager::Get()->RegisterTag("user", user),
        NProfiling::TProfileManager::Get()->RegisterTag("method", method)
    };
    auto counters = New<TCacheProfilingCounters>(tagIds);

    {
        NConcurrency::TWriterGuard guard(Lock_);
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
}

i64 TObjectServiceCache::GetWeight(const TObjectServiceCacheEntryPtr& entry) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return entry->GetTotalSpace();
}

bool TObjectServiceCache::IsExpired(
    const TObjectServiceCacheEntryPtr& entry,
    TDuration successExpirationTime,
    TDuration failureExpirationTime)
{
    return
        TInstant::Now() > entry->GetTimestamp() +
        (entry->GetSuccess() ? successExpirationTime : failureExpirationTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
