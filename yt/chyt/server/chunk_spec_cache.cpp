#include "chunk_spec_cache.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NHydra;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

struct TTableKey
{
    TObjectId ObjectId;
    //! Fields below are merely a payload, they are not used for comparison.
    TCellTag ExternalCellTag;
    i64 ChunkCount;
    TRevision ContentRevision;
    i64 ChunkMergerRevision;
};

bool operator==(const TTableKey& lhs, const TTableKey& rhs)
{
    return lhs.ObjectId == rhs.ObjectId;
}

void FormatValue(TStringBuilderBase* builder, const TTableKey& key, TStringBuf spec)
{
    FormatValue(builder, Format("#%v@%v/%v", key.ObjectId, key.ContentRevision, key.ChunkMergerRevision), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NClickHouseServer::TTableKey>
{
    size_t operator()(const NYT::NClickHouseServer::TTableKey& key) const
    {
        return THash<NYT::NObjectClient::TObjectId>()(key.ObjectId);
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TChunkSpecCacheEntry)

class TChunkSpecCacheEntry
    : public TAsyncCacheValueBase<TTableKey, TChunkSpecCacheEntry>
{
public:
    TChunkSpecCacheEntry(
        TTableKey key,
        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs)
        : TAsyncCacheValueBase(key)
        , ChunkSpecs(std::move(chunkSpecs))
        , Weight_(ComputeWeight(ChunkSpecs))
    { }

    TRevision GetContentRevision() const
    {
        return GetKey().ContentRevision;
    }

    i64 GetChunkMergerRevision() const
    {
        return GetKey().ChunkMergerRevision;
    }

    i64 GetWeight() const
    {
        return Weight_;
    }

    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;

private:
    const i64 Weight_;

    static i64 ComputeWeight(const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs)
    {
        i64 weight = 0;
        for (const auto& chunkSpec : chunkSpecs) {
            weight += chunkSpec.ByteSizeLong();
        }
        return weight;
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkSpecCacheEntry)

////////////////////////////////////////////////////////////////////////////////

class TChunkSpecCache::TImpl
    : public TAsyncSlruCacheBase<TTableKey, TChunkSpecCacheEntry>
{
public:
    TImpl(
        TSlruCacheConfigPtr config,
        int maxChunksPerFetch,
        int maxChunksPerLocateRequest,
        IInvokerPtr invoker,
        TLogger logger,
        TProfiler profiler)
        : TAsyncSlruCacheBase<TTableKey, TChunkSpecCacheEntry>(
            std::move(config),
            profiler)
        , MaxChunksPerFetch_(maxChunksPerFetch)
        , MaxChunksPerLocateRequest_(maxChunksPerLocateRequest)
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger))
    { }

    TFuture<std::vector<TErrorOr<std::vector<NChunkClient::NProto::TChunkSpec>>>> GetChunkSpecs(
        std::vector<TRequest> requests,
        NApi::NNative::IClientPtr client,
        NApi::TMasterReadOptions masterReadOptions)
    {
        std::vector<TErrorOr<std::vector<NChunkClient::NProto::TChunkSpec>>> finalResults;
        finalResults.resize(requests.size());

        std::vector<TFuture<TChunkSpecCacheEntryPtr>> pendingFutures;
        std::vector<TRequest> pendingRequests;
        std::vector<int> pendingIndices;
        pendingFutures.reserve(requests.size());
        pendingRequests.reserve(requests.size());
        pendingIndices.reserve(requests.size());

        std::vector<TTableKey> fetchKeys;
        std::vector<TInsertCookie> fetchCookies;

        int hitCount = 0;
        int missCount = 0;
        int obsoleteCount = 0;

        for (int index = 0; index < std::ssize(requests); ++index) {
            const auto& request = requests[index];
            TTableKey key{
                request.ObjectId,
                request.ExternalCellTag,
                request.ChunkCount,
                request.MinContentRevision,
                request.MinChunkMergerRevision,
            };

            YT_LOG_TRACE("Getting fresh chunk specs (Key: %v)", key);

            if (auto entry = Find(key)) {
                if (entry->GetContentRevision() >= request.MinContentRevision &&
                    entry->GetChunkMergerRevision() >= request.MinChunkMergerRevision)
                {
                    ++hitCount;
                    finalResults[index] = entry->ChunkSpecs;
                    continue;
                }
                ++obsoleteCount;
                TryRemove(key, true);
            } else {
                ++missCount;
            }

            auto cookie = BeginInsert(key);
            pendingFutures.push_back(cookie.GetValue());
            pendingRequests.push_back(request);
            pendingIndices.push_back(index);
            if (cookie.IsActive()) {
                fetchKeys.push_back(key);
                fetchCookies.push_back(std::move(cookie));
            }
        }

        YT_LOG_DEBUG(
            "Got synchronous results from cache (HitCount: %v, MissCount: %v, ObsoleteCount: %v)",
            hitCount,
            missCount,
            obsoleteCount);

        if (!fetchCookies.empty()) {
            YT_LOG_DEBUG("Fetching missing chunk specs from master (RequestCount: %v)", fetchCookies.size());
            FetchChunkSpecs(std::move(fetchKeys), std::move(fetchCookies), client, masterReadOptions);
        }

        if (pendingIndices.empty()) {
            return MakeFuture(std::move(finalResults));
        }

        YT_LOG_DEBUG(
            "Getting chunk specs from cache asynchronously (RequestCount: %v)",
            pendingIndices.size());

        return AllSet(pendingFutures).Apply(BIND(
            ThrowOnDestroyed(&TImpl::CombineResults),
            MakeWeak(this),
            Passed(std::move(finalResults)),
            Passed(std::move(pendingRequests)),
            Passed(std::move(pendingIndices)),
            std::move(client),
            std::move(masterReadOptions))
            .AsyncVia(Invoker_));
    }

private:
    const int MaxChunksPerFetch_;
    const int MaxChunksPerLocateRequest_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;

    TFuture<std::vector<TErrorOr<std::vector<NChunkClient::NProto::TChunkSpec>>>> CombineResults(
        std::vector<TErrorOr<std::vector<NChunkClient::NProto::TChunkSpec>>>&& finalResults,
        std::vector<TRequest>&& pendingRequests,
        std::vector<int>&& pendingIndices,
        NApi::NNative::IClientPtr client,
        NApi::TMasterReadOptions masterReadOptions,
        const std::vector<TErrorOr<TChunkSpecCacheEntryPtr>>& pendingResults)
    {
        YT_VERIFY(pendingIndices.size() == pendingResults.size());
        YT_VERIFY(pendingRequests.size() == pendingResults.size());

        std::vector<TRequest> staleRequests;
        std::vector<int> staleIndices;

        for (int index = 0; index < std::ssize(pendingIndices); ++index) {
            auto pendingIndex = pendingIndices[index];
            const auto& entryOrError = pendingResults[index];
            if (!entryOrError.IsOK()) {
                finalResults[pendingIndex] = TError(entryOrError);
                continue;
            }

            const auto& entry = entryOrError.Value();
            const auto& request = pendingRequests[index];
            if (entry->GetContentRevision() >= request.MinContentRevision &&
                entry->GetChunkMergerRevision() >= request.MinChunkMergerRevision)
            {
                finalResults[pendingIndex] = entry->ChunkSpecs;
            } else {
                staleRequests.push_back(request);
                staleIndices.push_back(pendingIndex);
            }
        }

        YT_LOG_DEBUG(
            "Got asynchronous results from cache (Count: %v, StaleCount: %v)",
            pendingIndices.size(),
            staleRequests.size());

        if (staleRequests.empty()) {
            return MakeFuture(std::move(finalResults));
        }

        YT_LOG_DEBUG(
            "Refetching chunk specs that were served by a stale in-flight fetch (RequestCount: %v)",
            staleRequests.size());

        return GetChunkSpecs(std::move(staleRequests), std::move(client), std::move(masterReadOptions)).Apply(BIND(
            [finalResults = std::move(finalResults), staleIndices = std::move(staleIndices)]
            (std::vector<TErrorOr<std::vector<NChunkClient::NProto::TChunkSpec>>> staleResults) mutable {
                YT_VERIFY(staleIndices.size() == staleResults.size());
                for (int index = 0; index < std::ssize(staleIndices); ++index) {
                    finalResults[staleIndices[index]] = std::move(staleResults[index]);
                }
                return std::move(finalResults);
            }));
    }

    void FetchChunkSpecs(
        std::vector<TTableKey> keys,
        std::vector<TInsertCookie> cookies,
        NApi::NNative::IClientPtr client,
        NApi::TMasterReadOptions masterReadOptions)
    {
        auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
            std::move(client),
            New<NNodeTrackerClient::TNodeDirectory>(),
            Invoker_,
            TMasterChunkSpecFetcherOptions{
                .MasterReadOptions = std::move(masterReadOptions),
                .MaxChunksPerFetch = MaxChunksPerFetch_,
                .MaxChunksPerLocateRequest = MaxChunksPerLocateRequest_,
                .FetchRequestInitializer = [] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int /*tableIndex*/) {
                    req->set_fetch_all_meta_extensions(false);
                    req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                    req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                    req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value);
                    // Valid since we only read specs of static chunks.
                    req->set_omit_dynamic_stores(true);
                    SetTransactionId(req, NullTransactionId);
                    SetSuppressAccessTracking(req, true);
                    SetSuppressExpirationTimeoutRenewal(req, true);
                },
            },
            Logger);

        for (int index = 0; index < std::ssize(keys); ++index) {
            const auto& key = keys[index];
            chunkSpecFetcher->Add(key.ObjectId, key.ExternalCellTag, key.ChunkCount, index);
        }

        chunkSpecFetcher->Fetch().Subscribe(BIND(
            ThrowOnDestroyed(&TImpl::OnChunkSpecsFetched),
            MakeWeak(this),
            chunkSpecFetcher,
            Passed(std::move(keys)),
            Passed(std::move(cookies))));
    }

    void OnChunkSpecsFetched(
        const TMasterChunkSpecFetcherPtr& chunkSpecFetcher,
        std::vector<TTableKey>&& keys,
        std::vector<TInsertCookie>&& cookies,
        const TError& error)
    {
        if (!error.IsOK()) {
            YT_LOG_DEBUG(error, "Failed to fetch chunk specs from master (RequestCount: %v)", keys.size());
            for (auto& cookie : cookies) {
                cookie.Cancel(error);
            }
            return;
        }

        std::vector<std::vector<NChunkClient::NProto::TChunkSpec>> chunkSpecsByTable(keys.size());
        for (auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
            auto tableIndex = chunkSpec.table_index();
            chunkSpec.set_table_index(0);
            chunkSpecsByTable[tableIndex].push_back(std::move(chunkSpec));
        }

        for (int index = 0; index < std::ssize(keys); ++index) {
            auto entry = New<TChunkSpecCacheEntry>(keys[index], std::move(chunkSpecsByTable[index]));
            cookies[index].EndInsert(std::move(entry));
        }

        YT_LOG_DEBUG("Fetched chunk specs from master (RequestCount: %v)", keys.size());
    }

    i64 GetWeight(const TChunkSpecCacheEntryPtr& entry) const override
    {
        return entry->GetWeight();
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkSpecCache::TChunkSpecCache(
    TSlruCacheConfigPtr config,
    int maxChunksPerFetch,
    int maxChunksPerLocateRequest,
    IInvokerPtr invoker,
    TLogger logger,
    TProfiler profiler)
    : Impl_(New<TImpl>(
        std::move(config),
        maxChunksPerFetch,
        maxChunksPerLocateRequest,
        std::move(invoker),
        std::move(logger),
        std::move(profiler)))
{ }

TChunkSpecCache::~TChunkSpecCache() = default;

TFuture<std::vector<TErrorOr<std::vector<NChunkClient::NProto::TChunkSpec>>>> TChunkSpecCache::GetChunkSpecs(
    std::vector<TRequest> requests,
    NApi::NNative::IClientPtr client,
    NApi::TMasterReadOptions masterReadOptions)
{
    return Impl_->GetChunkSpecs(std::move(requests), std::move(client), std::move(masterReadOptions));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
