#include "table_columnar_statistics_cache.h"

#include "config.h"
#include "columnar_statistics_fetcher.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NHydra;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

struct TColumnarStatisticsEntry
{
    TRevision Revision;
    TTableSchemaPtr Schema;
    TColumnarStatistics Statistics;
};

struct TTableKey
{
    TObjectId ObjectId;
    //! Fields below are merely a payload, they are not used for comparison.
    TCellTag ExternalCellTag;
    i64 ChunkCount;
    TTableSchemaPtr Schema;
    TRevision Revision;
};

bool operator == (const TTableKey& lhs, const TTableKey& rhs)
{
    return lhs.ObjectId == rhs.ObjectId;
}

TString ToString(const TTableKey& key)
{
    return Format("#%v@%v%v",
        key.ObjectId,
        key.Revision,
        MakeShrunkFormattableView(key.Schema->GetColumnNames(), TDefaultFormatter(), 5));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NTableClient::TTableKey>
{
    size_t operator()(const NYT::NTableClient::TTableKey& key)
    {
        return THash<NYT::NObjectClient::TObjectId>()(key.ObjectId);
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableColumnarStatisticsCache::TImpl
    : public TAsyncExpiringCache<TTableKey, TColumnarStatisticsEntry>
{
public:
    TImpl(
        TTableColumnarStatisticsCacheConfigPtr config,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        TLogger logger,
        TProfiler registry)
        : TAsyncExpiringCache<TTableKey, TColumnarStatisticsEntry>(
            config,
            logger,
            std::move(registry))
        , Config_(config)
        , Client_(client)
        , Invoker_(invoker)
        , Logger(logger)
    {
        YT_VERIFY(!Config_->RefreshTime);
    }

    TFuture<std::vector<TErrorOr<TNamedColumnarStatistics>>> GetFreshStatistics(std::vector<TRequest> requests)
    {
        // First perform a synchronous lookup.
        std::vector<TTableKey> keys;
        keys.reserve(requests.size());
        for (const auto& request : requests) {
            keys.push_back({request.ObjectId, request.ExternalCellTag, request.ChunkCount, request.Schema, request.MinRevision});
            YT_LOG_TRACE("Getting fresh columnar statistics (Key: %v)", keys.back());
        }

        std::vector<TErrorOr<TNamedColumnarStatistics>> finalResults;
        finalResults.resize(requests.size());

        YT_LOG_DEBUG(
            "Getting columnar statistics from cache synchronously (RequestCount: %v)",
            requests.size());

        auto syncResults = FindMany(keys);
        // Collect paths for which statistics are missing or obsolete.
        std::vector<TTableKey> missedKeys;
        std::vector<int> missedIndices;
        missedKeys.reserve(requests.size());
        missedIndices.reserve(requests.size());
        int hitCount = 0;
        int missCount = 0;
        int obsoleteCount = 0;
        for (size_t index = 0; index < keys.size(); ++index) {
            const auto& request = requests[index];
            const auto& syncResult = syncResults[index];
            if (!syncResult) {
                // Result is not cached.
                ++missCount;
                missedKeys.emplace_back(std::move(keys[index]));
                missedIndices.push_back(index);
            } else if (syncResult->IsOK() && request.MinRevision > syncResult->Value().Revision) {
                // Result is cached but related to an older revision.
                ++obsoleteCount;
                // TODO(max42): multikey version of Invalidate.
                InvalidateActive(keys[index]);
                missedKeys.emplace_back(std::move(keys[index]));
                missedIndices.push_back(index);
            } else {
                ++hitCount;
                finalResults[index] = EntryToNamedStatistics(*syncResult);
            }
        }

        YT_LOG_DEBUG(
            "Got synchronous results from cache (HitCount: %v, MissCount: %v, ObsoleteCount: %v)",
            hitCount,
            missCount,
            obsoleteCount);

        if (missedKeys.empty()) {
            return MakeFuture(finalResults);
        }

        YT_LOG_DEBUG(
            "Getting columnar statistics from cache asynchronously (RequestCount: %v)",
            missedKeys.size());

        auto asyncResults = GetMany(missedKeys);

        return asyncResults.ApplyUnique(BIND(&TImpl::CombineResult, MakeStrong(this), Passed(std::move(finalResults)), Passed(std::move(missedIndices))));
    }

    std::vector<TErrorOr<TNamedColumnarStatistics>> CombineResult(
        std::vector<TErrorOr<TNamedColumnarStatistics>>&& finalResults,
        std::vector<int>&& missedIndices,
        std::vector<TErrorOr<TColumnarStatisticsEntry>>&& missedResults)
    {
        YT_VERIFY(missedIndices.size() == missedResults.size());
        for (size_t index = 0; index < missedIndices.size(); ++index) {
            auto missedIndex = missedIndices[index];
            auto& missedResult = missedResults[index].Value();
            finalResults[missedIndex] = missedResult.Statistics.MakeNamedStatistics(missedResult.Schema->GetColumnNames());
        }

        YT_LOG_DEBUG("Got asynchronous results from cache (Count: %v)", missedIndices.size());

        return finalResults;
    }

    TErrorOr<TNamedColumnarStatistics> EntryToNamedStatistics(TErrorOr<TColumnarStatisticsEntry> entry) const
    {
        if (!entry.IsOK()) {
            return TError(entry);
        } else {
            return entry.Value().Statistics.MakeNamedStatistics(entry.Value().Schema->GetColumnNames());
        }
    }

    TFuture<std::vector<TErrorOr<TColumnarStatisticsEntry>>> DoGetMany(
        const std::vector<TTableKey>& keys,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
            Client_,
            TMasterReadOptions{},
            New<TNodeDirectory>(),
            Invoker_,
            Config_->MaxChunksPerFetch,
            Config_->MaxChunksPerLocateRequest,
            [=] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int /*tableIndex*/) {
                req->set_fetch_all_meta_extensions(false);
                req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value);
                req->set_omit_dynamic_stores(true);
                SetTransactionId(req, NullTransactionId);
                SetSuppressAccessTracking(req, true);
                SetSuppressExpirationTimeoutRenewal(req, true);
            },
            Logger);
        for (const auto& [index, key] : Enumerate(keys)) {
            chunkSpecFetcher->Add(key.ObjectId, key.ExternalCellTag, key.ChunkCount, index);
        }

        return chunkSpecFetcher->Fetch()
            .Apply(BIND(&TImpl::OnChunkSpecsFetched, MakeStrong(this), chunkSpecFetcher, keys));
    }

    TFuture<std::vector<TErrorOr<TColumnarStatisticsEntry>>> OnChunkSpecsFetched(TMasterChunkSpecFetcherPtr chunkSpecFetcher, const std::vector<TTableKey>& keys)
    {
        auto columnarStatisticsFetcher = New<TColumnarStatisticsFetcher>(
            Invoker_,
            Client_,
            TColumnarStatisticsFetcher::TOptions{
                .Config = Config_->Fetcher,
                .NodeDirectory = chunkSpecFetcher->GetNodeDirectory(),
                .Mode = Config_->ColumnarStatisticsFetcherMode,
                .AggregatePerTableStatistics = true,
                .Logger = Logger,
            });
        for (const auto& chunk : chunkSpecFetcher->ChunkSpecs()) {
            auto inputChunk = New<TInputChunk>(chunk);
            columnarStatisticsFetcher->AddChunk(inputChunk, keys[chunk.table_index()].Schema->GetColumnStableNames());
        }
        return columnarStatisticsFetcher->Fetch()
            .Apply(BIND(&TImpl::OnColumnarStatisticsFetched, MakeStrong(this), columnarStatisticsFetcher, keys));
    }

    std::vector<TErrorOr<TColumnarStatisticsEntry>> OnColumnarStatisticsFetched(TColumnarStatisticsFetcherPtr columnarStatisticsFetcher, const std::vector<TTableKey>& keys)
    {
        auto tableStatistics = columnarStatisticsFetcher->GetTableStatistics();
        YT_VERIFY(tableStatistics.size() <= keys.size());
        tableStatistics.resize(keys.size());
        std::vector<TErrorOr<TColumnarStatisticsEntry>> results;
        results.reserve(keys.size());
        for (const auto& [statistics, key] : Zip(tableStatistics, keys)) {
            results.push_back(TColumnarStatisticsEntry{
                .Revision = key.Revision,
                .Schema = key.Schema,
                .Statistics = statistics,
            });
        }
        return results;
    }

    TFuture<TColumnarStatisticsEntry> DoGet(const TTableKey& key, bool isPeriodicUpdate) noexcept override
    {
        return DoGetMany({key}, isPeriodicUpdate)
            .Apply(BIND([] (const std::vector<TErrorOr<TColumnarStatisticsEntry>>& results) {
                YT_VERIFY(results.size() == 1);
                if (!results.back().IsOK()) {
                    THROW_ERROR(results.back());
                } else {
                    return results.back().Value();
                }
            }));
    }

private:
    TTableColumnarStatisticsCacheConfigPtr Config_;
    NApi::NNative::IClientPtr Client_;
    IInvokerPtr Invoker_;
    TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

TTableColumnarStatisticsCache::TTableColumnarStatisticsCache(
    TTableColumnarStatisticsCacheConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    TLogger logger,
    TProfiler profiler)
    : Impl_(New<TImpl>(std::move(config), std::move(client), std::move(invoker), logger, profiler))
{ }

TTableColumnarStatisticsCache::~TTableColumnarStatisticsCache() = default;

TFuture<std::vector<TErrorOr<TNamedColumnarStatistics>>> TTableColumnarStatisticsCache::GetFreshStatistics(std::vector<TRequest> requests)
{
    return Impl_->GetFreshStatistics(std::move(requests));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
