#include "row_digest_fetcher.h"
#include "sorted_chunk_store.h"
#include "tablet.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/lsm/helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/quantile_digest/quantile_digest.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NTabletNode {

using namespace NLsm;
using namespace NProfiling;
using namespace NConcurrency;
using namespace NClusterNode;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

// Fetch chunk digests to determine |timestamp| when chunk should be compacted.
// More than MaxObsoleteTimestampRatio rows should be obsolete at |timestamp|,
// these rows will definitely be deleted by compaction.
class TRowDigestFetcher
    : public TCompactionHintFetcher
{
public:
    TRowDigestFetcher(
        TTabletCellId cellId,
        IInvokerPtr invoker,
        const TClusterNodeDynamicConfigPtr& config)
        : TCompactionHintFetcher(
            std::move(invoker),
            TabletNodeProfiler
                .WithPrefix("/chunk_row_digest_fetcher")
                .WithTag("cell_id", ToString(cellId)))
        , RowDigestParseCumulativeTime_(Profiler_.TimeCounter("/row_digest_parse_cumulative_time"))
    {
        Reconfigure(config);
    }

    void Reconfigure(const TClusterNodeDynamicConfigPtr& config) override
    {
        const auto& storeCompactorConfig = config->TabletNode->StoreCompactor;
        FetchingExecutor_->SetPeriod(storeCompactorConfig->RowDigestFetchPeriod);
        RequestThrottler_->Reconfigure(storeCompactorConfig->RowDigestRequestThrottler);

        Invoker_->Invoke(BIND(
            &TRowDigestFetcher::DoReconfigure,
            MakeWeak(this),
            storeCompactorConfig->UseRowDigests));
    }

private:
    const TTimeCounter RowDigestParseCumulativeTime_;

    bool UseRowDigests_ = false;

    void DoReconfigure(bool useRowDigests)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);
        if (UseRowDigests_ == useRowDigests) {
            return;
        }

        if (UseRowDigests_ = useRowDigests) {
            FetchingExecutor_->Start();
        } else {
            YT_UNUSED_FUTURE(WaitFor(FetchingExecutor_->Stop()));
            ClearQueue();
        }
    }

    void ResetResult(const IStorePtr& store) const override
    {
        store->AsSortedChunk()->CompactionHints().RowDigest.CompactionHint.reset();
    }

    bool IsFetchableStore(const IStorePtr& store) const override
    {
        return store->GetType() == EStoreType::SortedChunk;
    }

    bool IsFetchableTablet(const TTablet& tablet) const override
    {
        return UseRowDigests_ && tablet.IsPhysicallySorted();
    }

    TCompactionHintFetchStatus& GetFetchStatus(const IStorePtr& store) const override
    {
        return store->AsSortedChunk()->CompactionHints().RowDigest.FetchStatus;
    }

    void MakeRequest(std::vector<IStorePtr>&& stores) override
    {
        static constexpr int RequestStep = 1;

        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_ASSERT(!stores.empty());

        std::vector<TFuture<TRefCountedChunkMetaPtr>> asyncRowDigestMetas;
        for (const auto& store : stores) {
            auto sortedChunkStore = store->AsSortedChunk();

            auto reader = sortedChunkStore->GetBackendReaders(
                EWorkloadCategory::SystemTabletCompaction).ChunkReader;
            asyncRowDigestMetas.push_back(reader->GetMeta(
                /*options*/ {},
                /*partitionTag*/ {},
                std::vector<int>{TProtoExtensionTag<TVersionedRowDigestExt>::Value}));

            GetFetchStatus(store).RequestStep = RequestStep;

            YT_LOG_DEBUG("Requesting row digest for store (StoreId: %v, ChunkId: %v)",
                sortedChunkStore->GetId(),
                sortedChunkStore->GetChunkId());
        }

        AllSet(std::move(asyncRowDigestMetas))
            .SubscribeUnique(BIND(
                &TRowDigestFetcher::OnRowDigestMetaReceived,
                MakeStrong(this),
                std::move(stores))
                .Via(Invoker_));
    }

    void OnRowDigestMetaReceived(
        const std::vector<IStorePtr>& stores,
        TErrorOr<std::vector<TErrorOr<TRefCountedChunkMetaPtr>>> allSetResult)
    {
        static constexpr int RequestStep = 2;

        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(allSetResult.IsOK());
        auto errorOrRsps = allSetResult.Value();
        YT_VERIFY(stores.size() == errorOrRsps.size());

        i64 finishedRequestCount = 0;
        for (const auto& [store, errorOrRsp] : Zip(stores, errorOrRsps)) {
            if (!errorOrRsp.IsOK()) {
                OnRequestFailed(store, RequestStep - 1);
                YT_LOG_WARNING(errorOrRsp, "Failed to receive row digest for chunk (StoreId: %v)",
                    store->GetId());
                continue;
            }

            if (IsValidStoreState(store) && GetFetchStatus(store).ShouldConsumeRequestResult(RequestStep)) {
                auto sortedChunkStore = store->AsSortedChunk();

                auto rowDigestExt = FindProtoExtension<TVersionedRowDigestExt>(
                    errorOrRsp.Value()->extensions());

                if (!rowDigestExt) {
                    YT_LOG_DEBUG(errorOrRsp, "Chunk meta does not contain row digest (StoreId: %v, ChunkId: %v)",
                        sortedChunkStore->GetId(),
                        sortedChunkStore->GetChunkId());
                    continue;
                }

                TVersionedRowDigest digest;

                TWallTimer timer;
                FromProto(&digest, rowDigestExt.value());
                RowDigestParseCumulativeTime_.Add(timer.GetElapsedTime());

                auto& compactionHints = sortedChunkStore->CompactionHints().RowDigest;
                compactionHints.FetchStatus.RequestStep = RequestStep;
                compactionHints.CompactionHint = GetUpcomingCompactionInfo(
                    store->GetId(),
                    store->GetTablet()->GetSettings().MountConfig,
                    digest);

                ++finishedRequestCount;
                YT_LOG_DEBUG("Finished fetching row digest (StoreId: %v, ChunkId: %v, "
                    "CompactionHintReason: %v, CompactionHintTimestamp: %v)",
                    sortedChunkStore->GetId(),
                    sortedChunkStore->GetChunkId(),
                    compactionHints.CompactionHint->Reason,
                    compactionHints.CompactionHint->Timestamp);
            }

            FinishedRequestCount_.Increment(finishedRequestCount);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetcherPtr CreateRowDigestFetcher(
    TTabletCellId cellId,
    IInvokerPtr invoker,
    const TClusterNodeDynamicConfigPtr& config)
{
    return New<TRowDigestFetcher>(
        cellId,
        std::move(invoker),
        config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
