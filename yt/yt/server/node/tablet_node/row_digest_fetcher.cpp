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
        TClusterNodeDynamicConfigPtr config,
        IInvokerPtr invoker)
        : TCompactionHintFetcher(
            std::move(invoker),
            TabletNodeProfiler
                .WithPrefix("/chunk_row_digest_fetcher")
                .WithTag("cell_id", ToString(cellId)))
        , RowDigestParseCumulativeTime_(Profiler_.TimeCounter("/row_digest_parse_cumulative_time"))
    {
        Invoker_->Invoke(BIND(
            &TCompactionHintFetcher::Reconfigure,
            MakeWeak(this),
            /*oldConfig*/ nullptr,
            std::move(config)));
    }

    void Reconfigure(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& config = newConfig->TabletNode->StoreCompactor;
        RequestThrottler_->Reconfigure(config->RowDigestRequestThrottler);
        ThrottlerTryAcquireBackoff_ = config->RowDigestThrottlerTryAcquireBackoff;
        UseRowDigests_ = config->UseRowDigests;

        if (!UseRowDigests_) {
            ClearQueue();
        }
    }

private:
    const TTimeCounter RowDigestParseCumulativeTime_;

    bool UseRowDigests_;

    static TCompactionHintRequestWithResult<TRowDigestUpcomingCompactionInfo>& GetRowDigestCompactionHints(
        const IStorePtr& store)
    {
        return store->AsSortedChunk()->CompactionHints().RowDigest;
    }

    bool HasRequestStatus(const IStorePtr& store) const override
    {
        return GetRowDigestCompactionHints(store).IsRequestStatus();
    }

    ECompactionHintRequestStatus GetRequestStatus(const IStorePtr& store) const override
    {
        return GetRowDigestCompactionHints(store).AsRequestStatus();
    }

    void SetRequestStatus(const IStorePtr& store, ECompactionHintRequestStatus status) const override
    {
        GetRowDigestCompactionHints(store).SetRequestStatus(status);
    }

    bool IsFetchingRequiredForStore(const IStorePtr& store) const override
    {
        return store->GetType() == EStoreType::SortedChunk;
    }

    bool IsFetchingRequiredForTablet(const TTablet& tablet) const override
    {
        return UseRowDigests_ && tablet.IsPhysicallySorted();
    }

    void TryMakeRequest() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto stores = GetStoresForRequest();
        if (stores.empty()) {
            return;
        }

        if (!UseRowDigests_) {
            for (const auto& store : stores) {
                SetRequestStatus(store, ECompactionHintRequestStatus::None);
            }
            return;
        }

        std::vector<TFuture<TRefCountedChunkMetaPtr>> asyncRowDigestMetas;
        for (auto& store : stores) {
            SetRequestStatus(store, ECompactionHintRequestStatus::Requested);

            auto sortedChunkStore = store->AsSortedChunk();

            auto reader = sortedChunkStore->GetBackendReaders(
                EWorkloadCategory::SystemTabletCompaction).ChunkReader;
            asyncRowDigestMetas.push_back(reader->GetMeta(
                /*options*/ {},
                /*partitionTag*/ {},
                std::vector<int>{TProtoExtensionTag<TVersionedRowDigestExt>::Value}));
            YT_LOG_DEBUG("Requesting row digest for store (StoreId: %v, ChunkId: %v)",
                sortedChunkStore->GetId(),
                sortedChunkStore->GetChunkId());
        }

        AllSet(std::move(asyncRowDigestMetas))
            .SubscribeUnique(BIND(
                &TRowDigestFetcher::OnRowDigestMetaReceived,
                MakeStrong(this),
                Passed(std::move(stores)))
                .Via(Invoker_));
    }

    void OnRowDigestMetaReceived(
        std::vector<IStorePtr> stores,
        TErrorOr<std::vector<TErrorOr<TRefCountedChunkMetaPtr>>> allSetResult)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(allSetResult.IsOK());
        auto errorOrRsps = allSetResult.Value();
        YT_VERIFY(stores.size() == errorOrRsps.size());

        if (!UseRowDigests_) {
            for (const auto& store : stores) {
                SetRequestStatus(store, ECompactionHintRequestStatus::None);
            }
            return;
        }

        for (const auto& [store, errorOrRsp] : Zip(stores, errorOrRsps)) {
            if (!errorOrRsp.IsOK()) {
                OnRequestFailed(store);
                YT_LOG_WARNING(errorOrRsp, "Failed to receive row digest for chunk (StoreId: %v)",
                    store->GetId());
                continue;
            }

            auto rowDigestExt = FindProtoExtension<TVersionedRowDigestExt>(
                errorOrRsp.Value()->extensions());

            if (!rowDigestExt) {
                YT_LOG_DEBUG(errorOrRsp, "Chunk meta does not contain row digest (StoreId: %v)",
                    store->GetId());
                continue;
            }

            TVersionedRowDigest digest;

            TWallTimer timer;
            FromProto(&digest, rowDigestExt.value());
            RowDigestParseCumulativeTime_.Add(timer.GetElapsedTime());

            GetRowDigestCompactionHints(store).SetResult(GetUpcomingCompactionInfo(
                store->GetId(),
                store->GetTablet()->GetSettings().MountConfig,
                digest));
        }

        TryMakeRequest();
    }
};

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetcherPtr CreateRowDigestFetcher(
    TTabletCellId cellId,
    TClusterNodeDynamicConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TRowDigestFetcher>(
        cellId,
        std::move(config),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
