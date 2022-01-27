#include "queue_controller.h"

#include "snapshot.h"
#include "snapshot_representation.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NQueueAgent {

using namespace NHydra;
using namespace NYTree;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYson;

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////

using TConsumerSnapshotMap = THashMap<TCrossClusterReference, TConsumerSnapshotPtr>;

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicTableController
    : public IQueueController
{
public:
    TOrderedDynamicTableController(
        TQueueControllerConfigPtr config,
        TClusterDirectoryPtr clusterDirectory,
        TCrossClusterReference queueRef,
        TQueueTableRow queueRow,
        TConsumerRowMap consumerRowMap,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , ClusterDirectory_(std::move(clusterDirectory))
        , QueueRef_(std::move(queueRef))
        , Invoker_(std::move(invoker))
        , Logger(QueueAgentLogger.WithTag("Queue: %Qv", QueueRef_))
        , LoopExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TOrderedDynamicTableController::Loop, MakeWeak(this)),
            Config_->LoopPeriod))
    {
        QueueSnapshot_ = New<TQueueSnapshot>();
        QueueSnapshot_->Row = std::move(queueRow);
        QueueSnapshot_->Error = TError("Queue is not processed yet");

        for (auto& [ref, row] : consumerRowMap) {
            auto snapshot = New<TConsumerSnapshot>();
            snapshot->Row = std::move(row);
            snapshot->Error = TError("Consumer is not processed yet");
            ConsumerSnapshots_.emplace(std::move(ref), std::move(snapshot));
        }
    }

    EQueueFamily GetQueueFamily() const override
    {
        return EQueueFamily::OrderedDynamicTable;
    }

    void BuildOrchid(TFluentMap fluent) const override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        fluent
            .Item("loop_index").Value(LoopIndex_)
            .Item("loop_instant").Value(LoopInstant_)
            .Item("row").Value(QueueSnapshot_->Row)
            .Item("status").Do(std::bind(BuildQueueStatusYson, QueueSnapshot_, _1))
            .Item("partitions").Do(std::bind(BuildQueuePartitionListYson, QueueSnapshot_, _1));
    }

    void BuildConsumerOrchid(const TCrossClusterReference& consumerRef, TFluentMap fluent) const override
    {
        Y_UNUSED(consumerRef);

        const auto& consumerSnapshot = GetOrCrash(ConsumerSnapshots_, consumerRef);

        fluent
            .Item("loop_index").Value(LoopIndex_)
            .Item("loop_instant").Value(LoopInstant_)
            .Item("row").Value(consumerSnapshot->Row)
            .Item("status").Do(std::bind(BuildConsumerStatusYson, consumerSnapshot, _1))
            .Item("partitions").Do(std::bind(BuildConsumerPartitionListYson, consumerSnapshot, _1));
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Queue controller started");

        LoopExecutor_->Start();
    }

    TFuture<void> Stop() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Queue controller stopped");

        return LoopExecutor_->Stop();
    }

    IInvokerPtr GetInvoker() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Invoker_;
    }

private:
    const TQueueControllerConfigPtr Config_;
    const TClusterDirectoryPtr ClusterDirectory_;
    const TCrossClusterReference QueueRef_;

    TQueueSnapshotPtr QueueSnapshot_;
    TConsumerSnapshotMap ConsumerSnapshots_;

    const IInvokerPtr Invoker_;

    const NLogging::TLogger Logger;

    const TPeriodicExecutorPtr LoopExecutor_;

    i64 LoopIndex_ = 0;
    TInstant LoopInstant_ = TInstant::Zero();

    void Loop()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        const auto& Logger = QueueAgentLogger;

        LoopInstant_ = TInstant::Now();
        ++LoopIndex_;

        YT_LOG_INFO("Controller loop started (LoopIndex: %v)", LoopIndex_);
        auto logFinally = Finally([&] {
            YT_LOG_INFO("Controller loop finished (LoopIndex: %v)", LoopIndex_);
        });

        // First, update queue snapshot.
        {
            auto nextQueueSnapshot = New<TQueueSnapshot>();
            nextQueueSnapshot->Row = std::move(QueueSnapshot_->Row);
            GuardedUpdateQueueSnapshot(nextQueueSnapshot);
            QueueSnapshot_ = std::move(nextQueueSnapshot);
        }

        // Second, update consumer snapshots.

        {
            std::vector<TFuture<void>> consumerSnapshotUpdateFutures;
            TConsumerSnapshotMap nextConsumerSnapshots;

            auto consumerCount = ConsumerSnapshots_.size();
            consumerSnapshotUpdateFutures.reserve(consumerCount);
            nextConsumerSnapshots.reserve(consumerCount);

            for (const auto& [consumerRef, consumerSnapshot] : ConsumerSnapshots_) {
                auto& nextConsumerSnapshot = nextConsumerSnapshots[consumerRef];
                nextConsumerSnapshot = New<TConsumerSnapshot>();
                nextConsumerSnapshot->Row = std::move(consumerSnapshot->Row);
                consumerSnapshotUpdateFutures.emplace_back(
                    BIND(&TOrderedDynamicTableController::GuardedUpdateConsumerSnapshot, MakeWeak(this), consumerRef, nextConsumerSnapshot)
                        .AsyncVia(Invoker_)
                        .Run());
            }

            // None of snapshot update methods may throw.
            YT_VERIFY(WaitFor(AllSucceeded(consumerSnapshotUpdateFutures)).IsOK());

            ConsumerSnapshots_.swap(nextConsumerSnapshots);
        }
    }

    void GuardedUpdateQueueSnapshot(const TQueueSnapshotPtr& snapshot)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        try {
            UpdateQueueSnapshot(snapshot);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_DEBUG(error, "Error updating queue snapshot");
            snapshot->Error = std::move(error);
        }
    }

    void UpdateQueueSnapshot(const TQueueSnapshotPtr& snapshot)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto client = GetClusterClient(ClusterDirectory_, QueueRef_.Cluster);
        const auto& tableMountCache = client->GetTableMountCache();

        // Fetch partition count (which is equal to tablet count).

        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(QueueRef_.Path))
            .ValueOrThrow();

        auto& partitionCount = snapshot->PartitionCount;
        partitionCount = tableInfo->Tablets.size();

        auto& partitionSnapshots = snapshot->PartitionSnapshots;
        partitionSnapshots.resize(partitionCount);
        for (auto& partitionSnapshot : partitionSnapshots) {
            partitionSnapshot = New<TQueuePartitionSnapshot>();
        }

        // Fetch tablet infos.

        std::vector<int> tabletIndexes;
        tabletIndexes.reserve(partitionCount);
        for (int index = 0; index < partitionCount; ++index) {
            const auto& tabletInfo = tableInfo->Tablets[index];
            if (tabletInfo->State != ETabletState::Mounted) {
                partitionSnapshots[index]->Error = TError("Tablet %v is not mounted", tabletInfo->TabletId)
                    << TErrorAttribute("state", tabletInfo->State);
            } else {
                tabletIndexes.push_back(index);
            }
        }

        auto tabletInfos = WaitFor(client->GetTabletInfos(QueueRef_.Path, tabletIndexes))
            .ValueOrThrow();

        YT_VERIFY(std::ssize(tabletInfos) == std::ssize(tabletIndexes));

        // Fill partition snapshots from tablet infos.

        for (int index = 0; index < std::ssize(tabletInfos); ++index) {
            const auto& partitionSnapshot = partitionSnapshots[tabletIndexes[index]];
            const auto& tabletInfo = tabletInfos[index];
            partitionSnapshot->UpperRowIndex = tabletInfo.TotalRowCount;
            partitionSnapshot->LowerRowIndex = tabletInfo.TrimmedRowCount;
            partitionSnapshot->AvailableRowCount = partitionSnapshot->UpperRowIndex - partitionSnapshot->LowerRowIndex;
            partitionSnapshot->LastRowCommitTime = TimestampToInstant(tabletInfo.LastWriteTimestamp).first;
        }
    }

    void GuardedUpdateConsumerSnapshot(const TCrossClusterReference& consumerRef, const TConsumerSnapshotPtr& snapshot)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        snapshot->TargetQueue = QueueRef_;
        // TODO(max42): extend model.
        // snapshot->Vital = snapshot->Row->Vital;

        try {
            UpdateConsumerSnapshot(consumerRef, snapshot);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_DEBUG(error, "Error updating consumer snapshot (Consumer: %Qv)", consumerRef);
            snapshot->Error = std::move(error);
        }
    }

    void UpdateConsumerSnapshot(const TCrossClusterReference& consumerRef, const TConsumerSnapshotPtr& snapshot)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        Y_UNUSED(consumerRef);
        Y_UNUSED(snapshot);
    }
};

DEFINE_REFCOUNTED_TYPE(TOrderedDynamicTableController)

////////////////////////////////////////////////////////////////////////////////

IQueueControllerPtr CreateQueueController(
    TQueueControllerConfigPtr config,
    NHiveClient::TClusterDirectoryPtr clusterDirectory,
    TCrossClusterReference queueRef,
    EQueueFamily queueFamily,
    TQueueTableRow queueRow,
    THashMap<TCrossClusterReference, TConsumerTableRow> consumerRefToRow,
    IInvokerPtr invoker)
{
    switch (queueFamily) {
        case EQueueFamily::OrderedDynamicTable:
            return New<TOrderedDynamicTableController>(
                std::move(config),
                std::move(clusterDirectory),
                std::move(queueRef),
                std::move(queueRow),
                std::move(consumerRefToRow),
                std::move(invoker));
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
