#include "queue_controller.h"

#include "snapshot.h"
#include "snapshot_representation.h"
#include "config.h"
#include "helpers.h"
#include "profile_manager.h"

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/ema_counter.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NQueueAgent {

using namespace NHydra;
using namespace NYTree;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NQueueClient;
using namespace NYson;
using namespace NTracing;
using namespace NLogging;

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

//! Collect cumulative row indices from rows with given (tablet_index, row_index) pairs and
//! return them as a collection of (tablet_index, cumulative_data_weight) pairs.
std::vector<std::pair<int, i64>> CollectCumulativeDataWeights(
    const TYPath& path,
    NApi::IClientPtr client,
    const std::vector<std::pair<int, i64>>& tabletAndRowIndices,
    const TLogger& logger)
{
    const auto& Logger = logger;

    if (tabletAndRowIndices.empty()) {
        return {};
    }

    TStringBuilder queryBuilder;
    queryBuilder.AppendFormat("[$tablet_index], [$cumulative_data_weight] from [%v] where ([$tablet_index], [$row_index]) in (",
        path);
    bool isFirstTuple = true;
    for (const auto& [partitionIndex, rowIndex] : tabletAndRowIndices) {
        if (!isFirstTuple) {
            queryBuilder.AppendString(", ");
        }
        queryBuilder.AppendFormat("(%vu, %vu)", partitionIndex, rowIndex);
        isFirstTuple = false;
    }

    queryBuilder.AppendString(")");

    YT_VERIFY(!isFirstTuple);

    auto query = queryBuilder.Flush();
    YT_LOG_TRACE("Executing query for cumulative data weights (Query: %Qv)", query);
    auto selectResult = WaitFor(client->SelectRows(query))
        .ValueOrThrow();

    std::vector<std::pair<int, i64>> result;
    result.reserve(selectResult.Rowset->GetRows().size());

    for (const auto& row : selectResult.Rowset->GetRows()) {
        YT_VERIFY(row.GetCount() == 2);
        YT_VERIFY(row[0].Type == EValueType::Int64);
        auto tabletIndex = row[0].Data.Int64;

        YT_VERIFY(row[1].Type == EValueType::Int64);
        auto cumulativeDataWeight = row[1].Data.Int64;
        result.emplace_back(tabletIndex, cumulativeDataWeight);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TQueueSnapshotBuildSession final
{
public:
    TQueueSnapshotBuildSession(
        TQueueSnapshotPtr previousQueueSnapshot,
        IInvokerPtr invoker,
        TLogger logger,
        TClientDirectoryPtr clientDirectory)
        : PreviousQueueSnapshot_(std::move(previousQueueSnapshot))
        , Invoker_(std::move(invoker))
        , Logger(logger)
        , ClientDirectory_(std::move(clientDirectory))
    { }

    TQueueSnapshotPtr Build()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        try {
            DoBuild();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_DEBUG(error, "Error updating queue snapshot");
            QueueSnapshot_->Error = std::move(error);
        }

        return QueueSnapshot_;
    }

private:
    TQueueSnapshotPtr PreviousQueueSnapshot_;
    IInvokerPtr Invoker_;
    TLogger Logger;
    TClientDirectoryPtr ClientDirectory_;

    TQueueSnapshotPtr QueueSnapshot_ = New<TQueueSnapshot>();

    void DoBuild()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Building queue snapshot");

        QueueSnapshot_->Row = PreviousQueueSnapshot_->Row;

        auto queueRef = QueueSnapshot_->Row.Queue;

        QueueSnapshot_->Family = EQueueFamily::OrderedDynamicTable;
        auto client = ClientDirectory_->GetClientOrThrow(queueRef.Cluster);
        const auto& tableMountCache = client->GetTableMountCache();

        // Fetch partition count (which is equal to tablet count).

        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(queueRef.Path))
            .ValueOrThrow();

        YT_LOG_DEBUG("Table info collected (TabletCount: %v)", tableInfo->Tablets.size());

        const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
        QueueSnapshot_->HasTimestampColumn = schema->HasTimestampColumn();
        QueueSnapshot_->HasCumulativeDataWeightColumn = schema->FindColumn(CumulativeDataWeightColumnName);

        auto& partitionCount = QueueSnapshot_->PartitionCount;
        partitionCount = tableInfo->Tablets.size();

        auto& partitionSnapshots = QueueSnapshot_->PartitionSnapshots;
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

        auto tabletInfos = WaitFor(client->GetTabletInfos(queueRef.Path, tabletIndexes))
            .ValueOrThrow();

        YT_VERIFY(std::ssize(tabletInfos) == std::ssize(tabletIndexes));

        // Fill partition snapshots from tablet infos.

        for (int index = 0; index < std::ssize(tabletInfos); ++index) {
            const auto& partitionSnapshot = partitionSnapshots[tabletIndexes[index]];
            auto previousPartitionSnapshot = (index < std::ssize(PreviousQueueSnapshot_->PartitionSnapshots))
                ? PreviousQueueSnapshot_->PartitionSnapshots[index]
                : nullptr;
            const auto& tabletInfo = tabletInfos[index];
            partitionSnapshot->UpperRowIndex = tabletInfo.TotalRowCount;
            partitionSnapshot->LowerRowIndex = tabletInfo.TrimmedRowCount;
            partitionSnapshot->AvailableRowCount = partitionSnapshot->UpperRowIndex - partitionSnapshot->LowerRowIndex;
            partitionSnapshot->LastRowCommitTime = TimestampToInstant(tabletInfo.LastWriteTimestamp).first;
            partitionSnapshot->CommitIdleTime = TInstant::Now() - partitionSnapshot->LastRowCommitTime;

            if (previousPartitionSnapshot) {
                partitionSnapshot->WriteRate = previousPartitionSnapshot->WriteRate;
            }

            partitionSnapshot->WriteRate.RowCount.Update(tabletInfo.TotalRowCount);
        }

        CollectCumulativeDataWeights();

        for (int index = 0; index < std::ssize(tabletInfos); ++index) {
            const auto& partitionSnapshot = partitionSnapshots[tabletIndexes[index]];
            QueueSnapshot_->WriteRate += partitionSnapshot->WriteRate;
        }

        YT_LOG_DEBUG("Queue snapshot built");
    }

    void CollectCumulativeDataWeights()
    {
        YT_LOG_DEBUG("Collecting queue cumulative data weights");

        auto queueRef = QueueSnapshot_->Row.Queue;

        std::vector<std::pair<int, i64>> tabletAndRowIndices;

        for (const auto& [partitionIndex, partitionSnapshot] : Enumerate(QueueSnapshot_->PartitionSnapshots)) {
            // Partition should not be erroneous and contain at least one row.
            if (partitionSnapshot->Error.IsOK() && partitionSnapshot->UpperRowIndex > 0) {
                tabletAndRowIndices.emplace_back(partitionIndex, partitionSnapshot->UpperRowIndex - 1);
            }
        }

        const auto& client = ClientDirectory_->GetClientOrThrow(queueRef.Cluster);
        auto result = NDetail::CollectCumulativeDataWeights(queueRef.Path, client, tabletAndRowIndices, Logger);

        for (const auto& [tabletIndex, cumulativeDataWeight] : result) {
            auto& partitionSnapshot = QueueSnapshot_->PartitionSnapshots[tabletIndex];
            partitionSnapshot->CumulativeDataWeight = cumulativeDataWeight;
            partitionSnapshot->WriteRate.DataWeight.Update(partitionSnapshot->CumulativeDataWeight);
        }

        YT_LOG_DEBUG("Consumer cumulative data weights collected");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConsumerSnapshotBuildSession final
{
public:
    TConsumerSnapshotBuildSession(
        TConsumerSnapshotPtr previousConsumerSnapshot,
        IInvokerPtr invoker,
        TLogger logger,
        TClientDirectoryPtr clientDirectory,
        TQueueSnapshotPtr queueSnapshot)
        : PreviousConsumerSnapshot_(std::move(previousConsumerSnapshot))
        , Invoker_(std::move(invoker))
        , Logger(logger)
        , ClientDirectory_(std::move(clientDirectory))
        , QueueSnapshot_(std::move(queueSnapshot))
    { }

    TConsumerSnapshotPtr Build()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        try {
            DoBuild();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_DEBUG(error, "Error building consumer snapshot");
            ConsumerSnapshot_->Error = std::move(error);
        }

        return ConsumerSnapshot_;
    }

private:
    TConsumerSnapshotPtr PreviousConsumerSnapshot_;
    IInvokerPtr Invoker_;
    TLogger Logger;
    TClientDirectoryPtr ClientDirectory_;
    TQueueSnapshotPtr QueueSnapshot_;

    TConsumerSnapshotPtr ConsumerSnapshot_ = New<TConsumerSnapshot>();

    void DoBuild()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        ConsumerSnapshot_->Row = PreviousConsumerSnapshot_->Row;

        auto consumerRef = ConsumerSnapshot_->Row.Consumer;
        auto queueRef = *ConsumerSnapshot_->Row.TargetQueue;

        ConsumerSnapshot_->TargetQueue = queueRef;
        ConsumerSnapshot_->Vital = ConsumerSnapshot_->Row.Vital.value_or(false);
        ConsumerSnapshot_->Owner = *ConsumerSnapshot_->Row.Owner;

        YT_LOG_DEBUG("Building consumer snapshot");

        if (!ConsumerSnapshot_->Row.Schema) {
            THROW_ERROR_EXCEPTION("Consumer schema is not known yet");
        }

        // Assume partition count to be the same as the partition count in the current queue snapshot.
        auto partitionCount = QueueSnapshot_->PartitionCount;
        ConsumerSnapshot_->PartitionCount = partitionCount;

        // Allocate partition snapshots.
        ConsumerSnapshot_->PartitionSnapshots.resize(ConsumerSnapshot_->PartitionCount);
        for (auto& consumerPartitionSnapshot : ConsumerSnapshot_->PartitionSnapshots) {
            consumerPartitionSnapshot = New<TConsumerPartitionSnapshot>();
            consumerPartitionSnapshot->NextRowIndex = 0;
        }

        // Collect partition infos from the consumer table.
        {
            auto client = ClientDirectory_->GetClientOrThrow(consumerRef.Cluster);
            auto consumerClient = CreateConsumerClient(consumerRef.Path, *ConsumerSnapshot_->Row.Schema);

            auto consumerPartitionInfos = WaitFor(consumerClient->CollectPartitions(client, partitionCount, /*withLastConsumeTime*/ true))
                .ValueOrThrow();

            for (const auto& consumerPartitionInfo : consumerPartitionInfos) {
                auto partitionIndex = consumerPartitionInfo.PartitionIndex;

                if (consumerPartitionInfo.PartitionIndex >= partitionCount) {
                    // Probably that is a row for an obsolete partition. Just ignore it.
                    continue;
                }

                auto& consumerPartitionSnapshot = ConsumerSnapshot_->PartitionSnapshots[partitionIndex];

                consumerPartitionSnapshot->NextRowIndex = consumerPartitionInfo.NextRowIndex;
                consumerPartitionSnapshot->LastConsumeTime = consumerPartitionInfo.LastConsumeTime;
            }
        }

        for (const auto& [partitionIndex, consumerPartitionSnapshot] : Enumerate(ConsumerSnapshot_->PartitionSnapshots)) {
            consumerPartitionSnapshot->ConsumeIdleTime = TInstant::Now() - consumerPartitionSnapshot->LastConsumeTime;

            auto previousPartitionSnapshot = (partitionIndex < std::size(PreviousConsumerSnapshot_->PartitionSnapshots))
                ? PreviousConsumerSnapshot_->PartitionSnapshots[partitionIndex]
                : nullptr;

            if (previousPartitionSnapshot) {
                consumerPartitionSnapshot->ReadRate = previousPartitionSnapshot->ReadRate;
            }

            consumerPartitionSnapshot->ReadRate.RowCount.Update(consumerPartitionSnapshot->NextRowIndex);

            const auto& queuePartitionSnapshot = QueueSnapshot_->PartitionSnapshots[partitionIndex];
            if (queuePartitionSnapshot->Error.IsOK()) {
                // NB: may be negative if the consumer is ahead of the partition.
                consumerPartitionSnapshot->UnreadRowCount = queuePartitionSnapshot->UpperRowIndex - consumerPartitionSnapshot->NextRowIndex;

                if (consumerPartitionSnapshot->UnreadRowCount < 0) {
                    consumerPartitionSnapshot->Disposition = EConsumerPartitionDisposition::Ahead;
                } else if (consumerPartitionSnapshot->UnreadRowCount == 0) {
                    consumerPartitionSnapshot->Disposition = EConsumerPartitionDisposition::UpToDate;
                } else if (consumerPartitionSnapshot->UnreadRowCount <= queuePartitionSnapshot->AvailableRowCount) {
                    consumerPartitionSnapshot->Disposition = EConsumerPartitionDisposition::PendingConsumption;
                } else if (consumerPartitionSnapshot->UnreadRowCount > queuePartitionSnapshot->AvailableRowCount) {
                    consumerPartitionSnapshot->Disposition = EConsumerPartitionDisposition::Expired;
                } else {
                    Y_UNREACHABLE();
                }
            } else {
                consumerPartitionSnapshot->Error = queuePartitionSnapshot->Error;
            }
        }

        std::vector<TFuture<void>> futures;

        if (QueueSnapshot_->HasTimestampColumn) {
            futures.emplace_back(BIND(&TConsumerSnapshotBuildSession::CollectTimestamps, MakeStrong(this))
                .AsyncVia(Invoker_)
                .Run());
        }

        if (QueueSnapshot_->HasCumulativeDataWeightColumn) {
            futures.emplace_back(BIND(&TConsumerSnapshotBuildSession::CollectCumulativeDataWeights, MakeStrong(this))
                .AsyncVia(Invoker_)
                .Run());
        }

        WaitFor(AllSucceeded(futures))
            .ThrowOnError();

        for (const auto& consumerPartitionSnapshot : ConsumerSnapshot_->PartitionSnapshots) {
            // If consumer has read all rows in the partition, we assume its processing lag to be zero;
            // otherwise the processing lag is defined as the duration since the commit time of the next
            // row in the partition to be read by the consumer.
            consumerPartitionSnapshot->ProcessingLag = consumerPartitionSnapshot->NextRowCommitTime
                ? TInstant::Now() - *consumerPartitionSnapshot->NextRowCommitTime
                : TDuration::Zero();
            ConsumerSnapshot_->ReadRate += consumerPartitionSnapshot->ReadRate;
        }

        YT_LOG_DEBUG("Consumer snapshot built");
    }

    void CollectTimestamps()
    {
        YT_LOG_DEBUG("Collecting consumer timestamps");

        auto queueRef = *ConsumerSnapshot_->Row.TargetQueue;

        TStringBuilder queryBuilder;
        queryBuilder.AppendFormat("[$tablet_index], [$timestamp] from [%v] where ([$tablet_index], [$row_index]) in (",
            QueueSnapshot_->Row.Queue.Path);
        bool isFirstTuple = true;

        for (const auto& [partitionIndex, consumerPartitionSnapshot] : Enumerate(ConsumerSnapshot_->PartitionSnapshots)) {
            if (consumerPartitionSnapshot->Error.IsOK() && consumerPartitionSnapshot->Disposition == EConsumerPartitionDisposition::PendingConsumption) {
                if (!isFirstTuple) {
                    queryBuilder.AppendString(", ");
                }
                queryBuilder.AppendFormat("(%vu, %vu)", partitionIndex, consumerPartitionSnapshot->NextRowIndex );
                isFirstTuple = false;
            }
        }

        queryBuilder.AppendString(")");

        if (isFirstTuple) {
            return;
        }

        // Calculate next row commit times and processing lags.
        const auto& client = ClientDirectory_->GetClientOrThrow(queueRef.Cluster);
        auto query = queryBuilder.Flush();
        YT_LOG_TRACE("Executing query for next row commit times (Query: %Qv)", query);
        auto result = WaitFor(client->SelectRows(query))
            .ValueOrThrow();

        for (const auto& row : result.Rowset->GetRows()) {
            YT_VERIFY(row.GetCount() == 2);
            YT_VERIFY(row[0].Type == EValueType::Int64);
            auto tabletIndex = row[0].Data.Int64;

            auto& consumerPartitionSnapshot = ConsumerSnapshot_->PartitionSnapshots[tabletIndex];

            YT_VERIFY(row[1].Type == EValueType::Uint64);
            auto commitTimestamp = row[1].Data.Uint64;
            consumerPartitionSnapshot->NextRowCommitTime = TimestampToInstant(commitTimestamp).first;
        }

        YT_LOG_DEBUG("Consumer timestamps collected");
    }

    void CollectCumulativeDataWeights()
    {
        YT_LOG_DEBUG("Collecting consumer cumulative data weights");

        auto queueRef = *ConsumerSnapshot_->Row.TargetQueue;

        std::vector<std::pair<int, i64>> tabletAndRowIndices;

        for (const auto& [partitionIndex, consumerPartitionSnapshot] : Enumerate(ConsumerSnapshot_->PartitionSnapshots)) {
            const auto& queuePartitionSnapshot = QueueSnapshot_->PartitionSnapshots[partitionIndex];
            // Partition should not be erroneous and previous row should exist.
            if (consumerPartitionSnapshot->Error.IsOK() &&
                consumerPartitionSnapshot->NextRowIndex > queuePartitionSnapshot->LowerRowIndex &&
                consumerPartitionSnapshot->NextRowIndex <= queuePartitionSnapshot->UpperRowIndex)
            {
                tabletAndRowIndices.emplace_back(partitionIndex, consumerPartitionSnapshot->NextRowIndex - 1);
            }
        }

        const auto& client = ClientDirectory_->GetClientOrThrow(queueRef.Cluster);
        auto result = NDetail::CollectCumulativeDataWeights(queueRef.Path, client, tabletAndRowIndices, Logger);

        for (const auto& [tabletIndex, cumulativeDataWeight] : result) {
            auto& consumerPartitionSnapshot = ConsumerSnapshot_->PartitionSnapshots[tabletIndex];
            consumerPartitionSnapshot->CumulativeDataWeight = cumulativeDataWeight;
            consumerPartitionSnapshot->ReadRate.DataWeight.Update(consumerPartitionSnapshot->CumulativeDataWeight);
        }

        YT_LOG_DEBUG("Consumer cumulative data weights collected");
    }
};

////////////////////////////////////////////////////////////////////////////////

using TConsumerSnapshotMap = THashMap<TCrossClusterReference, TConsumerSnapshotPtr>;

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicTableController
    : public IQueueController
{
public:
    TOrderedDynamicTableController(
        TQueueControllerDynamicConfigPtr dynamicConfig,
        TClientDirectoryPtr clientDirectory,
        TCrossClusterReference queueRef,
        TQueueTableRow queueRow,
        TConsumerRowMap consumerRowMap,
        IInvokerPtr invoker)
        : DynamicConfig_(std::move(dynamicConfig))
        , ClientDirectory_(std::move(clientDirectory))
        , QueueRef_(std::move(queueRef))
        , Invoker_(std::move(invoker))
        , Logger(QueueAgentLogger.WithTag("Queue: %Qv", QueueRef_))
        , PassExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TOrderedDynamicTableController::Pass, MakeWeak(this)),
            DynamicConfig_->PassPeriod))
        , ProfileManager_(CreateQueueProfileManager(
            Invoker_,
            QueueAgentProfiler
                .WithPrefix("/controller")
                .WithTag("queue_path", QueueRef_.Path)
                .WithTag("queue_cluster", QueueRef_.Cluster)))
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

        QueueSnapshot_->ConsumerSnapshots = ConsumerSnapshots_;
    }

    EQueueFamily GetQueueFamily() const override
    {
        return EQueueFamily::OrderedDynamicTable;
    }

    void BuildOrchid(TFluentMap fluent) const override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Building queue controller orchid (PassIndex: %v)", PassIndex_ - 1);

        fluent
            .Item("pass_index").Value(PassIndex_)
            .Item("pass_instant").Value(PassInstant_)
            .Item("row").Value(QueueSnapshot_->Row)
            .Item("status").Do(std::bind(BuildQueueStatusYson, QueueSnapshot_, _1))
            .Item("partitions").Do(std::bind(BuildQueuePartitionListYson, QueueSnapshot_, _1));
    }

    void BuildConsumerOrchid(const TCrossClusterReference& consumerRef, TFluentMap fluent) const override
    {
        YT_LOG_DEBUG("Building consumer controller orchid (Consumer: %Qv, PassIndex: %v)", consumerRef, PassIndex_ - 1);

        const auto& consumerSnapshot = GetOrCrash(ConsumerSnapshots_, consumerRef);

        fluent
            .Item("pass_index").Value(PassIndex_)
            .Item("pass_instant").Value(PassInstant_)
            .Item("row").Value(consumerSnapshot->Row)
            .Item("status").Do(std::bind(BuildConsumerStatusYson, consumerSnapshot, _1))
            .Item("partitions").Do(std::bind(BuildConsumerPartitionListYson, consumerSnapshot, _1));
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Queue controller started");

        PassExecutor_->Start();
    }

    TFuture<void> Stop() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Queue controller stopped");

        return PassExecutor_->Stop();
    }

    IInvokerPtr GetInvoker() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Invoker_;
    }

    void OnDynamicConfigChanged(
        const TQueueControllerDynamicConfigPtr& oldConfig,
        const TQueueControllerDynamicConfigPtr& newConfig) override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        DynamicConfig_ = newConfig;

        PassExecutor_->SetPeriod(newConfig->PassPeriod);

        YT_LOG_DEBUG(
            "Updated queue controller dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

private:
    TQueueControllerDynamicConfigPtr DynamicConfig_;
    const TClientDirectoryPtr ClientDirectory_;
    const TCrossClusterReference QueueRef_;

    TQueueSnapshotPtr QueueSnapshot_;
    TConsumerSnapshotMap ConsumerSnapshots_;

    const IInvokerPtr Invoker_;

    const TLogger Logger;

    const TPeriodicExecutorPtr PassExecutor_;

    IQueueProfileManagerPtr ProfileManager_;

    i64 PassIndex_ = 0;
    TInstant PassInstant_ = TInstant::Zero();

    void Pass()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("QueueController"));

        auto nextPassInstant = TInstant::Now();
        auto nextPassIndex = PassIndex_ + 1;

        YT_LOG_INFO("Controller pass started (NextPassIndex: %v)", nextPassIndex);
        auto logFinally = Finally([&] {
            YT_LOG_INFO("Controller pass finished (PassIndex: %v -> %v)", PassIndex_, nextPassIndex);
            PassIndex_ = nextPassIndex;
            PassInstant_ = nextPassInstant;
        });

        auto previousQueueSnapshot = UpdateSnapshots();

        ProfileManager_->Profile(previousQueueSnapshot, QueueSnapshot_);
    }

    //! Builds new queue/consumer snapshots and returns the old queue snapshot.
    TQueueSnapshotPtr UpdateSnapshots()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_LOG_INFO("Updating controller snapshots");

        // First, update queue snapshot.
        auto nextQueueSnapshot = TQueueSnapshotBuildSession(
            QueueSnapshot_,
            Invoker_,
            Logger,
            ClientDirectory_)
            .Build();

        // Second, update consumer snapshots.
        TConsumerSnapshotMap nextConsumerSnapshots;
        {
            std::vector<TFuture<TConsumerSnapshotPtr>> consumerSnapshotFutures;

            auto consumerCount = ConsumerSnapshots_.size();
            consumerSnapshotFutures.reserve(consumerCount);

            for (const auto& [consumerRef, consumerSnapshot] : ConsumerSnapshots_) {
                auto session = New<TConsumerSnapshotBuildSession>(
                    consumerSnapshot,
                    Invoker_,
                    Logger.WithTag("Consumer: %Qv", consumerRef),
                    ClientDirectory_,
                    nextQueueSnapshot);
                consumerSnapshotFutures.emplace_back(BIND(&TConsumerSnapshotBuildSession::Build, session)
                    .AsyncVia(Invoker_)
                    .Run());
            }

            // None of snapshot update methods may throw.
            YT_VERIFY(WaitFor(AllSucceeded(consumerSnapshotFutures)).IsOK());

            for (const auto& consumerSnapshotFuture : consumerSnapshotFutures) {
                const auto& consumerSnapshot = consumerSnapshotFuture.Get().Value();
                nextConsumerSnapshots[consumerSnapshot->Row.Consumer] = consumerSnapshot;
            }
        }

        // By this moment we may be sure that updating succeeds.

        // Connect queue snapshot to consumer snapshots with pointers.
        nextQueueSnapshot->ConsumerSnapshots = nextConsumerSnapshots;

        // NB: these swaps must be performed without context switches in order to not expose the partially altered state.
        ConsumerSnapshots_.swap(nextConsumerSnapshots);
        QueueSnapshot_.Swap(nextQueueSnapshot);

        YT_LOG_INFO("Controller snapshots updated");

        // nextQueueSnapshot is actually a previous queue snapshot now.
        return nextQueueSnapshot;
    }
};

DEFINE_REFCOUNTED_TYPE(TOrderedDynamicTableController)

////////////////////////////////////////////////////////////////////////////////

IQueueControllerPtr CreateQueueController(
    TQueueControllerDynamicConfigPtr dynamicConfig,
    NHiveClient::TClientDirectoryPtr clientDirectory,
    TCrossClusterReference queueRef,
    EQueueFamily queueFamily,
    TQueueTableRow queueRow,
    THashMap<TCrossClusterReference, TConsumerTableRow> consumerRefToRow,
    IInvokerPtr invoker)
{
    switch (queueFamily) {
        case EQueueFamily::OrderedDynamicTable:
            return New<TOrderedDynamicTableController>(
                std::move(dynamicConfig),
                std::move(clientDirectory),
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
