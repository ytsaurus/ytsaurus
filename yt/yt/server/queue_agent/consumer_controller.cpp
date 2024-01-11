#include "consumer_controller.h"

#include "snapshot.h"
#include "snapshot_representation.h"
#include "config.h"
#include "helpers.h"
#include "profile_manager.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

#include <yt/yt/client/queue_client/consumer_client.h>
#include <yt/yt/client/queue_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NQueueAgent {

using namespace NApi;
using namespace NHiveClient;
using namespace NLogging;
using namespace NQueueClient;
using namespace NConcurrency;
using namespace NTracing;
using namespace NYson;
using namespace NHiveClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;

using namespace std::placeholders;

//////////////////////////////////////////////////////////////////////////////////

class TConsumerSnapshotBuildSession final
{
public:
    TConsumerSnapshotBuildSession(
        TConsumerTableRow row,
        std::optional<TReplicatedTableMappingTableRow> replicatedTableMappingRow,
        TConsumerSnapshotPtr previousConsumerSnapshot,
        std::vector<TConsumerRegistrationTableRow> registrations,
        TLogger logger,
        TQueueAgentClientDirectoryPtr clientDirectory,
        const IObjectStore* store)
        : Row_(std::move(row))
        , ReplicatedTableMappingRow_(std::move(replicatedTableMappingRow))
        , PreviousConsumerSnapshot_(std::move(previousConsumerSnapshot))
        , Registrations_(std::move(registrations))
        , Logger(logger)
        , ClientDirectory_(std::move(clientDirectory))
        , Store_(store)
    { }

    TConsumerSnapshotPtr Build()
    {
        ConsumerSnapshot_->PassIndex = PreviousConsumerSnapshot_->PassIndex + 1;
        ConsumerSnapshot_->PassInstant = TInstant::Now();
        ConsumerSnapshot_->Row = Row_;
        ConsumerSnapshot_->ReplicatedTableMappingRow = ReplicatedTableMappingRow_;

        try {
            GuardedBuild();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_DEBUG(error, "Error building consumer snapshot");
            ConsumerSnapshot_->Error = std::move(error);
        }

        return ConsumerSnapshot_;
    }

private:
    const TConsumerTableRow Row_;
    const std::optional<TReplicatedTableMappingTableRow> ReplicatedTableMappingRow_;
    const TConsumerSnapshotPtr PreviousConsumerSnapshot_;
    const std::vector<TConsumerRegistrationTableRow> Registrations_;
    const TLogger Logger;
    const TQueueAgentClientDirectoryPtr ClientDirectory_;
    const IObjectStore* Store_;

    IClientPtr Client_;
    IConsumerClientPtr ConsumerClient_;

    TConsumerSnapshotPtr ConsumerSnapshot_ = New<TConsumerSnapshot>();

    void GuardedBuild()
    {
        YT_LOG_DEBUG("Building consumer snapshot (PassIndex: %v)", ConsumerSnapshot_->PassIndex);

        if (Row_.SynchronizationError && !Row_.SynchronizationError->IsOK()) {
            THROW_ERROR TError("Consumer synchronization failed")
                << *Row_.SynchronizationError;
        }

        if (!Row_.RowRevision) {
            THROW_ERROR_EXCEPTION("Consumer is not in-sync yet");
        }
        if (!Row_.ObjectType) {
            THROW_ERROR_EXCEPTION("Consumer object type is not known yet");
        }
        if (!Row_.Schema) {
            THROW_ERROR_EXCEPTION("Consumer schema is not known yet");
        }

        if (IsReplicatedTableObjectType(Row_.ObjectType) && !ReplicatedTableMappingRow_) {
            THROW_ERROR_EXCEPTION("No replicated table mapping row is known for replicated consumer");
        }
        if (ReplicatedTableMappingRow_) {
            ReplicatedTableMappingRow_->Validate();
        }

        auto consumerRef = ConsumerSnapshot_->Row.Ref;

        auto clientContext = ClientDirectory_->GetDataReadContext(ConsumerSnapshot_, /*onlyDataReplicas*/ true);
        Client_ = clientContext.Client;
        ConsumerClient_ = CreateConsumerClient(Client_, clientContext.Path, *ConsumerSnapshot_->Row.Schema);

        THashMap<TCrossClusterReference, TFuture<TSubConsumerSnapshotPtr>> subSnapshotFutures;
        for (const auto& registration : Registrations_) {
            auto queueRef = registration.Queue;
            auto queueSnapshot = DynamicPointerCast<const TQueueSnapshot>(Store_->FindSnapshot(queueRef));
            if (!queueSnapshot) {
                YT_LOG_DEBUG("Snapshot is missing for the queue while building subconsumer snapshot (Queue: %v)", queueRef);
                auto errorQueueSnapshot = New<TQueueSnapshot>();
                errorQueueSnapshot->Error = TError("Queue %Qv snapshot is missing", queueRef);
                queueSnapshot = std::move(errorQueueSnapshot);
            }
            subSnapshotFutures[queueRef] = BIND(
                &TConsumerSnapshotBuildSession::BuildSubConsumerSnapshot,
                MakeStrong(this),
                queueRef,
                Passed(std::move(queueSnapshot)))
                .AsyncVia(GetCurrentInvoker())
                .Run();
        }

        // Cannot throw unless cancellation occurs.
        WaitFor(AllSet(GetValues(subSnapshotFutures)))
            .ThrowOnError();

        for (const auto& [queueRef, subConsumerSnapshotFuture] : subSnapshotFutures) {
            YT_VERIFY(subConsumerSnapshotFuture.IsSet());
            auto subConsumerSnapshotOrError = subConsumerSnapshotFuture.GetUnique();
            TSubConsumerSnapshotPtr subConsumerSnapshot;
            if (subConsumerSnapshotOrError.IsOK()) {
                subConsumerSnapshot = std::move(subConsumerSnapshotOrError.Value());
            } else {
                YT_LOG_DEBUG(subConsumerSnapshotOrError, "Error building subconsumer snapshot (Queue: %v)", queueRef);
                subConsumerSnapshot = New<TSubConsumerSnapshot>();
                subConsumerSnapshot->Error = std::move(subConsumerSnapshotOrError);
            }
            ConsumerSnapshot_->SubSnapshots[queueRef] = std::move(subConsumerSnapshot);
        }

        ConsumerSnapshot_->Registrations = Registrations_;

        YT_LOG_DEBUG("Consumer snapshot built");
    }

    TSubConsumerSnapshotPtr BuildSubConsumerSnapshot(
        TCrossClusterReference queueRef,
        TQueueSnapshotConstPtr queueSnapshot)
    {
        auto Logger = this->Logger.WithTag("Queue: %v", queueRef);

        YT_LOG_DEBUG("Building subconsumer snapshot (PassIndex: %v)", ConsumerSnapshot_->PassIndex);
        auto logFinally = Finally([&] {
            YT_LOG_DEBUG("Subconsumer snapshot built");
        });

        auto subSnapshot = New<TSubConsumerSnapshot>();

        if (!queueSnapshot->Error.IsOK()) {
            subSnapshot->Error = queueSnapshot->Error;
            return subSnapshot;
        }

        subSnapshot->HasCumulativeDataWeightColumn = queueSnapshot->HasCumulativeDataWeightColumn;

        // Assume partition count to be the same as the partition count in the current queue snapshot.
        auto partitionCount = queueSnapshot->PartitionCount;
        subSnapshot->PartitionCount = partitionCount;
        // Allocate partition snapshots.
        subSnapshot->PartitionSnapshots.resize(partitionCount);
        for (auto& consumerPartitionSnapshot : subSnapshot->PartitionSnapshots) {
            consumerPartitionSnapshot = New<TConsumerPartitionSnapshot>();
            consumerPartitionSnapshot->NextRowIndex = 0;
        }

        // Collect partition infos from the consumer table.

        {
            auto subConsumerClient = ConsumerClient_->GetSubConsumerClient(/*queueClient*/ nullptr, queueRef);

            auto consumerPartitionInfos = WaitFor(subConsumerClient->CollectPartitions(partitionCount, /*withLastConsumeTime*/ true))
                .ValueOrThrow();

            for (const auto& consumerPartitionInfo : consumerPartitionInfos) {
                auto partitionIndex = consumerPartitionInfo.PartitionIndex;

                if (consumerPartitionInfo.PartitionIndex >= partitionCount) {
                    // Probably that is a row for an obsolete partition. Just ignore it.
                    continue;
                }

                auto& subConsumerPartitionSnapshot = subSnapshot->PartitionSnapshots[partitionIndex];

                subConsumerPartitionSnapshot->NextRowIndex = consumerPartitionInfo.NextRowIndex;
                subConsumerPartitionSnapshot->LastConsumeTime = consumerPartitionInfo.LastConsumeTime;

                if (consumerPartitionInfo.ConsumerMeta) {
                    if (auto cumulativeDataWeight = consumerPartitionInfo.ConsumerMeta->CumulativeDataWeight) {
                        subConsumerPartitionSnapshot->CumulativeDataWeight = cumulativeDataWeight;
                        subConsumerPartitionSnapshot->ReadRate.DataWeight.Update(*cumulativeDataWeight);
                    }
                    if (consumerPartitionInfo.ConsumerMeta->OffsetTimestamp) {
                        subConsumerPartitionSnapshot->NextRowCommitTime = TimestampToInstant(*consumerPartitionInfo.ConsumerMeta->OffsetTimestamp).first;
                    }
                }
            }
        }

        for (const auto& [partitionIndex, subConsumerPartitionSnapshot] : Enumerate(subSnapshot->PartitionSnapshots)) {
            subConsumerPartitionSnapshot->ConsumeIdleTime = TInstant::Now() - subConsumerPartitionSnapshot->LastConsumeTime;

            TConsumerPartitionSnapshotPtr previousPartitionSnapshot = nullptr;
            if (auto it = PreviousConsumerSnapshot_->SubSnapshots.find(queueRef); it != PreviousConsumerSnapshot_->SubSnapshots.end()) {
                const auto& previousSubSnapshot = it->second;
                if (partitionIndex < previousSubSnapshot->PartitionSnapshots.size()) {
                    previousPartitionSnapshot = previousSubSnapshot->PartitionSnapshots[partitionIndex];
                }
            }

            if (previousPartitionSnapshot) {
                subConsumerPartitionSnapshot->ReadRate = previousPartitionSnapshot->ReadRate;
            }

            subConsumerPartitionSnapshot->ReadRate.RowCount.Update(subConsumerPartitionSnapshot->NextRowIndex);

            const auto& queuePartitionSnapshot = queueSnapshot->PartitionSnapshots[partitionIndex];
            if (queuePartitionSnapshot->Error.IsOK()) {
                // NB: may be negative if the consumer is ahead of the partition.
                subConsumerPartitionSnapshot->UnreadRowCount =
                    queuePartitionSnapshot->UpperRowIndex - subConsumerPartitionSnapshot->NextRowIndex;

                // TODO(max42): this seems to not work properly as cumulative data weight is not set yet.
                // Re-think this code when working on new profiling.
                subConsumerPartitionSnapshot->UnreadDataWeight = OptionalSub(
                    queuePartitionSnapshot->CumulativeDataWeight,
                    subConsumerPartitionSnapshot->CumulativeDataWeight);

                if (subConsumerPartitionSnapshot->UnreadRowCount < 0) {
                    subConsumerPartitionSnapshot->Disposition = EConsumerPartitionDisposition::Ahead;
                } else if (subConsumerPartitionSnapshot->UnreadRowCount == 0) {
                    subConsumerPartitionSnapshot->Disposition = EConsumerPartitionDisposition::UpToDate;
                } else if (subConsumerPartitionSnapshot->UnreadRowCount <= queuePartitionSnapshot->AvailableRowCount) {
                    subConsumerPartitionSnapshot->Disposition = EConsumerPartitionDisposition::PendingConsumption;
                } else if (subConsumerPartitionSnapshot->UnreadRowCount > queuePartitionSnapshot->AvailableRowCount) {
                    subConsumerPartitionSnapshot->Disposition = EConsumerPartitionDisposition::Expired;
                } else {
                    Y_UNREACHABLE();
                }
            } else {
                subConsumerPartitionSnapshot->Error = queuePartitionSnapshot->Error;
            }
        }

        std::vector<TFuture<void>> futures;

        if (queueSnapshot->HasTimestampColumn) {
            futures.emplace_back(BIND(&TConsumerSnapshotBuildSession::CollectTimestamps, MakeStrong(this), Logger, queueSnapshot, subSnapshot)
                .AsyncVia(GetCurrentInvoker())
                .Run());
        }

        if (queueSnapshot->HasCumulativeDataWeightColumn) {
            futures.emplace_back(BIND(&TConsumerSnapshotBuildSession::CollectCumulativeDataWeights, MakeStrong(this), Logger, queueSnapshot, subSnapshot)
                .AsyncVia(GetCurrentInvoker())
                .Run());
        }

        WaitFor(AllSucceeded(futures))
            .ThrowOnError();

        for (const auto& subConsumerPartitionSnapshot : subSnapshot->PartitionSnapshots) {
            // If consumer has read all rows in the partition, we assume its processing lag to be zero;
            // otherwise the processing lag is defined as the duration since the commit time of the next
            // row in the partition to be read by the consumer.
            subConsumerPartitionSnapshot->ProcessingLag = subConsumerPartitionSnapshot->NextRowCommitTime
                ? TInstant::Now() - *subConsumerPartitionSnapshot->NextRowCommitTime
                : TDuration::Zero();
            subSnapshot->ReadRate += subConsumerPartitionSnapshot->ReadRate;
        }

        return subSnapshot;
    }

    void CollectTimestamps(
        const TLogger& logger,
        const TQueueSnapshotConstPtr& queueSnapshot,
        const TSubConsumerSnapshotPtr& subSnapshot)
    {
        const auto& Logger = logger;

        // TODO(nadya73): Use CollectPartitionRowInfos.
        YT_LOG_DEBUG("Collecting consumer timestamps");

        auto clientContext = ClientDirectory_->GetDataReadContext(queueSnapshot);

        TStringBuilder queryBuilder;
        queryBuilder.AppendFormat("[$tablet_index], [$timestamp] from [%v] where ([$tablet_index], [$row_index]) in (",
            clientContext.Path);
        bool isFirstTuple = true;

        for (const auto& [partitionIndex, consumerPartitionSnapshot] : Enumerate(subSnapshot->PartitionSnapshots)) {
            if (consumerPartitionSnapshot->Error.IsOK() && consumerPartitionSnapshot->Disposition == EConsumerPartitionDisposition::PendingConsumption) {
                if (!isFirstTuple) {
                    queryBuilder.AppendString(", ");
                }
                queryBuilder.AppendFormat("(%vu, %vu)", partitionIndex, consumerPartitionSnapshot->NextRowIndex);
                isFirstTuple = false;
            }
        }

        queryBuilder.AppendString(")");

        if (isFirstTuple) {
            return;
        }

        // Calculate next row commit times and processing lags.
        auto query = queryBuilder.Flush();
        TSelectRowsOptions options;
        options.ReplicaConsistency = EReplicaConsistency::Sync;
        YT_LOG_TRACE("Executing query for next row commit times (Query: %v)", query);
        auto result = WaitFor(clientContext.Client->SelectRows(query))
            .ValueOrThrow();

        for (const auto& row : result.Rowset->GetRows()) {
            YT_VERIFY(row.GetCount() == 2);
            auto tabletIndex = FromUnversionedValue<i64>(row[0]);

            auto& consumerPartitionSnapshot = subSnapshot->PartitionSnapshots[tabletIndex];

            if (auto commitTimestamp = FromUnversionedValue<std::optional<ui64>>(row[1])) {
                consumerPartitionSnapshot->NextRowCommitTime = TimestampToInstant(*commitTimestamp).first;
            }
        }

        YT_LOG_DEBUG("Consumer timestamps collected");
    }

    void CollectCumulativeDataWeights(
        const TLogger& logger,
        const TQueueSnapshotConstPtr& queueSnapshot,
        const TSubConsumerSnapshotPtr& subSnapshot)
    {
        const auto& Logger = logger;

        YT_LOG_DEBUG("Collecting consumer cumulative data weights");

        std::vector<std::pair<int, i64>> tabletAndRowIndices;

        for (const auto& [partitionIndex, consumerPartitionSnapshot] : Enumerate(subSnapshot->PartitionSnapshots)) {
            const auto& queuePartitionSnapshot = queueSnapshot->PartitionSnapshots[partitionIndex];
            // Partition should not be erroneous and previous row should exist.
            if (consumerPartitionSnapshot->Error.IsOK() &&
                consumerPartitionSnapshot->NextRowIndex > queuePartitionSnapshot->LowerRowIndex)
            {
                tabletAndRowIndices.emplace_back(partitionIndex, consumerPartitionSnapshot->NextRowIndex - 1);
            }
        }

        auto clientContext = ClientDirectory_->GetDataReadContext(queueSnapshot);

        auto params = TCollectPartitionRowInfoParams{
            .HasCumulativeDataWeightColumn = true,
        };

        auto result = NQueueClient::CollectPartitionRowInfos(clientContext.Path, clientContext.Client, tabletAndRowIndices, params, Logger);

        for (const auto& [tabletIndex, partitionInfo] : result) {
            auto& consumerPartitionSnapshot = subSnapshot->PartitionSnapshots[tabletIndex];

            const auto& partitionRowInfo = partitionInfo.begin()->second;

            if (!consumerPartitionSnapshot->CumulativeDataWeight && partitionRowInfo.CumulativeDataWeight) {
                consumerPartitionSnapshot->CumulativeDataWeight = *partitionRowInfo.CumulativeDataWeight;
                consumerPartitionSnapshot->ReadRate.DataWeight.Update(*partitionRowInfo.CumulativeDataWeight);

                consumerPartitionSnapshot->UnreadDataWeight = OptionalSub(
                    queueSnapshot->PartitionSnapshots[tabletIndex]->CumulativeDataWeight,
                    consumerPartitionSnapshot->CumulativeDataWeight);

            }
        }

        YT_LOG_DEBUG("Consumer cumulative data weights collected");
    }
};

class TConsumerController
    : public IObjectController
{
public:
    TConsumerController(
        bool leading,
        const TConsumerTableRow& row,
        const std::optional<TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
        const IObjectStore* store,
        const TQueueControllerDynamicConfigPtr& dynamicConfig,
        TQueueAgentClientDirectoryPtr clientDirectory,
        IInvokerPtr invoker)
        : Leading_(leading)
        , ConsumerRow_(row)
        , ReplicatedTableMappingRow_(replicatedTableMappingRow)
        , ConsumerRef_(row.Ref)
        , ObjectStore_(store)
        , DynamicConfig_(dynamicConfig)
        , ClientDirectory_(std::move(clientDirectory))
        , Invoker_(std::move(invoker))
        , Logger(QueueAgentLogger.WithTag("Consumer: %v, Leading: %v", ConsumerRef_, Leading_))
        , PassExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TConsumerController::Pass, MakeWeak(this)),
            dynamicConfig->PassPeriod))
        , ProfileManager_(CreateConsumerProfileManager(
            QueueAgentProfilerGlobal
                .WithRequiredTag("consumer_path", ConsumerRef_.Path)
                .WithRequiredTag("consumer_cluster", ConsumerRef_.Cluster),
            Logger))
    {
        // Prepare initial erroneous snapshot.
        auto consumerSnapshot = New<TConsumerSnapshot>();
        consumerSnapshot->Row = row;
        consumerSnapshot->ReplicatedTableMappingRow = replicatedTableMappingRow;
        consumerSnapshot->Error = TError("Consumer is not processed yet");
        ConsumerSnapshot_.Exchange(std::move(consumerSnapshot));

        YT_LOG_INFO("Consumer controller started");

        PassExecutor_->Start();
    }

    void OnRowUpdated(std::any row) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& consumerRow = std::any_cast<const TConsumerTableRow&>(row);

        ConsumerRow_.Store(consumerRow);
    }

    void OnReplicatedTableMappingRowUpdated(const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& row) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ReplicatedTableMappingRow_.Store(row);
    }

    void OnDynamicConfigChanged(
        const TQueueControllerDynamicConfigPtr& oldConfig,
        const TQueueControllerDynamicConfigPtr& newConfig) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DynamicConfig_.Exchange(newConfig);

        PassExecutor_->SetPeriod(newConfig->PassPeriod);

        YT_LOG_DEBUG(
            "Updated consumer controller dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

    TRefCountedPtr GetLatestSnapshot() const override
    {
        return ConsumerSnapshot_.Acquire();
    }

    void BuildOrchid(IYsonConsumer* consumer) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto consumerSnapshot = ConsumerSnapshot_.Acquire();

        YT_LOG_DEBUG("Building consumer controller orchid (PassIndex: %v)", consumerSnapshot->PassIndex);

        BuildYsonFluently(consumer).BeginMap()
            .Item("leading").Value(Leading_)
            .Item("pass_index").Value(consumerSnapshot->PassIndex)
            .Item("pass_instant").Value(consumerSnapshot->PassInstant)
            .Item("row").Value(consumerSnapshot->Row)
            .Item("replicated_table_mapping_row").Value(consumerSnapshot->ReplicatedTableMappingRow)
            .Item("status").Do(std::bind(BuildConsumerStatusYson, consumerSnapshot, _1))
            .Item("partitions").Do(std::bind(BuildConsumerPartitionListYson, consumerSnapshot, _1))
        .EndMap();
    }

    bool IsLeading() const override
    {
        return Leading_;
    }

private:
    bool Leading_;
    TAtomicObject<TConsumerTableRow> ConsumerRow_;
    TAtomicObject<std::optional<TReplicatedTableMappingTableRow>> ReplicatedTableMappingRow_;
    const TCrossClusterReference ConsumerRef_;
    const IObjectStore* ObjectStore_;
    using TQueueControllerDynamicConfigAtomicPtr = TAtomicIntrusivePtr<TQueueControllerDynamicConfig>;
    TQueueControllerDynamicConfigAtomicPtr DynamicConfig_;

    const TQueueAgentClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr Invoker_;

    using TConsumerSnapshotAtomicPtr = TAtomicIntrusivePtr<TConsumerSnapshot>;
    TConsumerSnapshotAtomicPtr ConsumerSnapshot_;

    const TLogger Logger;
    const TPeriodicExecutorPtr PassExecutor_;
    const IConsumerProfileManagerPtr ProfileManager_;

    void Pass()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("ConsumerControllerPass"));

        YT_LOG_INFO("Consumer controller pass started");

        auto registrations = ObjectStore_->GetRegistrations(ConsumerRef_, EObjectKind::Consumer);
        YT_LOG_INFO("Registrations fetched (RegistrationCount: %v)", registrations.size());
        for (const auto& registration : registrations) {
            YT_LOG_DEBUG(
                "Relevant registration (Queue: %v, Consumer: %v, Vital: %v)",
                registration.Queue,
                registration.Consumer,
                registration.Vital);
        }

        auto nextConsumerSnapshot = New<TConsumerSnapshotBuildSession>(
            ConsumerRow_.Load(),
            ReplicatedTableMappingRow_.Load(),
            /*previousConsumerSnapshot*/ ConsumerSnapshot_.Acquire(),
            std::move(registrations),
            Logger,
            ClientDirectory_,
            ObjectStore_)
            ->Build();
        auto previousConsumerSnapshot = ConsumerSnapshot_.Exchange(nextConsumerSnapshot);

        YT_LOG_INFO("Consumer snapshot updated");

        if (Leading_) {
            YT_LOG_DEBUG("Consumer controller is leading, performing mutating operations");

            ProfileManager_->Profile(previousConsumerSnapshot, nextConsumerSnapshot);
        }

        YT_LOG_INFO("Consumer controller pass finished");
    }
};

////////////////////////////////////////////////////////////////////////////////

bool UpdateConsumerController(
    IObjectControllerPtr& controller,
    bool leading,
    const TConsumerTableRow& row,
    const std::optional<TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
    const IObjectStore* store,
    TQueueControllerDynamicConfigPtr dynamicConfig,
    TQueueAgentClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker)
{
    if (controller && controller->IsLeading() == leading) {
        return false;
    }

    controller = New<TConsumerController>(
        leading,
        row,
        replicatedTableMappingRow,
        store,
        std::move(dynamicConfig),
        std::move(clientDirectory),
        std::move(invoker));
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
