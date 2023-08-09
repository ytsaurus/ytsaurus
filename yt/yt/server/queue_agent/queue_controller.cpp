#include "queue_controller.h"

#include "snapshot.h"
#include "snapshot_representation.h"
#include "config.h"
#include "helpers.h"
#include "profile_manager.h"
#include "yt/yt/client/api/internal_client.h"

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/queue_client/config.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>

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
using namespace NObjectClient;

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////

struct IQueueController
    : public IObjectController
{
    virtual EQueueFamily GetFamily() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueController)

////////////////////////////////////////////////////////////////////////////////

class TQueueSnapshotBuildSession final
{
public:
    TQueueSnapshotBuildSession(
        TQueueTableRow row,
        TQueueSnapshotPtr previousQueueSnapshot,
        std::vector<TConsumerRegistrationTableRow> registrations,
        TLogger logger,
        TClientDirectoryPtr clientDirectory)
        : Row_(std::move(row))
        , PreviousQueueSnapshot_(std::move(previousQueueSnapshot))
        , Registrations_(std::move(registrations))
        , Logger(logger)
        , ClientDirectory_(std::move(clientDirectory))
    { }

    TQueueSnapshotPtr Build()
    {
        QueueSnapshot_->PassIndex = PreviousQueueSnapshot_->PassIndex + 1;
        QueueSnapshot_->PassInstant = TInstant::Now();
        QueueSnapshot_->Row = Row_;

        try {
            GuardedBuild();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            YT_LOG_DEBUG(error, "Error updating queue snapshot");
            QueueSnapshot_->Error = std::move(error);
        }

        return QueueSnapshot_;
    }

private:
    const TQueueTableRow Row_;
    TQueueSnapshotPtr PreviousQueueSnapshot_;
    std::vector<TConsumerRegistrationTableRow> Registrations_;
    TLogger Logger;
    TClientDirectoryPtr ClientDirectory_;

    TQueueSnapshotPtr QueueSnapshot_ = New<TQueueSnapshot>();

    void GuardedBuild()
    {
        YT_LOG_DEBUG("Building queue snapshot (PassIndex: %v)", QueueSnapshot_->PassIndex);

        auto queueRef = QueueSnapshot_->Row.Ref;

        QueueSnapshot_->Family = EQueueFamily::OrderedDynamicTable;
        auto client = ClientDirectory_->GetClientOrThrow(queueRef.Cluster);
        const auto& tableMountCache = client->GetTableMountCache();
        const auto& cellDirectory = client->GetNativeConnection()->GetCellDirectory();

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
            partitionSnapshots[index]->TabletState = tabletInfo->State;
            if (tabletInfo->State != ETabletState::Mounted && tabletInfo->State != ETabletState::Frozen) {
                partitionSnapshots[index]->Error = TError("Tablet %v is not mounted or frozen", tabletInfo->TabletId)
                    << TErrorAttribute("state", tabletInfo->State);
            } else {
                tabletIndexes.push_back(index);
                const auto& cellId = tabletInfo->CellId;
                std::optional<TString> host;
                if (auto cellDescriptor = cellDirectory->FindDescriptor(cellId)) {
                    for (const auto& peer : cellDescriptor->Peers) {
                        if (peer.GetVoting()) {
                            host = peer.GetDefaultAddress();
                            break;
                        }
                    }
                }
                partitionSnapshots[index]->Meta = BuildYsonStringFluently()
                    .BeginMap()
                        .Item("cell_id").Value(cellId)
                        .Item("host").Value(host)
                    .EndMap();
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

        if (QueueSnapshot_->HasCumulativeDataWeightColumn) {
            CollectCumulativeDataWeights();
        }

        for (int index = 0; index < std::ssize(tabletInfos); ++index) {
            const auto& partitionSnapshot = partitionSnapshots[tabletIndexes[index]];
            QueueSnapshot_->WriteRate += partitionSnapshot->WriteRate;
        }

        QueueSnapshot_->Registrations = Registrations_;

        YT_LOG_DEBUG("Queue snapshot built");
    }

    void CollectCumulativeDataWeights()
    {
        YT_LOG_DEBUG("Collecting queue cumulative data weights");

        auto queueRef = QueueSnapshot_->Row.Ref;

        std::vector<std::pair<int, i64>> tabletAndRowIndices;

        for (const auto& [partitionIndex, partitionSnapshot] : Enumerate(QueueSnapshot_->PartitionSnapshots)) {
            // Partition should not be erroneous and contain at least one row.
            if (partitionSnapshot->Error.IsOK() && partitionSnapshot->UpperRowIndex > 0) {
                tabletAndRowIndices.emplace_back(partitionIndex, partitionSnapshot->LowerRowIndex);
                if (partitionSnapshot->UpperRowIndex - 1 != partitionSnapshot->LowerRowIndex) {
                    tabletAndRowIndices.emplace_back(partitionIndex, partitionSnapshot->UpperRowIndex - 1);
                }
            }
        }

        const auto& client = ClientDirectory_->GetClientOrThrow(queueRef.Cluster);
        auto result = NQueueAgent::CollectCumulativeDataWeights(queueRef.Path, client, tabletAndRowIndices, Logger);

        for (const auto& [tabletIndex, cumulativeDataWeights] : result) {
            auto& partitionSnapshot = QueueSnapshot_->PartitionSnapshots[tabletIndex];

            auto trimmedDataWeightIt = cumulativeDataWeights.find(partitionSnapshot->LowerRowIndex);
            if (trimmedDataWeightIt != cumulativeDataWeights.end()) {
                partitionSnapshot->TrimmedDataWeight = cumulativeDataWeights.find(partitionSnapshot->LowerRowIndex)->second;
            }

            auto cumulativeDataWeightIt = cumulativeDataWeights.find(partitionSnapshot->UpperRowIndex - 1);
            if (cumulativeDataWeightIt != cumulativeDataWeights.end()) {
                partitionSnapshot->CumulativeDataWeight = cumulativeDataWeights.find(partitionSnapshot->UpperRowIndex - 1)->second;
                partitionSnapshot->WriteRate.DataWeight.Update(*partitionSnapshot->CumulativeDataWeight);
            }

            partitionSnapshot->AvailableDataWeight = OptionalSub(
                partitionSnapshot->CumulativeDataWeight,
                partitionSnapshot->TrimmedDataWeight);
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
        bool leading,
        TQueueTableRow queueRow,
        const IObjectStore* store,
        const TQueueControllerDynamicConfigPtr& dynamicConfig,
        TClientDirectoryPtr clientDirectory,
        IInvokerPtr invoker)
        : Leading_(leading)
        , QueueRow_(queueRow)
        , QueueRef_(queueRow.Ref)
        , ObjectStore_(store)
        , DynamicConfig_(dynamicConfig)
        , ClientDirectory_(std::move(clientDirectory))
        , Invoker_(std::move(invoker))
        , Logger(QueueAgentLogger.WithTag("Queue: %v, Leading: %v", QueueRef_, Leading_))
        , PassExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TOrderedDynamicTableController::Pass, MakeWeak(this)),
            TPeriodicExecutorOptions{
                .Period = dynamicConfig->PassPeriod,
                .Splay = dynamicConfig->PassPeriod,
            }))
        , ProfileManager_(CreateQueueProfileManager(
            QueueAgentProfiler
                .WithRequiredTag("queue_path", QueueRef_.Path)
                .WithRequiredTag("queue_cluster", QueueRef_.Cluster),
            Logger))
    {
        // Prepare initial erroneous snapshot.
        auto queueSnapshot = New<TQueueSnapshot>();
        queueSnapshot->Row = std::move(queueRow);
        queueSnapshot->Error = TError("Queue is not processed yet");
        QueueSnapshot_.Exchange(std::move(queueSnapshot));

        YT_LOG_INFO("Queue controller started");

        PassExecutor_->Start();
    }

    void BuildOrchid(IYsonConsumer* consumer) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto queueSnapshot = QueueSnapshot_.Acquire();

        YT_LOG_DEBUG("Building queue controller orchid (PassIndex: %v)", queueSnapshot->PassIndex);

        BuildYsonFluently(consumer).BeginMap()
            .Item("leading").Value(Leading_)
            .Item("pass_index").Value(queueSnapshot->PassIndex)
            .Item("pass_instant").Value(queueSnapshot->PassInstant)
            .Item("row").Value(queueSnapshot->Row)
            .Item("status").Do(std::bind(BuildQueueStatusYson, queueSnapshot, _1))
            .Item("partitions").Do(std::bind(BuildQueuePartitionListYson, queueSnapshot, _1))
        .EndMap();
    }

    void OnRowUpdated(std::any row) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& queueRow = std::any_cast<const TQueueTableRow&>(row);

        QueueRow_.Store(queueRow);
    }

    void OnDynamicConfigChanged(
        const TQueueControllerDynamicConfigPtr& oldConfig,
        const TQueueControllerDynamicConfigPtr& newConfig) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DynamicConfig_.Exchange(newConfig);

        PassExecutor_->SetPeriod(newConfig->PassPeriod);

        YT_LOG_DEBUG(
            "Updated queue controller dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

    TRefCountedPtr GetLatestSnapshot() const override
    {
        return QueueSnapshot_.Acquire();
    }

    EQueueFamily GetFamily() const override
    {
        return EQueueFamily::OrderedDynamicTable;
    }

    bool IsLeading() const override
    {
        return Leading_;
    }

private:
    bool Leading_;
    TAtomicObject<TQueueTableRow> QueueRow_;
    const TCrossClusterReference QueueRef_;
    const IObjectStore* ObjectStore_;

    using TQueueControllerDynamicConfigAtomicPtr = TAtomicIntrusivePtr<TQueueControllerDynamicConfig>;
    TQueueControllerDynamicConfigAtomicPtr DynamicConfig_;

    const TClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr Invoker_;

    using TQueueSnapshotAtomicPtr = TAtomicIntrusivePtr<TQueueSnapshot>;
    TQueueSnapshotAtomicPtr QueueSnapshot_;

    const TLogger Logger;
    const TPeriodicExecutorPtr PassExecutor_;
    IQueueProfileManagerPtr ProfileManager_;

    void Pass()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("QueueControllerPass"));

        YT_LOG_INFO("Queue controller pass started");

        auto registrations = ObjectStore_->GetRegistrations(QueueRef_, EObjectKind::Queue);
        YT_LOG_INFO("Registrations fetched (RegistrationCount: %v)", registrations.size());
        for (const auto& registration : registrations) {
            YT_LOG_DEBUG(
                "Relevant registration (Queue: %v, Consumer: %v, Vital: %v)",
                registration.Queue,
                registration.Consumer,
                registration.Vital);
        }

        auto nextQueueSnapshot = New<TQueueSnapshotBuildSession>(
            QueueRow_.Load(),
            QueueSnapshot_.Acquire(),
            std::move(registrations),
            Logger,
            ClientDirectory_)
            ->Build();
        auto previousQueueSnapshot = QueueSnapshot_.Exchange(nextQueueSnapshot);

        YT_LOG_INFO("Queue snapshot updated");

        if (Leading_) {
            YT_LOG_DEBUG("Queue controller is leading, performing mutating operations");

            ProfileManager_->Profile(previousQueueSnapshot, nextQueueSnapshot);

            if (ShouldTrim(nextQueueSnapshot->PassIndex)) {
                Trim();
            }
        }

        YT_LOG_INFO("Queue controller pass finished");
    }

    bool ShouldTrim(i64 passIndex) const
    {
        auto config = DynamicConfig_.Acquire();

        if (!config->EnableAutomaticTrimming) {
            return false;
        }

        auto trimmingPeriodValue = config->TrimmingPeriod.value_or(config->PassPeriod).GetValue();
        auto passPeriodValue = config->PassPeriod.GetValue();
        auto frequency = (trimmingPeriodValue + passPeriodValue - 1) / passPeriodValue;

        return passIndex % frequency == 0;
    }

    //! Only EAutoTrimPolicy::VitalConsumers is supported right now.
    //!
    //! Trimming is only performed if the queue has at least one vital consumer.
    //! The queue is trimmed up to the smallest NextRowIndex over all vital consumers.
    void Trim()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        try {
            GuardedTrim();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error while trimming queue");
        }
    }

    void GuardedTrim()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        // Guard against context switches, just to be on the safe side.
        auto queueSnapshot = QueueSnapshot_.Acquire();

        if (!queueSnapshot->Error.IsOK()) {
            THROW_ERROR_EXCEPTION(
                "Trimming iteration skipped due to queue error")
                << queueSnapshot->Error;
        }

        auto queueObjectId = queueSnapshot->Row.ObjectId;
        auto queueObjectPath = FromObjectId(*queueObjectId);
        // This field should be initialized when reading from dynamic state.
        if (!queueObjectId) {
            THROW_ERROR_EXCEPTION("Trimming iteration skipped due to the absence of filled field \"object_id\"");
        }

        YT_LOG_DEBUG("Performing trimming iteration (Path: %v)", queueObjectPath);

        const auto& autoTrimConfig = queueSnapshot->Row.AutoTrimConfig;
        // This config should be initialized when reading from dynamic state.
        YT_VERIFY(autoTrimConfig);

        if (!autoTrimConfig->Enable) {
            YT_LOG_DEBUG(
                "Trimming disabled; trimming iteration skipped (AutoTrimConfig: %v)",
                ConvertToYsonString(autoTrimConfig, EYsonFormat::Text));
            return;
        }

        auto registrations = ObjectStore_->GetRegistrations(QueueRef_, EObjectKind::Queue);

        THashMap<TCrossClusterReference, TSubConsumerSnapshotConstPtr> vitalConsumerSubSnapshots;
        vitalConsumerSubSnapshots.reserve(registrations.size());
        for (const auto& registration : registrations) {
            if (!registration.Vital) {
                continue;
            }
            auto consumerSnapshot = DynamicPointerCast<const TConsumerSnapshot>(ObjectStore_->FindSnapshot(registration.Consumer));
            if (!consumerSnapshot) {
                THROW_ERROR_EXCEPTION(
                    "Trimming iteration skipped due to missing registered vital consumer %Qv",
                    registration.Consumer);
            } else if (!consumerSnapshot->Error.IsOK()) {
                THROW_ERROR_EXCEPTION(
                    "Trimming iteration skipped due to erroneous registered vital consumer %Qv",
                    consumerSnapshot->Row.Ref)
                    << consumerSnapshot->Error;
            }
            auto it = consumerSnapshot->SubSnapshots.find(QueueRef_);
            if (it == consumerSnapshot->SubSnapshots.end()) {
                THROW_ERROR_EXCEPTION(
                    "Trimming iteration skipped due to vital consumer %Qv snapshot not containing information about queue",
                    consumerSnapshot->Row.Ref);
            }
            vitalConsumerSubSnapshots[consumerSnapshot->Row.Ref] = it->second;
        }

        if (vitalConsumerSubSnapshots.empty()) {
            // TODO(achulkov2): This should produce some warning/misconfiguration alert to the client?
            YT_LOG_DEBUG(
                "Attempted trimming iteration on queue with no vital consumers (Queue: %v)",
                queueSnapshot->Row.Ref);
            return;
        }

        auto client = ClientDirectory_->GetClientOrThrow(QueueRef_.Cluster);

        const auto& lifetimeDuration = autoTrimConfig->RetainedLifetimeDuration;
        std::vector<NApi::TGetOrderedTabletSafeTrimRowCountRequest> safeTrimRowCountRequests;
        TTimestamp firstTimestampToRetainRows;
        if (lifetimeDuration) {
            safeTrimRowCountRequests.reserve(queueSnapshot->PartitionCount);
            auto timestampProvider = client->GetTimestampProvider();
            YT_VERIFY(timestampProvider);

            auto latestTimestampOrError = WaitFor(timestampProvider->GenerateTimestamps());
            if (!latestTimestampOrError.IsOK()) {
                // Skip trimming, because unable to generate timestamp.
                THROW_ERROR_EXCEPTION("Failed to generate last timestamp")
                    << latestTimestampOrError;
            }
            auto latestTimestamp = latestTimestampOrError.Value();
            auto now = TimestampToInstant(latestTimestamp).first;
            // InstantToTimestamp returns time span containing time instant passed to it, to guarantee trim of rows with MaxTimestamp < barrier time,
            // we need to trim rows by left boundary of span, thus we will trim rows with MaxTimestamp < left boundary of span <= barrier time.
            firstTimestampToRetainRows = InstantToTimestamp(now - *lifetimeDuration).first;
        }

        // We will be collecting partitions for which no error is set in the queue snapshot, nor in any of the consumer snapshots.
        THashSet<int> partitionsToTrim;
        for (int partitionIndex = 0; partitionIndex < queueSnapshot->PartitionCount; ++partitionIndex) {
            const auto& partitionSnapshot = queueSnapshot->PartitionSnapshots[partitionIndex];

            if (partitionSnapshot->TabletState != NTabletClient::ETabletState::Mounted) {
                YT_LOG_DEBUG("Partition %v is not in mounted state, actual state is %v, skip from trimming", partitionIndex, partitionSnapshot->TabletState);
                continue;
            }
            TError partitionError;

            if (!partitionSnapshot->Error.IsOK()) {
                partitionError = partitionSnapshot->Error;
            } else {
                for (const auto& [_, consumerSubSnapshot] : vitalConsumerSubSnapshots) {
                    // NB: there is no guarantee that consumer snapshot consists of the same number of partitions.
                    if (partitionIndex < std::ssize(consumerSubSnapshot->PartitionSnapshots)) {
                        const auto& consumerPartitionSubSnapshot = consumerSubSnapshot->PartitionSnapshots[partitionIndex];
                        if (!consumerPartitionSubSnapshot->Error.IsOK()) {
                            partitionError = consumerPartitionSubSnapshot->Error;
                            break;
                        }
                    } else {
                        partitionError = TError("Consumer snapshot does not know about partition snapshot");
                    }
                }
            }

            if (partitionError.IsOK()) {
                partitionsToTrim.insert(partitionIndex);
                if (lifetimeDuration) {
                    safeTrimRowCountRequests.push_back(
                        NApi::TGetOrderedTabletSafeTrimRowCountRequest{
                            queueObjectPath,
                            partitionIndex,
                            firstTimestampToRetainRows
                        }
                    );
                }
            } else {
                YT_LOG_DEBUG(
                    partitionError,
                    "Not trimming partition due to partition error (PartitionIndex: %v)",
                    partitionIndex);
            }
        }

        THashMap<int, i64> updatedTrimmedRowCounts;
        std::vector<TErrorOr<i64>> safeTrimRowCountsOrErrors;
        if (lifetimeDuration) {
            // Get row indices for each partition's rows that live no more than firstTimestampToRetainRows.
            auto internalClient = DynamicPointerCast<NApi::IInternalClient>(client);
            safeTrimRowCountsOrErrors = WaitFor(internalClient->GetOrderedTabletSafeTrimRowCount(safeTrimRowCountRequests))
                .ValueOrThrow();
            // Update list for partitions to trim with collected indices.
            for (int safeTrimRowCountsIndex = 0; safeTrimRowCountsIndex < std::ssize(safeTrimRowCountsOrErrors); ++safeTrimRowCountsIndex) {
                const auto& safeTrimRowCountOrError = safeTrimRowCountsOrErrors[safeTrimRowCountsIndex];
                int partitionIndex = safeTrimRowCountRequests[safeTrimRowCountsIndex].TabletIndex;
                if (!safeTrimRowCountOrError.IsOK()) {
                    YT_LOG_DEBUG(
                        safeTrimRowCountOrError,
                        "Error getting safe trim row count by timestamp, not trimming partition (PartitionIndex: %v)",
                        partitionIndex);
                    partitionsToTrim.erase(partitionIndex);
                }
            }
        }

        for (const auto& [consumerRef, consumerSubSnapshot] : vitalConsumerSubSnapshots) {
            for (const auto& partitionIndex : partitionsToTrim) {
                const auto& partitionSnapshot = consumerSubSnapshot->PartitionSnapshots[partitionIndex];

                // NextRowIndex should always be present in the snapshot.
                YT_LOG_DEBUG(
                    "Updating trimmed row count (Partition: %v, NextRowIndex: %v, Consumer: %v)",
                    partitionIndex,
                    partitionSnapshot->NextRowIndex,
                    consumerRef);
                auto updatedTrimmedRowCountIt = updatedTrimmedRowCounts.find(partitionIndex);
                if (updatedTrimmedRowCountIt != updatedTrimmedRowCounts.end()) {
                    updatedTrimmedRowCountIt->second = std::min(updatedTrimmedRowCountIt->second, partitionSnapshot->NextRowIndex);
                } else {
                    updatedTrimmedRowCounts[partitionIndex] = partitionSnapshot->NextRowIndex;
                }
            }
        }

        if (lifetimeDuration) {
            for (int safeTrimRowCountsIndex = 0; safeTrimRowCountsIndex < std::ssize(safeTrimRowCountsOrErrors); ++safeTrimRowCountsIndex) {
                const auto& safeTrimRowCountOrError = safeTrimRowCountsOrErrors[safeTrimRowCountsIndex];
                int partitionIndex = safeTrimRowCountRequests[safeTrimRowCountsIndex].TabletIndex;
                if (safeTrimRowCountOrError.IsOK()) {
                    auto updatedTrimmedRowCountIt = updatedTrimmedRowCounts.find(partitionIndex);
                    if (updatedTrimmedRowCountIt != updatedTrimmedRowCounts.end()) {
                        // We only need trim partitions with vital consumers, if partition wasn't added on previous cycle,
                        // then no vital consumer read from it.
                        updatedTrimmedRowCountIt->second = std::min(updatedTrimmedRowCountIt->second, safeTrimRowCountOrError.Value());
                    }
                }
            }
        }

        std::vector<TFuture<void>> asyncTrims;
        asyncTrims.reserve(updatedTrimmedRowCounts.size());
        std::vector<int> trimmedPartitions;
        trimmedPartitions.reserve(updatedTrimmedRowCounts.size());
        for (auto [partitionIndex, updatedTrimmedRowCount] : updatedTrimmedRowCounts) {
            const auto& queuePartitionSnapshot = queueSnapshot->PartitionSnapshots[partitionIndex];
            auto currentTrimmedRowCount = queuePartitionSnapshot->LowerRowIndex;

            if (const auto& retainedRows = autoTrimConfig->RetainedRows) {
                updatedTrimmedRowCount = std::min(
                    updatedTrimmedRowCount,
                    std::max<i64>(queuePartitionSnapshot->UpperRowIndex - *retainedRows, 0));
            }

            if (updatedTrimmedRowCount > currentTrimmedRowCount) {
                YT_LOG_DEBUG(
                    "Trimming partition (Partition: %v, TrimmedRowCount: %v -> %v)",
                    partitionIndex,
                    currentTrimmedRowCount,
                    updatedTrimmedRowCount);
                asyncTrims.push_back(client->TrimTable(queueObjectPath, partitionIndex, updatedTrimmedRowCount));
                trimmedPartitions.push_back(partitionIndex);
            }
        }

        auto trimmingResults = WaitFor(AllSet(asyncTrims))
            .ValueOrThrow();
        for (int trimmedPartitionIndex = 0; trimmedPartitionIndex < std::ssize(trimmingResults); ++trimmedPartitionIndex) {
            const auto& trimmingResult = trimmingResults[trimmedPartitionIndex];
            if (!trimmingResult.IsOK()) {
                YT_LOG_DEBUG(trimmingResult, "Error trimming partition %v", trimmedPartitions[trimmedPartitionIndex]);
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TOrderedDynamicTableController)

////////////////////////////////////////////////////////////////////////////////

class TErrorQueueController
    : public IQueueController
{
public:
    TErrorQueueController(
        TQueueTableRow row,
        TError error)
        : Row_(std::move(row))
        , Error_(std::move(error))
        , Snapshot_(New<TQueueSnapshot>())
    {
        Snapshot_->Error = Error_;
    }

    void OnDynamicConfigChanged(
        const TQueueControllerDynamicConfigPtr& /*oldConfig*/,
        const TQueueControllerDynamicConfigPtr& /*newConfig*/) override
    { }

    void OnRowUpdated(std::any /*row*/) override
    {
        // Row update is handled in RecreateQueueController.
    }

    TRefCountedPtr GetLatestSnapshot() const override
    {
        return Snapshot_;
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer) const override
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("row").Value(Row_)
                .Item("status").BeginMap()
                    .Item("error").Value(Error_)
                .EndMap()
                .Item("partitions").BeginList().EndList()
            .EndMap();
    }

    EQueueFamily GetFamily() const override
    {
        return EQueueFamily::Null;
    }

    bool IsLeading() const override
    {
        return false;
    }

private:
    TQueueTableRow Row_;
    TError Error_;
    const TQueueSnapshotPtr Snapshot_;
};

DEFINE_REFCOUNTED_TYPE(TErrorQueueController)

////////////////////////////////////////////////////////////////////////////////

bool UpdateQueueController(
    IObjectControllerPtr& controller,
    bool leading,
    const TQueueTableRow& row,
    const IObjectStore* store,
    TQueueControllerDynamicConfigPtr dynamicConfig,
    TClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker)
{
    // Recreating an error controller on each iteration seems ok as it does
    // not have any state. By doing so we make sure that the error of a queue controller
    // is not stale.

    if (row.SynchronizationError && !row.SynchronizationError->IsOK()) {
        controller = New<TErrorQueueController>(row, TError("Queue synchronization error") << *row.SynchronizationError);
        return true;
    }

    auto queueFamily = DeduceQueueFamily(row);
    if (!queueFamily.IsOK()) {
        controller = New<TErrorQueueController>(row, queueFamily);
        return true;
    }

    auto currentController = DynamicPointerCast<IQueueController>(controller);
    if (currentController && currentController->GetFamily() == queueFamily.Value() && currentController->IsLeading() == leading) {
        // Do not recreate the controller if it is of the same family and leader/follower status.
        return false;
    }

    switch (queueFamily.Value()) {
        case EQueueFamily::OrderedDynamicTable:
            controller = New<TOrderedDynamicTableController>(
                leading,
                row,
                store,
                std::move(dynamicConfig),
                std::move(clientDirectory),
                std::move(invoker));
            break;
        default:
            YT_ABORT();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
