#include "queue_controller.h"

#include "snapshot.h"
#include "snapshot_representation.h"
#include "config.h"
#include "helpers.h"
#include "profile_manager.h"
#include "queue_static_table_exporter.h"

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/internal_client.h>
#include "yt/yt/client/api/table_client.h"

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/queue_client/config.h>
#include <yt/yt/client/queue_client/helpers.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduled_executor.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/ema_counter.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NQueueAgent {

using namespace NApi;
using namespace NAlertManager;
using namespace NHydra;
using namespace NYPath;
using namespace NYTree;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NQueueClient;
using namespace NYson;
using namespace NTracing;
using namespace NThreading;
using namespace NLogging;
using namespace NObjectClient;
using namespace NProfiling;

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
        std::optional<TReplicatedTableMappingTableRow> replicatedTableMappingRow,
        TQueueSnapshotPtr previousQueueSnapshot,
        std::vector<TConsumerRegistrationTableRow> registrations,
        TLogger logger,
        TQueueAgentClientDirectoryPtr clientDirectory)
        : Row_(std::move(row))
        , ReplicatedTableMappingRow_(std::move(replicatedTableMappingRow))
        , PreviousQueueSnapshot_(std::move(previousQueueSnapshot))
        , Registrations_(std::move(registrations))
        , ClientDirectory_(std::move(clientDirectory))
        , Logger(logger)
    { }

    TQueueSnapshotPtr Build()
    {
        QueueSnapshot_->PassIndex = PreviousQueueSnapshot_->PassIndex + 1;
        QueueSnapshot_->PassInstant = TInstant::Now();
        QueueSnapshot_->Row = Row_;
        QueueSnapshot_->ReplicatedTableMappingRow = ReplicatedTableMappingRow_;

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
    const std::optional<TReplicatedTableMappingTableRow> ReplicatedTableMappingRow_;
    const TQueueSnapshotPtr PreviousQueueSnapshot_;
    const std::vector<TConsumerRegistrationTableRow> Registrations_;
    const TQueueAgentClientDirectoryPtr ClientDirectory_;
    const TLogger Logger;

    TQueueSnapshotPtr QueueSnapshot_ = New<TQueueSnapshot>();

    void GuardedBuild()
    {
        YT_LOG_DEBUG("Building queue snapshot (PassIndex: %v)", QueueSnapshot_->PassIndex);

        auto queueRef = QueueSnapshot_->Row.Ref;

        // TODO(achulkov2): Check partition count of control queue for replicated tables.
        // TODO(achulkov2): Check schema for chaos_replicated_table object (we only check for a sync replica below)?

        QueueSnapshot_->Family = EQueueFamily::OrderedDynamicTable;
        auto syncClientContext = ClientDirectory_->GetNativeSyncClient(QueueSnapshot_);
        const auto& tableMountCache = syncClientContext.Client->GetTableMountCache();
        const auto& cellDirectory = syncClientContext.Client->GetNativeConnection()->GetCellDirectory();

        // Fetch partition count (which is equal to tablet count).

        auto tableInfo = WaitFor(tableMountCache->GetTableInfo(syncClientContext.Path))
            .ValueOrThrow();

        YT_LOG_DEBUG("Table info collected (TabletCount: %v)", tableInfo->Tablets.size());

        const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
        if (!schema || schema->IsSorted()) {
            THROW_ERROR_EXCEPTION("Invalid queue schema %v", schema);
        }
        QueueSnapshot_->HasTimestampColumn = schema->HasTimestampColumn();
        QueueSnapshot_->HasCumulativeDataWeightColumn = schema->FindColumn(CumulativeDataWeightColumnName);

        auto& partitionCount = QueueSnapshot_->PartitionCount;
        partitionCount = std::ssize(tableInfo->Tablets);

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
                if (auto cellDescriptor = cellDirectory->FindDescriptorByCellId(cellId)) {
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

        auto tabletInfos = WaitFor(syncClientContext.Client->GetTabletInfos(syncClientContext.Path, tabletIndexes))
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

        auto clientContext = ClientDirectory_->GetDataReadContext(QueueSnapshot_);

        auto params = TCollectPartitionRowInfoParams{
            .HasCumulativeDataWeightColumn = true,
        };
        auto result = WaitFor(NQueueClient::CollectPartitionRowInfos(clientContext.Path, clientContext.Client, tabletAndRowIndices, params, Logger()))
            .ValueOrThrow();

        for (const auto& [tabletIndex, tabletInfo] : result) {
            auto& partitionSnapshot = QueueSnapshot_->PartitionSnapshots[tabletIndex];

            auto lowerTabletRowInfoIt = tabletInfo.find(partitionSnapshot->LowerRowIndex);
            if (lowerTabletRowInfoIt != tabletInfo.end()) {
                partitionSnapshot->TrimmedDataWeight = lowerTabletRowInfoIt->second.CumulativeDataWeight;
            }

            auto upperTabletRowInfoIt = tabletInfo.find(partitionSnapshot->UpperRowIndex - 1);
            if (upperTabletRowInfoIt != tabletInfo.end()) {
                partitionSnapshot->CumulativeDataWeight = upperTabletRowInfoIt->second.CumulativeDataWeight;
                if (partitionSnapshot->CumulativeDataWeight) {
                    partitionSnapshot->WriteRate.DataWeight.Update(*partitionSnapshot->CumulativeDataWeight);
                }
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
        std::optional<TReplicatedTableMappingTableRow> replicatedTableMappingRow,
        const IObjectStore* store,
        const TQueueControllerDynamicConfigPtr& dynamicConfig,
        TQueueAgentClientDirectoryPtr clientDirectory,
        IInvokerPtr invoker)
        : Leading_(leading)
        , QueueRow_(queueRow)
        , ReplicatedTableMappingRow_(replicatedTableMappingRow)
        , QueueRef_(queueRow.Ref)
        , ObjectStore_(store)
        , DynamicConfig_(dynamicConfig)
        , ClientDirectory_(std::move(clientDirectory))
        , Invoker_(std::move(invoker))
        , Logger(QueueAgentLogger().WithTag("Queue: %v, Leading: %v", QueueRef_, Leading_))
        , PassExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TOrderedDynamicTableController::Pass, MakeWeak(this)),
            TPeriodicExecutorOptions{
                .Period = dynamicConfig->PassPeriod,
                .Splay = dynamicConfig->PassPeriod,
            }))
        , ProfileManager_(CreateQueueProfileManager(
            QueueAgentProfilerGlobal
                .WithRequiredTag("queue_path", QueueRef_.Path)
                .WithRequiredTag("queue_cluster", QueueRef_.Cluster),
            Logger))
        , AlertManager_(CreateAlertManager(Logger, ProfileManager_->GetQueueProfiler(), Invoker_))
        , TrimAlertCollector_(CreateAlertCollector(AlertManager_))
        , QueueExportsAlertCollector_(CreateAlertCollector(AlertManager_))
    {
        // Prepare initial erroneous snapshot.
        auto queueSnapshot = New<TQueueSnapshot>();
        queueSnapshot->Row = std::move(queueRow);
        queueSnapshot->ReplicatedTableMappingRow = std::move(replicatedTableMappingRow);
        queueSnapshot->Error = TError("Queue is not processed yet");
        QueueSnapshot_.Exchange(std::move(queueSnapshot));

        PassExecutor_->Start();
        AlertManager_->Start();

        YT_LOG_INFO("Queue controller started");
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
            .Item("replicated_table_mapping_row").Value(queueSnapshot->ReplicatedTableMappingRow)
            .Item("status").Do(std::bind(BuildQueueStatusYson, queueSnapshot, AlertManager_, _1))
            .Item("partitions").Do(std::bind(BuildQueuePartitionListYson, queueSnapshot, _1))
        .EndMap();
    }

    void OnRowUpdated(std::any row) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& queueRow = std::any_cast<const TQueueTableRow&>(row);

        QueueRow_.Store(queueRow);
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

        AlertManager_->Reconfigure(oldConfig->AlertManager, newConfig->AlertManager);

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
    TAtomicObject<std::optional<TReplicatedTableMappingTableRow>> ReplicatedTableMappingRow_;
    const TCrossClusterReference QueueRef_;
    const IObjectStore* ObjectStore_;

    using TQueueControllerDynamicConfigAtomicPtr = TAtomicIntrusivePtr<TQueueControllerDynamicConfig>;
    TQueueControllerDynamicConfigAtomicPtr DynamicConfig_;

    const TQueueAgentClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr Invoker_;

    using TQueueSnapshotAtomicPtr = TAtomicIntrusivePtr<TQueueSnapshot>;
    TQueueSnapshotAtomicPtr QueueSnapshot_;

    const TLogger Logger;
    const TPeriodicExecutorPtr PassExecutor_;
    const IQueueProfileManagerPtr ProfileManager_;
    const IAlertManagerPtr AlertManager_;
    // TODO(achulkov2, nadya73): Separate trim into separate periodic executor.
    const IAlertCollectorPtr TrimAlertCollector_;
    const IAlertCollectorPtr QueueExportsAlertCollector_;

    struct TQueueExport
    {
        TScheduledExecutorPtr Executor;
        TQueueExporterPtr Exporter;
    };

    using QueueExportsMappingOrError = TErrorOr<THashMap<TString, TQueueExport>>;
    QueueExportsMappingOrError QueueExports_;
    TReaderWriterSpinLock QueueExportsLock_;

    void Export(const TString& exportName)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!DynamicConfig_.Acquire()->EnableQueueStaticExport) {
            YT_LOG_DEBUG("Skipping queue static export iteration, since it is disabled controller-wide");
            return;
        }

        TQueueExporterPtr exporter;
        {
            auto guard = ReaderGuard(QueueExportsLock_);
            if (!QueueExports_.IsOK()) {
                YT_LOG_DEBUG(QueueExports_, "Skipping queue static export iteration, because config is incorrect (ExportName: %v)", exportName);
                return;
            }
            const auto& queueExports = QueueExports_.Value();
            auto queueExport = queueExports.find(exportName);
            if (queueExport == queueExports.end()) {
                YT_LOG_DEBUG("Skipping queue static export iteration, since there is no exporter for it (ExportName: %v)", exportName);
                return;
            }
            exporter = queueExport->second.Exporter;
        }

        auto exportError = WaitFor(exporter->RunExportIteration());
        YT_LOG_ERROR_UNLESS(exportError.IsOK(), exportError, "Failed to perform static export for queue");
    }

    void Pass()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("QueueControllerPass"));

        if (auto queueRow = QueueRow_.Load(); queueRow.QueueAgentBanned.value_or(false)) {
            YT_LOG_INFO("Skipping queue controller pass because queue is banned by @queue_agent_banned attribute");
            auto queueSnapshot = New<TQueueSnapshot>();
            queueSnapshot->Row = std::move(queueRow);
            queueSnapshot->ReplicatedTableMappingRow = std::move(ReplicatedTableMappingRow_.Load());
            queueSnapshot->Error = TError("Queue is banned");
            QueueSnapshot_.Exchange(queueSnapshot);
            return;
        }

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
            ReplicatedTableMappingRow_.Load(),
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

            UpdateExports(nextQueueSnapshot);

            if (ShouldTrim(nextQueueSnapshot->PassIndex)) {
                Trim();
            }
        }

        YT_LOG_INFO("Queue controller pass finished");
    }

    void UpdateExports(const TQueueSnapshotPtr& nextQueueSnapshot)
    {
        // NB(apachee): We keep the exports even in the case of "enableQueueStaticExport" being false
        // to allow trimming exported rows, but prevent trimming past them.

        auto finalizeUpdate = Finally([&] {
            QueueExportsAlertCollector_->PublishAlerts();
        });

        auto enableQueueStaticExport = DynamicConfig_.Acquire()->EnableQueueStaticExport;
        const auto& staticExportConfig = nextQueueSnapshot->Row.StaticExportConfig;

        auto guard = WriterGuard(QueueExportsLock_);

        if (!staticExportConfig) {
            QueueExports_ = QueueExportsMappingOrError();
            return;
        }
        if (auto staticExportConfigError = CheckStaticExportConfig(*staticExportConfig); !staticExportConfigError.IsOK()) {
            QueueExports_ = staticExportConfigError;
            QueueExportsAlertCollector_->StageAlert(CreateAlert(
                NAlerts::EErrorCode::QueueAgentQueueControllerStaticExportMisconfiguration,
                "Failed to update exports due to misconfiguration",
                /*tags*/ {},
                /*error*/ QueueExports_));
            return;
        }

        if (!QueueExports_.IsOK()) {
            QueueExports_ = QueueExportsMappingOrError();
        }
        auto& queueExports = QueueExports_.Value();
        for (const auto& [name, config] : *staticExportConfig) {
            if (queueExports.find(name) == queueExports.end()) {
                queueExports[name] = {
                    .Executor = New<TScheduledExecutor>(
                        Invoker_,
                        BIND(&TOrderedDynamicTableController::Export, MakeWeak(this), name),
                        /*interval*/ std::nullopt),
                    .Exporter = New<TQueueExporter>(
                        name,
                        QueueRef_,
                        config,
                        ClientDirectory_->GetUnderlyingClientDirectory(),
                        Invoker_,
                        CreateAlertCollector(AlertManager_),
                        ProfileManager_->GetQueueProfiler(),
                        Logger),
                };

                queueExports[name].Executor->Start();
            } else {
                queueExports[name].Exporter->Reconfigure(config);
            }

            queueExports[name].Executor->SetInterval(enableQueueStaticExport
                ? std::optional(config.ExportPeriod)
                : std::nullopt);
        }

        // Remove unused exports.
        std::vector<TString> unusedExportNames;
        for (const auto& [exportName, _] : queueExports) {
            if (staticExportConfig->find(exportName) == staticExportConfig->end()) {
                unusedExportNames.push_back(exportName);
            }
        }
        for (const auto& name : unusedExportNames) {
            queueExports.erase(name);
        }
    }

    TError CheckStaticExportConfig(const THashMap<TString, TQueueStaticExportConfig>& configs) const
    {
        THashSet<TYPath> directories;
        THashSet<TYPath> duplicateDirectories;
        for (const auto& [_, config] : configs) {
            if (auto [_, inserted] = directories.insert(config.ExportDirectory); !inserted) {
                YT_LOG_DEBUG("There are duplicate export directories in queue static export config (Value: %v)", config.ExportDirectory);
                duplicateDirectories.insert(config.ExportDirectory);
            }
        }

        if (duplicateDirectories.empty()) {
            return {};
        }

        auto error = TError("Static export config check failed");
        for (const auto& directory : duplicateDirectories) {
            error <<= TError("Duplicate directory in config (Value: %v)", directory);
        }
        return error;
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
            TrimAlertCollector_->StageAlert(CreateAlert(
                NAlerts::EErrorCode::QueueAgentQueueControllerTrimFailed,
                "Error while trimming queue",
                /*tags*/ {},
                ex));
        }

        TrimAlertCollector_->PublishAlerts();
    }

    struct TPartitionTrimContext
    {
        int PartitionIndex;
        TError PartitionError;
        bool IsErrorCritical = true;

        //! Signifies that the partition could (and should) be trimmed up to this point.
        std::optional<i64> MinTrimmedRowCount;
        //! Partition will not be trimmed past this point under any circumstances. Overrides the value above.
        std::optional<i64> MaxTrimmedRowCount;

        bool HasCriticalError() const
        {
            return !PartitionError.IsOK() && IsErrorCritical;
        }

        explicit operator bool() const
        {
            return PartitionError.IsOK();
        }

        //! NB: Verifies that no error is currently set.
        void SetError(const TError& error)
        {
            YT_VERIFY(PartitionError.IsOK());
            PartitionError = error;
        }

        void Update(const TPartitionTrimContext& other)
        {
            if (PartitionError.IsOK() || (!HasCriticalError() && other.HasCriticalError())) {
                PartitionError = other.PartitionError;
                IsErrorCritical = other.IsErrorCritical;
            }

            MinTrimmedRowCount = std::max(MinTrimmedRowCount, other.MinTrimmedRowCount);

            MaxTrimmedRowCount = MinOrValue(MaxTrimmedRowCount, other.MaxTrimmedRowCount);
        }

        std::optional<i64> GetUpdatedTrimmedRowCount(i64 currentTrimmedRowCount) const
        {
            if (!PartitionError.IsOK()) {
                return {};
            }

            if (!MinTrimmedRowCount) {
                return {};
            }

            i64 updatedTrimmedRowCount = *MinTrimmedRowCount;
            if (MaxTrimmedRowCount) {
                updatedTrimmedRowCount = std::min(*MaxTrimmedRowCount, updatedTrimmedRowCount);
            }

            if (updatedTrimmedRowCount > currentTrimmedRowCount) {
                return updatedTrimmedRowCount;
            }

            return {};
        }
    };

    struct TQueueTrimContext
    {
        TCrossClusterReference Ref;
        TQueueSnapshotConstPtr ReplicaSnapshot;
        TYPath ObjectPath;
        std::vector<TPartitionTrimContext> Partitions;
        // TODO(achulkov2): Add upstream replica id field + server-side check in Trim.

        TQueueTrimContext(TCrossClusterReference ref, TQueueSnapshotConstPtr replicaSnapshot)
            : Ref(std::move(ref))
            , ReplicaSnapshot(std::move(replicaSnapshot))
        {
            auto replicaQueueObjectId = ReplicaSnapshot->Row.ObjectId;
            if (!replicaQueueObjectId) {
                THROW_ERROR_EXCEPTION("Object id is not known for queue replica %Qv, trimming iteration skipped", Ref);
            }
            ObjectPath = FromObjectId(*replicaQueueObjectId);

            YT_VERIFY(ReplicaSnapshot->PartitionCount == std::ssize(ReplicaSnapshot->PartitionSnapshots));

            Partitions.resize(ReplicaSnapshot->PartitionCount);
            for (int partitionIndex = 0; partitionIndex < ReplicaSnapshot->PartitionCount; ++partitionIndex) {
                Partitions[partitionIndex].PartitionIndex = partitionIndex;
            }
        }
    };

    std::vector<TQueueTrimContext> GetReplicasToTrim(const TQueueSnapshotPtr& queueSnapshot)
    {
        YT_VERIFY(queueSnapshot->Row.ObjectType);
        auto objectType = *queueSnapshot->Row.ObjectType;
        switch (objectType) {
            case EObjectType::Table:
                return {{QueueRef_, queueSnapshot}};
            case EObjectType::ReplicatedTable:
                return GetReplicatedTableReplicasToTrim(queueSnapshot);
            case EObjectType::ChaosReplicatedTable:
                return GetChaosReplicatedTableReplicasToTrim(queueSnapshot);
            default:
                YT_ABORT();
        }
    }

    std::vector<TQueueTrimContext> GetReplicatedTableReplicasToTrim(const TQueueSnapshotPtr& queueSnapshot)
    {
        std::vector<TQueueTrimContext> replicaContexts;

        for (const auto& replica : queueSnapshot->ReplicatedTableMappingRow->GetReplicas()) {
            auto replicaRef = TCrossClusterReference::FromRichYPath(replica);
            auto replicaSnapshot = DynamicPointerCast<const TQueueSnapshot>(ObjectStore_->FindSnapshot(replicaRef));
            if (!replicaSnapshot) {
                THROW_ERROR_EXCEPTION("Trimming iteration skipped due to missing snapshot for queue replica %Qv", replicaRef);
            }

            auto& replicaContext = replicaContexts.emplace_back(replicaRef, replicaSnapshot);
            for (const auto& [partitionContext, partitionSnapshot] : Zip(replicaContext.Partitions, replicaSnapshot->PartitionSnapshots)) {
                partitionContext.Update({.MaxTrimmedRowCount = partitionSnapshot->UpperRowIndex});
            }
        }

        return replicaContexts;
    }

    std::vector<TQueueTrimContext> GetChaosReplicatedTableReplicasToTrim(const TQueueSnapshotPtr& queueSnapshot)
    {
        auto federatedClient = ClientDirectory_->GetFederatedClient(GetRelevantReplicas(*queueSnapshot->ReplicatedTableMappingRow));

        NApi::TGetReplicationCardOptions options;
        options.IncludeProgress = true;
        auto replicationCard = WaitFor(federatedClient->GetReplicationCard(
            queueSnapshot->ReplicatedTableMappingRow->Meta->ChaosReplicatedTableMeta->ReplicationCardId,
            options))
            .ValueOrThrow();

        std::vector<TQueueTrimContext> replicaContexts;
        for (const auto& replicaInfo : GetValues(replicationCard->Replicas)) {
            TCrossClusterReference replicaRef{
                .Cluster = replicaInfo.ClusterName,
                .Path = replicaInfo.ReplicaPath,
            };
            auto replicaSnapshot = DynamicPointerCast<const TQueueSnapshot>(ObjectStore_->FindSnapshot(replicaRef));
            if (!replicaSnapshot) {
                THROW_ERROR_EXCEPTION("Trimming iteration skipped due to missing replica snapshot %Qv", replicaRef);
            }
            replicaContexts.emplace_back(replicaRef, replicaSnapshot);
        }

        std::vector<std::optional<TTimestamp>> minReplicationTimestamps(queueSnapshot->PartitionCount);
        for (const auto& [replicaInfo, replicaContext] : Zip(GetValues(replicationCard->Replicas), replicaContexts)) {
            for (int partitionIndex = 0; partitionIndex < replicaContext.ReplicaSnapshot->PartitionCount; ++partitionIndex) {
                minReplicationTimestamps[partitionIndex] = MinOrValue<TTimestamp>(
                    minReplicationTimestamps[partitionIndex],
                    GetReplicationProgressMinTimestamp(
                        replicaInfo.ReplicationProgress,
                        MakeUnversionedOwningRow(partitionIndex),
                        MakeUnversionedOwningRow(partitionIndex + 1)));
            }
        }

        std::vector<TFuture<std::vector<TErrorOr<i64>>>> asyncSafeTrimRowCounts;
        std::vector<NApi::IInternalClientPtr> internalClients;
        for (const auto& replicaContext : replicaContexts) {
            auto internalClient = DynamicPointerCast<NApi::IInternalClient>(ClientDirectory_->GetClientOrThrow(replicaContext.Ref.Cluster));
            std::vector<NApi::TGetOrderedTabletSafeTrimRowCountRequest> safeTrimRowCountRequests;
            for (int partitionIndex = 0; partitionIndex < replicaContext.ReplicaSnapshot->PartitionCount; ++partitionIndex) {
                YT_VERIFY(minReplicationTimestamps[partitionIndex]);
                safeTrimRowCountRequests.push_back({
                    .Path = replicaContext.ObjectPath,
                    .TabletIndex = partitionIndex,
                    .Timestamp = *minReplicationTimestamps[partitionIndex],
                });
            }
            asyncSafeTrimRowCounts.push_back(internalClient->GetOrderedTabletSafeTrimRowCount(safeTrimRowCountRequests));
            internalClients.push_back(internalClient);
        }

        auto asyncSafeTrimRowCountsOrErrors = WaitFor(AllSet(asyncSafeTrimRowCounts))
            .ValueOrThrow();

        for (const auto& [replicaContext, safeTrimRowCountsOrError] : Zip(replicaContexts, asyncSafeTrimRowCountsOrErrors)) {
            if (!safeTrimRowCountsOrError.IsOK()) {
                THROW_ERROR_EXCEPTION(
                    "Unable to get safe trim row counts for replica %Qv, trimming iteration skipped",
                    replicaContext.Ref)
                    << safeTrimRowCountsOrError;
            }

            for (const auto& [partitionContext, safeTrimRowCountOrError] : Zip(replicaContext.Partitions, safeTrimRowCountsOrError.Value())) {
                partitionContext.Update({.PartitionError = safeTrimRowCountOrError});
                if (partitionContext) {
                    partitionContext.Update({.MaxTrimmedRowCount = safeTrimRowCountOrError.Value()});
                }
            }
        }

        return replicaContexts;
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

        const auto& autoTrimConfig = queueSnapshot->Row.AutoTrimConfig;
        if (!autoTrimConfig.Enable) {
            YT_LOG_DEBUG(
                "Trimming disabled; trimming iteration skipped (AutoTrimConfig: %v)",
                ConvertToYsonString(autoTrimConfig, EYsonFormat::Text));
            return;
        }

        auto timestampProvider = ClientDirectory_->GetClientOrThrow(QueueRef_.Cluster)->GetTimestampProvider();
        YT_VERIFY(timestampProvider);

        auto currentTimestampOrError = WaitFor(timestampProvider->GenerateTimestamps());
        if (!currentTimestampOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Cannot generate timestamp for cluster %Qv, trimming iteration skipped", QueueRef_.Cluster)
                << currentTimestampOrError;
        }
        auto currentTimestamp = currentTimestampOrError.Value();

        auto replicaContexts = GetReplicasToTrim(queueSnapshot);

        auto queueExportProgress = GetQueueExportProgressOrThrow();

        std::vector<TIntrusivePtr<TQueueTrimSession>> trimSessions;
        for (const auto& replicaContext : replicaContexts) {
            trimSessions.push_back(New<TQueueTrimSession>(
                QueueRef_,
                queueSnapshot,
                replicaContext,
                currentTimestamp,
                ClientDirectory_->GetClientOrThrow(replicaContext.Ref.Cluster),
                std::move(queueExportProgress),
                ObjectStore_,
                Logger));
            // NB: We do not invoke sessions immediately, so that we don't waste resources in case of an incomplete cluster directory.
        }

        std::vector<TFuture<void>> asyncTrimSessions;
        for (const auto& trimSession : trimSessions) {
            asyncTrimSessions.push_back(trimSession->Run());
        }
        auto trimSessionPotentialErrors = WaitFor(AllSet(asyncTrimSessions))
            .ValueOrThrow();

        std::vector<TError> trimSessionErrors;
        for (const auto& [replicaContext, trimSessionPotentialError] : Zip(replicaContexts, trimSessionPotentialErrors)) {
            if (!trimSessionPotentialError.IsOK()) {
                trimSessionErrors.push_back(trimSessionPotentialError << TErrorAttribute("replica", replicaContext.Ref));
            }
        }

        if (!trimSessionErrors.empty()) {
            THROW_ERROR_EXCEPTION("Error trimming %v queue replicas", trimSessionErrors.size())
                << trimSessionErrors;
        }
    }

    THashMap<TString, TQueueExportProgressPtr> GetQueueExportProgressOrThrow()
    {
        auto guard = ReaderGuard(QueueExportsLock_);

        THashMap<TString, TQueueExportProgressPtr> queueExportProgress;

        // NB(apachee): Since static queue exports are taken into account when trimming,
        // we skip trim iteration to prevent potential data loss due to misconfiguration of exports.
        if (!QueueExports_.IsOK()) {
            THROW_ERROR_EXCEPTION("Incorrect queue exports, trimming at this point can lead to data loss for queue exports, trimming iteration skipped")
                << QueueExports_;
        }

        const auto& queueExports = QueueExports_.Value();
        queueExportProgress.reserve(queueExports.size());
        for (const auto& [queueExportName, queueExport] : queueExports) {
            queueExportProgress[queueExportName] = queueExport.Exporter->GetExportProgress();
        }
        return queueExportProgress;
    }

    struct TQueueTrimSession final
    {
        const TCrossClusterReference QueueRef;
        const TQueueSnapshotPtr QueueSnapshot;
        //! NB: Modified in process of the session.
        TQueueTrimContext Context;
        TTimestamp CurrentTimestamp;
        //! Replica-cluster client.
        const NApi::NNative::IClientPtr Client;
        THashMap<TString, TQueueExportProgressPtr> QueueExportProgress;
        const IObjectStore* ObjectStore;
        NLogging::TLogger Logger;

        THashMap<TCrossClusterReference, TSubConsumerSnapshotConstPtr> VitalConsumerSubSnapshots;

        TQueueTrimSession(
            TCrossClusterReference queueRef,
            TQueueSnapshotPtr queueSnapshot,
            TQueueTrimContext context,
            TTimestamp currentTimestamp,
            NApi::NNative::IClientPtr client,
            THashMap<TString, TQueueExportProgressPtr> queueExportProgress,
            const IObjectStore* objectStore,
            const NLogging::TLogger& logger)
            : QueueRef(std::move(queueRef))
            , QueueSnapshot(std::move(queueSnapshot))
            , Context(std::move(context))
            , CurrentTimestamp(currentTimestamp)
            , Client(std::move(client))
            , QueueExportProgress(std::move(queueExportProgress))
            , ObjectStore(objectStore)
            , Logger(logger.WithTag("Replica: %v, ObjectPath: %v", Context.Ref, Context.ObjectPath))
        { }

        TFuture<void> Run()
        {
            return BIND(&TQueueTrimSession::DoRun, MakeStrong(this))
                .AsyncVia(GetCurrentInvoker())
                .Run();
        }

        void DoRun()
        {
            if (!Context.ReplicaSnapshot->Error.IsOK()) {
                THROW_ERROR_EXCEPTION(
                    "Trimming iteration skipped due to queue replica error")
                    << Context.ReplicaSnapshot->Error;
            }

            if (QueueSnapshot->PartitionCount != Context.ReplicaSnapshot->PartitionCount) {
                THROW_ERROR_EXCEPTION(
                    "Cannot perform trimming iteration, control queue %Qv and replica queue %Qv do not "
                    "have the same number of partitions: %v vs %v, respectively; this is probably a misconfiguration",
                    QueueRef,
                    Context.Ref,
                    QueueSnapshot->PartitionCount,
                    Context.ReplicaSnapshot->PartitionCount);
            }

            YT_LOG_DEBUG("Performing trimming iteration");

            CollectVitalConsumerSubSnapshots();

            ValidatePartitionContexts();

            HandleSnapshotErrors();

            const auto& autoTrimConfig = QueueSnapshot->Row.AutoTrimConfig;
            HandleRetainedLifetimeDuration(autoTrimConfig);
            HandleRetainedRows(autoTrimConfig);

            HandleVitalConsumersAndExports();

            RequestTrimming();
            ReportErrors();
        }

        //! Collects vital consumer snapshots from queue consumer registrations and validates error-correctness.
        void CollectVitalConsumerSubSnapshots()
        {
            auto registrations = ObjectStore->GetRegistrations(QueueRef, EObjectKind::Queue);

            VitalConsumerSubSnapshots.reserve(registrations.size());
            for (const auto& registration : registrations) {
                if (!registration.Vital) {
                    continue;
                }
                auto consumerSnapshot = DynamicPointerCast<const TConsumerSnapshot>(ObjectStore->FindSnapshot(registration.Consumer));
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
                auto it = consumerSnapshot->SubSnapshots.find(QueueRef);
                if (it == consumerSnapshot->SubSnapshots.end()) {
                    THROW_ERROR_EXCEPTION(
                        "Trimming iteration skipped due to vital consumer %Qv snapshot not containing information about queue",
                        consumerSnapshot->Row.Ref);
                }
                const auto& consumerSubSnapshot = it->second;
                if (!consumerSubSnapshot->Error.IsOK()) {
                    THROW_ERROR_EXCEPTION(
                        "Trimming iteration skipped due to erroneous queue sub-snapshot in registered vital consumer %Qv",
                        consumerSnapshot->Row.Ref)
                        << consumerSubSnapshot->Error;
                }
                VitalConsumerSubSnapshots[consumerSnapshot->Row.Ref] = consumerSubSnapshot;
            }

            if (VitalConsumerSubSnapshots.empty() && QueueExportProgress.empty()) {
                THROW_ERROR_EXCEPTION(
                    "Attempted trimming iteration on queue %Qv with no vital consumers and no configured static table exports",
                    QueueRef);
            }
        }

        //! Validates that the list of partition contexts is consistent with the queue snapshot.
        void ValidatePartitionContexts()
        {
            YT_VERIFY(std::ssize(Context.Partitions) == QueueSnapshot->PartitionCount);
            for (int partitionIndex = 0; partitionIndex < std::ssize(Context.Partitions); ++partitionIndex) {
                YT_VERIFY(Context.Partitions[partitionIndex].PartitionIndex == partitionIndex);
            }
        }

        //! Get timestamp past which we should not trim based on the specified retained lifetime duration.
        TTimestamp GetMaxTimestampToTrim(TDuration lifetimeDuration) const
        {
            auto now = TimestampToInstant(CurrentTimestamp).first;
            // InstantToTimestamp returns time span containing time instant passed to it, to guarantee trim of rows
            // with MaxTimestamp < barrier time, we need to trim rows by left boundary of span, thus we will trim rows
            // with MaxTimestamp < left boundary of span <= barrier time.
            return InstantToTimestamp(now - lifetimeDuration).first;
        }

        //! Fills partition contexts with partition errors from both control and replica queue snapshots,
        //! as well as any of the vital consumer snapshots.
        void HandleSnapshotErrors()
        {
            for (int partitionIndex = 0; partitionIndex < QueueSnapshot->PartitionCount; ++partitionIndex) {
                auto& partitionContext = Context.Partitions[partitionIndex];
                if (!partitionContext) {
                    continue;
                }

                const auto& queuePartitionSnapshot = QueueSnapshot->PartitionSnapshots[partitionIndex];
                const auto& replicaPartitionSnapshot = Context.ReplicaSnapshot->PartitionSnapshots[partitionIndex];

                if (replicaPartitionSnapshot->TabletState != NTabletClient::ETabletState::Mounted) {
                    partitionContext.Update({
                        .PartitionError = TError(
                            "Not trimming partition %v since its tablet is in state %Qlv and is not mounted",
                            partitionIndex,
                            replicaPartitionSnapshot->TabletState),
                        .IsErrorCritical = false,
                    });
                    continue;
                }

                if (!queuePartitionSnapshot->Error.IsOK()) {
                    partitionContext.Update({.PartitionError = queuePartitionSnapshot->Error});
                } else if (!replicaPartitionSnapshot->Error.IsOK()) {
                    partitionContext.Update({.PartitionError = replicaPartitionSnapshot->Error});
                } else {
                    for (const auto& [consumerRef, consumerSubSnapshot] : VitalConsumerSubSnapshots) {
                        // NB: there is no guarantee that consumer snapshot consists of the same number of partitions.
                        if (partitionIndex < std::ssize(consumerSubSnapshot->PartitionSnapshots)) {
                            const auto& consumerPartitionSubSnapshot = consumerSubSnapshot->PartitionSnapshots[partitionIndex];
                            if (!consumerPartitionSubSnapshot->Error.IsOK()) {
                                partitionContext.Update({.PartitionError = consumerPartitionSubSnapshot->Error});
                                break;
                            }
                        } else {
                            partitionContext.Update({.PartitionError = TError(
                                "Queue sub-snapshot for consumer %Qv does not contain a snapshot for partition %v",
                                consumerRef,
                                partitionIndex)});
                            break;
                        }
                    }
                }
            }
        }

        //! Updates partition contexts in accordance with the retained_lifetime_duration parameter.
        //! Only affects the maximum trimmed row count.
        //! Internally, fetches safe row indexes to trim based on the current generated timestamp and the specified duration.
        void HandleRetainedLifetimeDuration(const TQueueAutoTrimConfig& autoTrimConfig)
        {
            const auto& lifetimeDuration = autoTrimConfig.RetainedLifetimeDuration;

            if (!lifetimeDuration) {
                return;
            }

            auto maxTimestampToTrim = GetMaxTimestampToTrim(*lifetimeDuration);

            std::vector<NApi::TGetOrderedTabletSafeTrimRowCountRequest> safeTrimRowCountRequests;
            safeTrimRowCountRequests.reserve(QueueSnapshot->PartitionCount);

            for (const auto& partitionContext : Context.Partitions) {
                if (!partitionContext) {
                    // We don't need to check partitions with errors, since we will not be trimming them in any case.
                    continue;
                }

                safeTrimRowCountRequests.push_back(
                    NApi::TGetOrderedTabletSafeTrimRowCountRequest{
                        Context.ObjectPath,
                        partitionContext.PartitionIndex,
                        maxTimestampToTrim
                    });
            }

            auto internalClient = DynamicPointerCast<NApi::IInternalClient>(Client);

            auto safeTrimRowCountsOrError = WaitFor(internalClient->GetOrderedTabletSafeTrimRowCount(safeTrimRowCountRequests));
            if (!safeTrimRowCountsOrError.IsOK()) {
                THROW_ERROR_EXCEPTION(
                    "Unable to get safe trim row counts for replica %Qv to satisfy configured trimming parameters, trimming iteration skipped",
                    Context.Ref)
                    << safeTrimRowCountsOrError;
            }
            const auto& safeTrimRowCountsOrErrors = safeTrimRowCountsOrError.Value();

            for (int safeTrimRowCountsIndex = 0; safeTrimRowCountsIndex < std::ssize(safeTrimRowCountsOrErrors); ++safeTrimRowCountsIndex) {
                const auto& safeTrimRowCountOrError = safeTrimRowCountsOrErrors[safeTrimRowCountsIndex];
                int partitionIndex = safeTrimRowCountRequests[safeTrimRowCountsIndex].TabletIndex;
                if (!safeTrimRowCountOrError.IsOK()) {
                    // Requests were made for non-erroneous partitions only, so there should be no pre-existing error.
                    Context.Partitions[partitionIndex].SetError(TError(
                        "Error getting safe trim row count by timestamp %v, not trimming partition %v",
                        maxTimestampToTrim,
                        partitionIndex)
                        << safeTrimRowCountOrError);
                } else {
                    Context.Partitions[partitionIndex].Update({
                        .MaxTrimmedRowCount = safeTrimRowCountOrError.Value(),
                    });
                }
            }
        }

        //! Updates partition contexts in accordance with the retained_rows parameter.
        //! Only affects the maximum trimmed row count.
        void HandleRetainedRows(const TQueueAutoTrimConfig& autoTrimConfig)
        {
            const auto& retainedRows = autoTrimConfig.RetainedRows;
            if (!retainedRows) {
                return;
            }

            for (const auto& [partitionContext, partitionSnapshot] : Zip(Context.Partitions, Context.ReplicaSnapshot->PartitionSnapshots)) {
                partitionContext.Update({
                    .MaxTrimmedRowCount = std::max<i64>(partitionSnapshot->UpperRowIndex - *retainedRows, 0),
                });
            }
        }

        //! Updates partition contexts in accordance with the offsets of vital consumers and exports.
        //! Only affects the minimum trimmed row count.
        void HandleVitalConsumersAndExports()
        {
            for (auto& partitionContext : Context.Partitions) {
                if (!partitionContext) {
                    continue;
                }

                std::optional<i64> minTrimmedRowCount;

                // Handle vital consumers.
                for (const auto& [consumerRef, consumerSubSnapshot] : VitalConsumerSubSnapshots) {
                    minTrimmedRowCount = MinOrValue<i64>(
                        minTrimmedRowCount,
                        // NextRowIndex should always be present in the snapshot.
                        consumerSubSnapshot->PartitionSnapshots[partitionContext.PartitionIndex]->NextRowIndex);
                }

                // Handle queue exports.
                if (QueueExportProgress) {
                    for (auto& [exportName, exportProgress] : QueueExportProgress) {
                        auto partitionProgressIt = exportProgress->Tablets.find(partitionContext.PartitionIndex);
                        if (partitionProgressIt == exportProgress->Tablets.end()) {
                            minTrimmedRowCount = MinOrValue<i64>(minTrimmedRowCount, 0);
                            continue;
                        }
                        const auto& partitionProgress = partitionProgressIt->second;
                        minTrimmedRowCount = MinOrValue<i64>(
                            minTrimmedRowCount,
                            partitionProgress->RowCount);
                    }
                }

                partitionContext.Update({
                    .MinTrimmedRowCount = minTrimmedRowCount,
                });
            }
        }

        //! Performs and awaits individual trimming request for each partition.
        void RequestTrimming()
        {
            std::vector<TFuture<void>> asyncTrims;
            asyncTrims.reserve(QueueSnapshot->PartitionCount);

            std::vector<int> trimmedPartitions;
            trimmedPartitions.reserve(QueueSnapshot->PartitionCount);

            for (const auto& partitionContext : Context.Partitions) {
                auto partitionIndex = partitionContext.PartitionIndex;
                const auto& partitionSnapshot = Context.ReplicaSnapshot->PartitionSnapshots[partitionIndex];
                auto currentTrimmedRowCount = partitionSnapshot->LowerRowIndex;

                // TODO(achulkov2): Ideally, we want to have more verbose per-partition logging (including min/max),
                // but even the message below gets logged too much. We need to find a way to make the logging more compact,
                // maybe by aggregating by queue and only logging changes for the first 100 or so partitions.
                if (auto updatedTrimmedRowCount = partitionContext.GetUpdatedTrimmedRowCount(currentTrimmedRowCount)) {
                    YT_LOG_DEBUG(
                        "Trimming partition (Partition: %v, TrimmedRowCount: %v -> %v)",
                        partitionIndex,
                        currentTrimmedRowCount,
                        *updatedTrimmedRowCount);
                    asyncTrims.push_back(Client->TrimTable(
                        Context.ObjectPath, partitionIndex, *updatedTrimmedRowCount));
                    trimmedPartitions.push_back(partitionIndex);
                }
            }

            auto trimmingResults = WaitFor(AllSet(asyncTrims))
                .ValueOrThrow();
            for (const auto& [partitionIndex, trimmingResult] : Zip(trimmedPartitions, trimmingResults)) {
                if (!trimmingResult.IsOK()) {
                    Context.Partitions[partitionIndex].SetError(TError(
                        "Error occurred while executing trimming request for partition %v", partitionIndex)
                        << trimmingResult);
                }
            }
        }

        void ReportErrors()
        {
            std::vector<TError> partitionTrimErrors;
            for (const auto& partitionContext : Context.Partitions) {
                if (partitionContext.HasCriticalError()) {
                    partitionTrimErrors.push_back(partitionContext.PartitionError << TErrorAttribute("partition_index", partitionContext.PartitionIndex));
                }
            }

            if (!partitionTrimErrors.empty()) {
                THROW_ERROR_EXCEPTION("Failed to trim %v partitions", partitionTrimErrors.size())
                    << partitionTrimErrors;
            }
        }
    };
};

DEFINE_REFCOUNTED_TYPE(TOrderedDynamicTableController)

////////////////////////////////////////////////////////////////////////////////

class TErrorQueueController
    : public IQueueController
{
public:
    TErrorQueueController(
        TQueueTableRow row,
        std::optional<TReplicatedTableMappingTableRow> replicatedTableMappingRow,
        TError error)
        : Row_(std::move(row))
        , ReplicatedTableMappingRow_(std::move(replicatedTableMappingRow))
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
        // Row update is handled in UpdateQueueController.
    }

    void OnReplicatedTableMappingRowUpdated(const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& /*row*/) override
    {
        // Row update is handled in UpdateQueueController.
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
                .Item("replicated_table_mapping_row").Value(ReplicatedTableMappingRow_)
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
    const TQueueTableRow Row_;
    const std::optional<TReplicatedTableMappingTableRow> ReplicatedTableMappingRow_;
    const TError Error_;
    const TQueueSnapshotPtr Snapshot_;
};

DEFINE_REFCOUNTED_TYPE(TErrorQueueController)

////////////////////////////////////////////////////////////////////////////////

bool UpdateQueueController(
    IObjectControllerPtr& controller,
    bool leading,
    const TQueueTableRow& row,
    const std::optional<TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
    const IObjectStore* store,
    TQueueControllerDynamicConfigPtr dynamicConfig,
    TQueueAgentClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker)
{
    // Recreating an error controller on each iteration seems ok as it does
    // not have any state. By doing so we make sure that the error of a queue controller
    // is not stale.

    if (row.SynchronizationError && !row.SynchronizationError->IsOK()) {
        controller = New<TErrorQueueController>(row, replicatedTableMappingRow, TError("Queue synchronization error") << *row.SynchronizationError);
        return true;
    }

    auto queueFamily = DeduceQueueFamily(row, replicatedTableMappingRow);
    if (!queueFamily.IsOK()) {
        controller = New<TErrorQueueController>(row, replicatedTableMappingRow, queueFamily);
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
                replicatedTableMappingRow,
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
