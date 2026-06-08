#include "multi_consumer_controller.h"

#include "snapshot.h"
#include "config.h"
#include "helpers.h"
#include "pass_profiler.h"
#include "multi_consumer_profile_manager.h"
#include "object.h"
#include "snapshot_representation.h"

#include <yt/yt/server/lib/alert_manager/alert_manager.h>

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

#include <library/cpp/yt/misc/range_helpers.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NQueueAgent {

using namespace NAlertManager;
using namespace NQueryClient;
using namespace NQueueClient;
using namespace NTabletClient;
using namespace NLogging;
using namespace NConcurrency;
using namespace NApi;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NProfiling;

using namespace std::placeholders;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TMultiConsumerController
    : public IObjectController
{
public:
    TMultiConsumerController(
        const TIntrusivePtr<TConsumerTableRow>& row,
        const std::optional<TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
        const TQueueControllerDynamicConfigPtr& dynamicConfig,
        const TProfiler& profiler,
        TQueueAgentClientDirectoryPtr clientDirectory,
        IInvokerPtr invoker,
        TMultiConsumerNameTablePtr multiConsumerTable)
        : Path_(row->Path)
        , MultiConsumerTable_(std::move(multiConsumerTable))
        , ClientDirectory_(std::move(clientDirectory))
        , Invoker_(std::move(invoker))
        , Logger(MultiConsumerControllerLogger().WithTag("MultiConsumer: %v", Path_))
        , PassExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TMultiConsumerController::Pass, MakeWeak(this)),
            dynamicConfig->PassPeriod))
        , BaseProfiler_(profiler)
        , ConsumerRow_(row)
        , ReplicatedTableMappingRow_(replicatedTableMappingRow)
        , DynamicConfig_(dynamicConfig)
        , ProfileManager_(CreateMultiConsumerProfileManager(profiler, Logger, *row))
        , PassProfiler_(New<TPassProfiler>(ProfileManager_.Acquire()->GetProfiler(EProfilerScope::ObjectPass)))
        , AlertManager_(CreateAlertManager(
            Logger,
            ProfileManager_.Acquire()->GetProfiler(EProfilerScope::AlertManager),
            Invoker_))
        , SynchronizationAlertCollector_(CreateAlertCollector(AlertManager_.Acquire()))
    {
        // Prepare initial erroneous snapshot.
        auto snapshot = New<TMultiConsumerSnapshot>();
        snapshot->Row = row;
        snapshot->ReplicatedTableMappingRow = replicatedTableMappingRow;
        snapshot->Error = TError("Consumer is not processed yet");
        Snapshot_.Store(std::move(snapshot));
    }

    void Initialize() const
    {
        PassExecutor_->Start();
        AlertManager_.Acquire()->Start();

        YT_LOG_INFO("Multi consumer controller started");
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(PassExecutor_->Stop());
    }

    void OnRowUpdated(std::any row) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        const auto& consumerRow = std::any_cast<const TIntrusivePtr<TConsumerTableRow>&>(row);

        auto oldRow = ConsumerRow_.Exchange(consumerRow);
        if (oldRow->QueueConsumerProfilingTag != consumerRow->QueueConsumerProfilingTag) {
            ProfileManager_.Store(CreateMultiConsumerProfileManager(BaseProfiler_, Logger, *consumerRow));
            PassProfiler_.Store(New<TPassProfiler>(ProfileManager_.Acquire()->GetProfiler(EProfilerScope::ObjectPass)));

            auto alertManager = CreateAlertManager(
                Logger,
                ProfileManager_.Acquire()->GetProfiler(EProfilerScope::AlertManager),
                Invoker_);

            SynchronizationAlertCollector_.Acquire()->PublishAlerts();
            SynchronizationAlertCollector_.Acquire()->Stop();

            SynchronizationAlertCollector_.Store(CreateAlertCollector(alertManager));
            alertManager->Start();
            AlertManager_.Store(alertManager);
        }
    }

    void OnReplicatedTableMappingRowUpdated(const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& row) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        ReplicatedTableMappingRow_.Store(row);
    }

    void OnDynamicConfigChanged(
        const TQueueControllerDynamicConfigPtr& oldConfig,
        const TQueueControllerDynamicConfigPtr& newConfig) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        DynamicConfig_.Store(newConfig);

        PassExecutor_->SetPeriod(newConfig->PassPeriod);

        YT_LOG_DEBUG(
            "Updated multi consumer controller dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

    TRefCountedPtr GetLatestSnapshot() const override
    {
        return Snapshot_.Acquire();
    }

    void BuildOrchid(IYsonConsumer* consumer) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto snapshot = Snapshot_.Acquire();

        YT_LOG_DEBUG("Building multi consumer controller orchid (PassIndex: %v, TotalConsumersCount: %v)",
            snapshot->PassIndex, snapshot->QueueConsumerNames.size());

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("pass_index").Value(snapshot->PassIndex)
                .Item("pass_instant").Value(snapshot->PassInstant)
                .Item("row").Value(snapshot->Row)
                .Item("replicated_table_mapping_row").Value(snapshot->ReplicatedTableMappingRow)
                .Item("status").Do(std::bind_front(BuildMultiConsumerStatusYson, snapshot, AlertManager_.Acquire()))
            .EndMap();
    }

    bool IsLeading() const override
    {
        return true;
    }

private:
    const TTablePath Path_;

    const TMultiConsumerNameTablePtr MultiConsumerTable_;
    const TQueueAgentClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr Invoker_;

    using TMultiConsumerSnapshotAtomicPtr = TAtomicIntrusivePtr<TMultiConsumerSnapshot>;
    TMultiConsumerSnapshotAtomicPtr Snapshot_;

    const TLogger Logger;
    const TPeriodicExecutorPtr PassExecutor_;

    const TProfiler BaseProfiler_;

    TAtomicIntrusivePtr<TConsumerTableRow> ConsumerRow_;
    NThreading::TAtomicObject<std::optional<TReplicatedTableMappingTableRow>> ReplicatedTableMappingRow_;

    using TQueueControllerDynamicConfigAtomicPtr = TAtomicIntrusivePtr<TQueueControllerDynamicConfig>;
    TQueueControllerDynamicConfigAtomicPtr DynamicConfig_;

    TAtomicIntrusivePtr<IMultiConsumerProfileManager> ProfileManager_;
    TAtomicIntrusivePtr<TPassProfiler> PassProfiler_;
    TAtomicIntrusivePtr<IAlertManager> AlertManager_;
    TAtomicIntrusivePtr<IAlertCollector> SynchronizationAlertCollector_;

    void Pass()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto startTime = TInstant::Now();
        auto previousSnapshot = Snapshot_.Acquire();
        YT_VERIFY(previousSnapshot);
        auto passIndex = previousSnapshot->PassIndex + 1;

        YT_LOG_INFO("Multi consumer controller pass started (PassIndex: %v)", passIndex);

        auto passProfiler = PassProfiler_.Acquire();
        YT_VERIFY(passProfiler);
        passProfiler->OnStart(passIndex, startTime);

        try {
            GuardedPass(previousSnapshot, startTime);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Multi consumer controller pass failed");
            SynchronizationAlertCollector_.Acquire()->StageAlert(CreateAlert(
                NAlerts::EErrorCode::QueueAgentMultiConsumerControllerPassFailed,
                "Multi consumer controller pass failed",
                /*tags*/ {},
                /*error*/ ex));
            passProfiler->OnError();
        }

        YT_LOG_INFO("Multi consumer controller pass finished (PassIndex: %v)", passIndex);
        passProfiler->OnFinish(TInstant::Now() - startTime);
        SynchronizationAlertCollector_.Acquire()->PublishAlerts();
    }

    void GuardedPass(const TMultiConsumerSnapshotPtr& previousSnapshot, const TInstant& startTime)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto snapshotFuture = BuildSnapshot(previousSnapshot, startTime);
        auto consumerNamesInStateFuture = SelectConsumerNamesFromState();

        if (auto error = WaitFor(AllSucceeded(std::vector{snapshotFuture.AsVoid(), consumerNamesInStateFuture.AsVoid()})); !error.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to build snapshot or fetch consumer names in state")
                << error;
        }
        auto snapshot = snapshotFuture.GetOrCrash().Value();
        Snapshot_.Store(snapshot);

        YT_LOG_DEBUG("Multi consumer snapshot updated");

        if (snapshot->Banned) {
            YT_LOG_INFO(snapshot->Error, "Multi consumer is banned");
            return;
        }
        if (!snapshot->Error.IsOK()) {
            THROW_ERROR_EXCEPTION("Multi consumer controller has snapshot error")
                << snapshot->Error;
        }

        auto consumerNamesInState = consumerNamesInStateFuture.GetOrCrash().Value();

        auto namesToWrite = snapshot->QueueConsumerNames
            | std::views::filter([&] (const auto& name) {
                return !consumerNamesInState.contains(name) || consumerNamesInState[name] != snapshot->Row->QueueAgentStage;
            })
            | RangeTo<THashSet<std::string>>();

        auto consumersInStateSize = consumerNamesInState.size();
        auto namesToDelete = std::move(consumerNamesInState)
            | std::views::keys
            | std::views::filter([&] (const auto& name) {
                return !snapshot->QueueConsumerNames.contains(name);
            })
            | RangeTo<THashSet<std::string>>();

        YT_LOG_DEBUG("Performing mutating operations (NamesToWriteCount: %v, NamesToDeleteCount: %v, ConsumersInStateCount: %v, ConsumersInTableCount: %v)",
            namesToWrite.size(), namesToDelete.size(), consumersInStateSize, snapshot->QueueConsumerNames.size());
        if (auto error = WaitFor(SyncConsumerNamesInState(namesToWrite, namesToDelete));
                !error.IsOK()) {
            THROW_ERROR_EXCEPTION("Error while synchronizing multi_consumer_names table")
                << error;
        }

        ProfileManager_.Acquire()->Profile(previousSnapshot, snapshot);
    }

    TFuture<TMultiConsumerSnapshotPtr> BuildSnapshot(
        const TMultiConsumerSnapshotPtr& previousSnapshot,
        const TInstant& passInstant) const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto snapshot = New<TMultiConsumerSnapshot>();
        snapshot->Row = ConsumerRow_.Acquire();
        snapshot->ReplicatedTableMappingRow = ReplicatedTableMappingRow_.Load();
        snapshot->PassIndex = previousSnapshot->PassIndex + 1;
        snapshot->PassInstant = passInstant;
        snapshot->QueueConsumerNames = previousSnapshot->QueueConsumerNames;
        if (snapshot->Row->QueueAgentBanned.value_or(false)) {
            snapshot->Banned = true;

            // NB(apachee): Instead of relying on invariant that BannedSince is present when Banned is true
            // check BannedSince directly to avoid the hassle of guaranteeing said invariant.
            if (previousSnapshot->BannedSince) {
                snapshot->BannedSince = previousSnapshot->BannedSince;
            } else {
                snapshot->BannedSince = snapshot->PassInstant;
            }

            snapshot->Error = TError("Multi consumer is banned by \"queue_agent_banned\" attribute")
                << TErrorAttribute("banned_since", snapshot->BannedSince);

            return MakeFuture(snapshot);
        }

        try {
            ValidateConsumer(*snapshot->Row, snapshot->ReplicatedTableMappingRow);
        } catch (const std::exception& ex) {
            snapshot->Error = ex;
            YT_LOG_WARNING(snapshot->Error, "Invalid multi consumer");
            return MakeFuture(snapshot);
        }

        return SelectConsumerNamesFromUserTable(snapshot)
            .Apply(BIND([snapshot] (const TErrorOr<THashSet<std::string>>& consumerNamesOrError) {
                if (consumerNamesOrError.IsOK()) {
                    snapshot->QueueConsumerNames = consumerNamesOrError.Value();
                } else {
                    snapshot->Error = TError("Error while selecting from user table")
                        << consumerNamesOrError;
                }
                return snapshot;
            }));
    }

    TFuture<THashSet<std::string>> SelectConsumerNamesFromUserTable(const TMultiConsumerSnapshotPtr& snapshot) const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        // TODO(YT-28372): This might work incorrectly if there are a too many rows in the table. We might need to add pagination here.
        auto clientContext = ClientDirectory_->GetDataReadContext(*snapshot->Row, snapshot->ReplicatedTableMappingRow, /*onlyDataReplicas*/ true);
        auto query = Format("queue_consumer_name FROM [%v] GROUP BY queue_consumer_name", clientContext.Path);
        return clientContext.Client->SelectRows(query)
            .Apply(BIND([] (const TSelectRowsResult& selectResult) {
                return selectResult.Rowset->GetRows()
                    | std::views::transform([] (const auto& row) {
                        THROW_ERROR_EXCEPTION_IF(row.GetCount() != 1, "Expected 1 value in row while selecting \"queue_consumer_name\"");
                        return FromUnversionedValue<std::string>(row[0]);
                    })
                    | RangeTo<THashSet<std::string>>();
            }));
    }

    TFuture<THashMap<std::string, std::optional<std::string>>> SelectConsumerNamesFromState() const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        constexpr auto where = "cluster = {cluster} AND path = {path}";
        TSelectRowsOptions options;
        options.PlaceholderValues = BuildYsonStringFluently()
            .BeginMap()
                .Item("cluster").Value(Path_.GetCluster().value())
                .Item("path").Value(Path_.GetPath())
            .EndMap();
        return MultiConsumerTable_->Select(where, options)
            .Apply(BIND([] (const std::vector<TMultiConsumerNameTableRow>& selectResult) {
                return selectResult
                    | std::views::transform([] (const auto& row) {
                        return std::pair{row.Ref.GetQueueConsumerName().value(), row.QueueAgentStage};
                    })
                    | RangeTo<THashMap<std::string, std::optional<std::string>>>();
            }));
    }

    std::vector<TMultiConsumerNameTableRow> MakeMultiConsumerRows(const THashSet<std::string>& queueConsumerNames) const
    {
        auto queueAgentStage = ConsumerRow_.Acquire()->QueueAgentStage;
        return queueConsumerNames
            | std::views::transform([this, &queueAgentStage] (const auto& consumerName) {
                return TMultiConsumerNameTableRow{
                    .Ref = TNamedConsumerReference(Path_.GetPath(), *MakeConsumerAttributes(Path_.GetCluster().value(), consumerName)),
                    .QueueAgentStage = queueAgentStage,
                };
            })
            | RangeTo<std::vector<TMultiConsumerNameTableRow>>();
    }

    TFuture<void> SyncConsumerNamesInState(const THashSet<std::string>& namesToWrite, const THashSet<std::string>& namesToDelete) const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        std::vector<TFuture<void>> futures;
        if (!namesToWrite.empty()) {
            futures.emplace_back(MultiConsumerTable_->Insert(MakeMultiConsumerRows(namesToWrite)).AsVoid());
        }
        if (!namesToDelete.empty()) {
            futures.emplace_back(MultiConsumerTable_->Delete(MakeMultiConsumerRows(namesToDelete)).AsVoid());
        }
        return AllSucceeded(futures);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

//! Returns true if new controller was created.
bool UpdateMultiConsumerController(
    IObjectControllerPtr& controller,
    const TIntrusivePtr<NQueueClient::TConsumerTableRow>& row,
    const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
    const TQueueControllerDynamicConfigPtr& dynamicConfig,
    const TQueueAgentClientDirectoryPtr& clientDirectory,
    IInvokerPtr invoker,
    TDynamicStatePtr dynamicState)
{
    if (controller) {
        return false;
    }

    auto newController = New<TMultiConsumerController>(
        row,
        replicatedTableMappingRow,
        dynamicConfig,
        QueueAgentProfiler(),
        clientDirectory,
        std::move(invoker),
        dynamicState->MultiConsumerNames);
    newController->Initialize();
    controller = newController;
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
