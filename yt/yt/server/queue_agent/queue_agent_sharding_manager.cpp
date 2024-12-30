#include "queue_agent_sharding_manager.h"
#include "config.h"

#include <yt/yt/ytlib/discovery_client/helpers.h>
#include <yt/yt/ytlib/discovery_client/discovery_client.h>
#include <yt/yt/ytlib/discovery_client/member_client.h>

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/ypath_service.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <atomic>

namespace NYT::NQueueAgent {

using namespace NAlertManager;
using namespace NApi;
using namespace NConcurrency;
using namespace NDiscoveryClient;
using namespace NQueueClient;
using namespace NTracing;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = QueueAgentShardingManagerLogger;

////////////////////////////////////////////////////////////////////////////////

inline const TString BannedQueueAgentInstanceAttributeName = "banned_queue_agent_instance";

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentShardingManager
    : public IQueueAgentShardingManager
{
public:
    TQueueAgentShardingManager(
        IInvokerPtr controlInvoker,
        IClientPtr client,
        IAlertCollectorPtr alertCollector,
        TDynamicStatePtr dynamicState,
        IMemberClientPtr memberClient,
        IDiscoveryClientPtr discoveryClient,
        std::string queueAgentStage,
        TYPath dynamicStateRoot)
        : DynamicConfig_(New<TQueueAgentShardingManagerDynamicConfig>())
        , Client_(std::move(client))
        , ControlInvoker_(std::move(controlInvoker))
        , AlertCollector_(std::move(alertCollector))
        , DynamicState_(std::move(dynamicState))
        , MemberClient_(std::move(memberClient))
        , DiscoveryClient_(std::move(discoveryClient))
        , QueueAgentStage_(std::move(queueAgentStage))
        , DynamicStateRoot_(std::move(dynamicStateRoot))
        , PassExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TQueueAgentShardingManager::Pass, MakeWeak(this)),
            DynamicConfig_.Acquire()->PassPeriod))
        , OrchidService_(IYPathService::FromProducer(BIND(&TQueueAgentShardingManager::BuildOrchid, MakeWeak(this)))
            ->Via(ControlInvoker_))
        , SyncBannedQueueAgentInstancesFrequency_(CalculateSyncBannedQueueAgentInstancesFrequency(*DynamicConfig_.Acquire()))
    { }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

    void Start() const override
    {
        PassExecutor_->Start();
    }

    void OnDynamicConfigChanged(
        const TQueueAgentShardingManagerDynamicConfigPtr& oldConfig,
        const TQueueAgentShardingManagerDynamicConfigPtr& newConfig) override
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        DynamicConfig_.Exchange(newConfig);

        PassExecutor_->SetPeriod(newConfig->PassPeriod);

        SyncBannedQueueAgentInstancesFrequency_.exchange(CalculateSyncBannedQueueAgentInstancesFrequency(*newConfig));

        YT_LOG_DEBUG(
            "Updated queue agent manager dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

private:
    using TQueueAgentShardingManagerDynamicConfigAtomicPtr = TAtomicIntrusivePtr<TQueueAgentShardingManagerDynamicConfig>;
    TQueueAgentShardingManagerDynamicConfigAtomicPtr DynamicConfig_;
    IClientPtr Client_;
    const IInvokerPtr ControlInvoker_;
    const IAlertCollectorPtr AlertCollector_;
    const TDynamicStatePtr DynamicState_;
    const IMemberClientPtr MemberClient_;
    const IDiscoveryClientPtr DiscoveryClient_;
    const std::string QueueAgentStage_;
    const TYPath DynamicStateRoot_;
    const TPeriodicExecutorPtr PassExecutor_;
    const IYPathServicePtr OrchidService_;

    std::atomic<bool> Active_ = false;
    //! Current pass iteration error.
    TError PassError_;
    //! Current pass iteration instant.
    TInstant PassInstant_ = TInstant::Zero();
    //! Index of the current pass iteration.
    i64 PassIndex_ = -1;
    //! Cached set of banned queue agent instances.
    THashSet<TMemberId> BannedQueueAgentInstances_;
    std::atomic<i64> SyncBannedQueueAgentInstancesFrequency_ = 0;

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        BuildYsonFluently(consumer).BeginMap()
            .Item("active").Value(Active_)
            .Item("pass_instant").Value(PassInstant_)
            .Item("pass_index").Value(PassIndex_)
            .Item("pass_error").Value(PassError_)
        .EndMap();
    }

    void Pass()
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        PassInstant_ = TInstant::Now();
        ++PassIndex_;

        try {
            GuardedPass();
            PassError_ = TError();
        } catch (const std::exception& ex) {
            PassError_ = ex;
            YT_LOG_ERROR(ex, "Error performing queue agent manager pass (PassIndex: %v)", PassIndex_);
            AlertCollector_->StageAlert(CreateAlert(
                NAlerts::EErrorCode::QueueAgentShardingManagerPassFailed,
                "Error performing queue agent manager pass",
                /*tags*/ {},
                ex));
        }

        AlertCollector_->PublishAlerts();
    }

    //! Uses FarmFingerprint as a deterministic (both platform and time-agnostic) hash.
    static size_t FarmHashCombine(size_t hash, const std::vector<TStringBuf>& values)
    {
        for (const auto& value : values) {
            HashCombine(hash, FarmFingerprint(value));
        }
        return hash;
    }

    //! Picks host using rendezvous hashing.
    //! The probability of host reassignment in case of any small host set changes is low.
    static std::string PickHost(const TCrossClusterReference& object, const std::vector<TMemberInfo>& queueAgents)
    {
        YT_VERIFY(!queueAgents.empty());

        auto objectHash = FarmHashCombine(0, {object.Cluster, object.Path});

        auto getCombinedHash = [objectHash] (const TMemberInfo& queueAgent) {
            return FarmHashCombine(objectHash, {queueAgent.Id});
        };

        auto it = std::min_element(queueAgents.begin(), queueAgents.end(), [&] (const TMemberInfo& lhs, const TMemberInfo& rhs) {
            return std::pair{getCombinedHash(lhs), lhs.Id} < std::pair{getCombinedHash(rhs), rhs.Id};
        });

        return it->Id;
    }

    bool ShouldSyncBannedQueueAgentInstances() const
    {
        auto frequency = SyncBannedQueueAgentInstancesFrequency_.load();
        if (frequency <= 0) {
            // NB(apachee): Sync every pass, if SyncBannedInstancesPeriod is too small.
            return true;
        }

        return PassIndex_ % frequency == 0;
    }

    void SyncBannedQueueAgentInstances()
    {
        BannedQueueAgentInstances_.clear();

        YT_LOG_DEBUG("Synchronization of banned queue agents started");
        auto logFinally = Finally([&] {
            YT_LOG_DEBUG("Synchronization of banned queue agents finished (BannedInstances: %v)", BannedQueueAgentInstances_);
        });

        auto instancesPath = YPathJoin(DynamicStateRoot_, "instances");
        auto options = TListNodeOptions{
            .Attributes = TAttributeFilter({BannedQueueAgentInstanceAttributeName}),
        };

        auto yson = WaitFor(Client_->ListNode(instancesPath, options))
            .ValueOrThrow();
        auto instances = ConvertTo<IListNodePtr>(yson);

        for (const auto& instance : instances->GetChildren()) {
            YT_VERIFY(instance->GetType() == ENodeType::String);
            const auto& attributes = instance->Attributes();
            try {
                if (attributes.Get<bool>(BannedQueueAgentInstanceAttributeName)) {
                    BannedQueueAgentInstances_.insert(instance->GetValue<TString>());
                }
            } catch (std::exception& ex) {
                // NB(apachee): Ignore if attribute is missing, or if its value is not bool.
            }
        }
    }

    void GuardedPass()
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("QueueAgentShardingManager"));

        auto Logger = QueueAgentShardingManagerLogger().WithTag("PassIndex: %v", PassIndex_);

        YT_LOG_INFO("Pass started");
        auto logFinally = Finally([&] {
            YT_LOG_INFO("Pass finished");
        });

        // Collect discovery information.

        auto queueAgents = WaitFor(DiscoveryClient_->ListMembers(MemberClient_->GetGroupId(), {}))
            .ValueOrThrow();

        YT_LOG_DEBUG("Collected discovery information (Members: %v)", queueAgents.size());

        if (queueAgents.empty()) {
            THROW_ERROR_EXCEPTION("No queue agents in discovery");
        }

        if (ShouldSyncBannedQueueAgentInstances()) {
            SyncBannedQueueAgentInstances();
        } else {
            YT_LOG_DEBUG("Skipped the synchronization of banned queue agent instances");
        }

        // Filter out banned queue agent instances.

        std::vector<TMemberInfo> filteredQueueAgents;
        filteredQueueAgents.reserve(queueAgents.size());

        for (auto& queueAgent : queueAgents) {
            if (BannedQueueAgentInstances_.contains(queueAgent.Id)) {
                continue;
            }
            filteredQueueAgents.push_back(std::move(queueAgent));
        }

        if (filteredQueueAgents.empty()) {
            THROW_ERROR_EXCEPTION("All active instances are banned, leading host can't be chosen, skipping pass");
        }

        if (filteredQueueAgents[0].Id != MemberClient_->GetId()) {
            YT_LOG_DEBUG("Queue agent is not leading, skipping pass (LeadingHost: %v)", filteredQueueAgents[0].Id);
            Active_ = false;
            return;
        } else {
            Active_ = true;
        }

        // Collect rows from dynamic state.

        auto where = Format("[queue_agent_stage] = \"%v\"", QueueAgentStage_);
        auto asyncQueueRows = DynamicState_->Queues->Select(where);
        auto asyncConsumerRows = DynamicState_->Consumers->Select(where);
        auto asyncObjectMappingRows = DynamicState_->QueueAgentObjectMapping->Select();

        std::vector<TFuture<void>> futures{
            asyncQueueRows.AsVoid(),
            asyncConsumerRows.AsVoid(),
            asyncObjectMappingRows.AsVoid(),
        };

        WaitFor(AllSucceeded(futures))
            .ThrowOnError();

        const auto& queueRows = asyncQueueRows.Get().Value();
        const auto& consumerRows = asyncConsumerRows.Get().Value();
        const auto& objectMappingRows = asyncObjectMappingRows.Get().Value();

        YT_LOG_DEBUG(
            "State table rows collected (QueueRowCount: %v, ConsumerRowCount: %v, ObjectMappingRowCount: %v)",
            queueRows.size(),
            consumerRows.size(),
            objectMappingRows.size());

        // Map all objects to their responsible queue agents via rendezvous hashing.

        THashSet<TCrossClusterReference> allObjects;
        for (const auto& queueRow : queueRows) {
            allObjects.insert(queueRow.Ref);
        }
        for (const auto& consumerRow : consumerRows) {
            allObjects.insert(consumerRow.Ref);
        }

        auto currentMapping = TQueueAgentObjectMappingTable::ToMapping(objectMappingRows);

        std::vector<TQueueAgentObjectMappingTableRow> rowsToModify;
        std::vector<TQueueAgentObjectMappingTableRow> keysToDelete;

        for (const auto& object : allObjects) {
            auto responsibleQueueAgentHost = PickHost(object, filteredQueueAgents);

            auto currentMappingIt = currentMapping.find(object);
            // We don't want to modify rows for which the host hasn't changed.
            if (currentMappingIt == currentMapping.end() || currentMappingIt->second != responsibleQueueAgentHost) {
                rowsToModify.push_back(TQueueAgentObjectMappingTableRow{
                    .Object = object,
                    .QueueAgentHost = responsibleQueueAgentHost,
                });
                YT_LOG_DEBUG(
                    "Assigning object to queue agent (Object: %v, QueueAgentHost: %v -> %v)",
                    object,
                    (currentMappingIt == currentMapping.end() ? std::nullopt : std::optional(currentMappingIt->second)),
                    responsibleQueueAgentHost);
            }
        }

        // Queues & consumers that are no longer present in the dynamic state should be deleted.
        for (const auto& [object, queueAgentHost] : currentMapping) {
            if (!allObjects.contains(object)) {
                keysToDelete.push_back(TQueueAgentObjectMappingTableRow{
                    .Object = object,
                });
                YT_LOG_DEBUG(
                    "Removing object from mapping (Object: %v, LastQueueAgentHost: %v)",
                    object,
                    queueAgentHost);
            }
        }

        // The keys in these two requests shouldn't intersect by design.
        WaitFor(AllSucceeded(std::vector{
            DynamicState_->QueueAgentObjectMapping->Insert(rowsToModify),
            DynamicState_->QueueAgentObjectMapping->Delete(keysToDelete)}))
            .ThrowOnError();
        YT_LOG_DEBUG(
            "Updated queue agent object mapping (RowsModified: %v, RowsDeleted: %v)",
            rowsToModify.size(),
            keysToDelete.size());
    }

    i64 CalculateSyncBannedQueueAgentInstancesFrequency(const TQueueAgentShardingManagerDynamicConfig& dynamicConfig) const
    {
        auto syncBannedInstancesPeriodValue = dynamicConfig.SyncBannedInstancesPeriod.GetValue();
        auto passPeriodValue = dynamicConfig.PassPeriod.GetValue();
        auto frequency = (syncBannedInstancesPeriodValue + passPeriodValue - 1) / passPeriodValue;
        return frequency;
    }
};

IQueueAgentShardingManagerPtr CreateQueueAgentShardingManager(
    IInvokerPtr controlInvoker,
    IClientPtr client,
    IAlertCollectorPtr alertCollector,
    TDynamicStatePtr dynamicState,
    IMemberClientPtr memberClient,
    IDiscoveryClientPtr discoveryClient,
    std::string queueAgentStage,
    TYPath dynamicStateRoot)
{
    return New<TQueueAgentShardingManager>(
        std::move(controlInvoker),
        std::move(client),
        std::move(alertCollector),
        std::move(dynamicState),
        std::move(memberClient),
        std::move(discoveryClient),
        std::move(queueAgentStage),
        std::move(dynamicStateRoot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
