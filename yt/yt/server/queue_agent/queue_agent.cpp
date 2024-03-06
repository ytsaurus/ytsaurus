#include "queue_agent.h"

#include "config.h"
#include "helpers.h"
#include "snapshot.h"
#include "object.h"
#include "queue_controller.h"
#include "consumer_controller.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/discovery_client/member_client.h>

#include <yt/yt/ytlib/auth/native_authenticating_channel.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NQueueAgent {

using namespace NYTree;
using namespace NObjectClient;
using namespace NOrchid;
using namespace NAlertManager;
using namespace NApi;
using namespace NConcurrency;
using namespace NDiscoveryClient;
using namespace NYson;
using namespace NHydra;
using namespace NQueueClient;
using namespace NTracing;
using namespace NHiveClient;
using namespace NCypressElection;
using namespace NYPath;
using namespace NRpc::NBus;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueueAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TObjectMapBoundService
    : public TVirtualMapBase
{
public:
    TObjectMapBoundService(
        const TQueueAgent* owner,
        EObjectKind objectKind)
        : Owner_(owner)
        , ObjectKind_(objectKind)
        , QueryRoot_(Format("//queue_agent/%lvs", ObjectKind_))
    {
        SetOpaque(false);
    }

    i64 GetSize() const override
    {
        auto guard = ReaderGuard(Owner_->ObjectLock_);

        return Owner_->LeadingObjectCount_[ObjectKind_];
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        auto guard = ReaderGuard(Owner_->ObjectLock_);

        const auto& objectMap = Owner_->Objects_[ObjectKind_];
        const auto& objectToHost = Owner_->ObjectToHost_;

        std::vector<TString> keys;
        keys.reserve(std::min(std::ssize(objectMap), limit));
        for (const auto& [key, _] : objectMap) {
            if (auto it = objectToHost.find(key); it == objectToHost.end() || it->second != Owner_->AgentId_) {
                continue;
            }

            keys.push_back(ToString(key));
            if (std::ssize(keys) == limit) {
                break;
            }
        }
        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        auto guard = ReaderGuard(Owner_->ObjectLock_);

        auto objectRef = TCrossClusterReference::FromString(key);

        const auto& objectToHost = Owner_->ObjectToHost_;
        auto objectToHostIt = objectToHost.find(objectRef);
        if (objectToHostIt == objectToHost.end()) {
            THROW_ERROR_EXCEPTION("Object %Qv is not mapped to any queue agent", objectRef);
        }

        if (objectToHostIt->second != Owner_->AgentId_) {
            return Owner_->RedirectYPathRequest(objectToHostIt->second, QueryRoot_, key);
        }

        const auto& objectMap = Owner_->Objects_[ObjectKind_];

        auto it = objectMap.find(objectRef);
        if (it == objectMap.end()) {
            THROW_ERROR_EXCEPTION("Object %Qv is missing", objectRef);
        }

        return IYPathService::FromProducer(BIND(&IObjectController::BuildOrchid, it->second.Controller));
    }

private:
    // The queue agent is not supposed to be destroyed, so raw pointer is fine.
    const TQueueAgent* Owner_;
    EObjectKind ObjectKind_;
    TString QueryRoot_;
};

////////////////////////////////////////////////////////////////////////////////

TTaggedProfilingCounters::TTaggedProfilingCounters(TProfiler profiler)
    : Queues(profiler.Gauge("/queues"))
    , Consumers(profiler.Gauge("/consumers"))
    , Partitions(profiler.Gauge("/partitions"))
    , TrimmedQueues(profiler.Gauge("/trimmed_queues"))
    , ErroneousQueues(profiler.Gauge("/erroneous_queues"))
    , ErroneousConsumers(profiler.Gauge("/erroneous_consumers"))
{ }

TGlobalProfilingCounters::TGlobalProfilingCounters(TProfiler profiler)
    : Registrations(profiler.Gauge("/registrations"))
{ }

TQueueAgent::TQueueAgent(
    TQueueAgentConfigPtr config,
    NApi::NNative::IConnectionPtr nativeConnection,
    TClientDirectoryPtr clientDirectory,
    IInvokerPtr controlInvoker,
    TDynamicStatePtr dynamicState,
    ICypressElectionManagerPtr electionManager,
    TString agentId)
    : Config_(std::move(config))
    , DynamicConfig_(New<TQueueAgentDynamicConfig>())
    , ClientDirectory_(std::move(clientDirectory))
    , QAClientDirectory_(New<TQueueAgentClientDirectory>(ClientDirectory_))
    , ControlInvoker_(std::move(controlInvoker))
    , DynamicState_(std::move(dynamicState))
    , ElectionManager_(std::move(electionManager))
    , ControllerThreadPool_(CreateThreadPool(DynamicConfig_->ControllerThreadCount, "Controller"))
    , PassExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TQueueAgent::Pass, MakeWeak(this)),
        DynamicConfig_->PassPeriod))
    , AgentId_(std::move(agentId))
    , GlobalProfilingCounters_(QueueAgentProfilerGlobal)
    , QueueAgentChannelFactory_(
        NAuth::CreateNativeAuthenticationInjectingChannelFactory(
            CreateCachingChannelFactory(CreateTcpBusChannelFactory(Config_->BusClient)),
            nativeConnection->GetConfig()->TvmId))
{
    for (auto objectKind : {EObjectKind::Queue, EObjectKind::Consumer}) {
        ObjectServiceNodes_[objectKind] = CreateVirtualNode(
            New<TObjectMapBoundService>(
                this,
                objectKind));
    }
}

void TQueueAgent::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Starting queue agent");

    PassExecutor_->Start();
}

void TQueueAgent::PopulateAlerts(std::vector<TAlert>* alerts) const
{
    WaitFor(
        BIND(&TQueueAgent::DoPopulateAlerts, MakeStrong(this), alerts)
        .AsyncVia(ControlInvoker_)
        .Run())
        .ThrowOnError();
}

void TQueueAgent::DoPopulateAlerts(std::vector<TAlert>* alerts) const
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

    alerts->insert(alerts->end(), Alerts_.begin(), Alerts_.end());
}

IMapNodePtr TQueueAgent::GetOrchidNode() const
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_DEBUG("Executing orchid request (LastSuccessfulPassIndex: %v)", PassIndex_ - 1);

    auto virtualScalarNode = [] (auto callable) {
        return CreateVirtualNode(IYPathService::FromProducer(BIND([callable] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer).Value(callable());
        })));
    };

    auto node = GetEphemeralNodeFactory()->CreateMap();
    node->AddChild("pass_instant", virtualScalarNode([&] { return PassInstant_; }));
    node->AddChild("pass_index", virtualScalarNode([&] { return PassIndex_; }));
    node->AddChild("pass_error", virtualScalarNode([&] { return PassError_; }));
    node->AddChild("queues", ObjectServiceNodes_[EObjectKind::Queue]);
    node->AddChild("consumers", ObjectServiceNodes_[EObjectKind::Consumer]);

    return node;
}

void TQueueAgent::OnDynamicConfigChanged(
    const TQueueAgentDynamicConfigPtr& oldConfig,
    const TQueueAgentDynamicConfigPtr& newConfig)
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

    // NB: We do this in the beginning, so that we use the new config if a context switch happens below.
    DynamicConfig_ = newConfig;

    PassExecutor_->SetPeriod(newConfig->PassPeriod);

    ControllerThreadPool_->Configure(newConfig->ControllerThreadCount);

    {
        auto guard = ReaderGuard(ObjectLock_);

        for (auto objectKind : {EObjectKind::Queue, EObjectKind::Consumer}) {
            for (const auto& [_, object] : Objects_[objectKind]) {
                object.Controller->OnDynamicConfigChanged(oldConfig->Controller, newConfig->Controller);
            }
        }
    }

    YT_LOG_DEBUG(
        "Updated queue agent dynamic config (OldConfig: %v, NewConfig: %v)",
        ConvertToYsonString(oldConfig, EYsonFormat::Text),
        ConvertToYsonString(newConfig, EYsonFormat::Text));
}

TRefCountedPtr TQueueAgent::FindSnapshot(TCrossClusterReference objectRef) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(ObjectLock_);

    for (const auto& objectMap : Objects_) {
        if (auto it = objectMap.find(objectRef); it != objectMap.end()) {
            return it->second.Controller->GetLatestSnapshot();
        }
    }

    return nullptr;
}

std::vector<TConsumerRegistrationTableRow> TQueueAgent::GetRegistrations(
    TCrossClusterReference objectRef,
    EObjectKind objectKind) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(ObjectLock_);

    const auto& objectMap = Objects_[objectKind];
    if (auto it = objectMap.find(objectRef); it != objectMap.end()) {
        return it->second.Registrations;
    }

    return {};
}

void TQueueAgent::Pass()
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

    PassInstant_ = TInstant::Now();
    ++PassIndex_;

    auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("QueueAgent"));

    auto Logger = QueueAgentLogger.WithTag("PassIndex: %v", PassIndex_);

    // Collect queue and consumer rows.

    YT_LOG_INFO("Pass started");
    auto logFinally = Finally([&] {
        YT_LOG_INFO("Pass finished");
    });

    // NB: The tables below contain information about all stages.
    auto asyncQueueRows = DynamicState_->Queues->Select();
    auto asyncConsumerRows = DynamicState_->Consumers->Select();
    auto asyncRegistrationRows = DynamicState_->Registrations->Select();
    // NB: Only contains objects with the same stage as ours.
    auto asyncObjectMappingRows = DynamicState_->QueueAgentObjectMapping->Select();

    std::vector<TFuture<void>> futures{
        asyncQueueRows.AsVoid(),
        asyncConsumerRows.AsVoid(),
        asyncRegistrationRows.AsVoid(),
        asyncObjectMappingRows.AsVoid(),
    };

    if (auto error = WaitFor(AllSucceeded(futures)); !error.IsOK()) {
        PassError_ = error;
        YT_LOG_ERROR(error, "Error while reading dynamic state");
        auto alert = TError(
            NAlerts::EErrorCode::QueueAgentPassFailed,
            "Error while reading dynamic state")
            << error;
        Alerts_ = {CreateAlert<NAlerts::EErrorCode>(alert)};
        return;
    }
    auto queueRows = asyncQueueRows.GetUnique().Value();
    auto consumerRows = asyncConsumerRows.GetUnique().Value();
    const auto& registrationRows = asyncRegistrationRows.Get().Value();
    const auto& objectMappingRows = asyncObjectMappingRows.Get().Value();

    std::vector<TReplicatedTableMappingTableRow> replicatedTableMappingRows;
    // NB: This table might not exist and we should still perform passes.
    auto replicatedTableMappingRowsOrError = WaitForUnique(DynamicState_->ReplicatedTableMapping->Select());
    if (replicatedTableMappingRowsOrError.IsOK()) {
        replicatedTableMappingRows = std::move(replicatedTableMappingRowsOrError.Value());
    } else {
        YT_LOG_DEBUG(replicatedTableMappingRowsOrError, "Error while reading replicated table mapping");
    }

    YT_LOG_INFO(
        "State table rows collected (QueueRowCount: %v, ConsumerRowCount: %v, RegistrationRowCount: %v, "
        "QueueAgentObjectMappingRows: %v, ReplicatedTableMappingRowCount: %v)",
        queueRows.size(),
        consumerRows.size(),
        registrationRows.size(),
        objectMappingRows.size(),
        replicatedTableMappingRows.size());

    auto getHashTable = [] <class T>(const std::vector<T>& rowList) {
        THashMap<TCrossClusterReference, T> result;
        for (const auto& row : rowList) {
            result[row.Ref] = row;
        }
        return result;
    };

    auto allQueues = getHashTable(queueRows);
    auto allConsumers = getHashTable(consumerRows);

    // Fresh queue/consumer -> responsible queue agent mapping.
    auto objectMapping = TQueueAgentObjectMappingTable::ToMapping(objectMappingRows);

    // Mapping from refs for replicated objects to their meta-rows with information about potential replicas.
    auto replicatedTableMapping = getHashTable(replicatedTableMappingRows);

    // Filter only those queues and consumers for which our queue agent is responsible.

    auto filterRows = [&, this] <class T>(std::vector<T>& rowList) {
        rowList.erase(std::remove_if(rowList.begin(), rowList.end(), [&, this] (const T& row) {
            // Do not perform mutating requests to replicated table objects unless flag is set.
            if (!DynamicConfig_->HandleReplicatedObjects && IsReplicatedTableObjectType(row.ObjectType)) {
                return true;
            }

            // NB: We don't need to check the object's stage, since the object to host mapping only contains objects for our stage.
            auto it = objectMapping.find(row.Ref);
            return it == objectMapping.end() || it->second != AgentId_;
        }), rowList.end());
    };

    filterRows(queueRows);
    filterRows(consumerRows);

    // The remaining objects are considered leading for this queue agent.
    // Leading controllers only exist on a single queue agent, whereas follower-controllers can be present on multiple queue agents.

    auto leaderQueueRows = std::move(queueRows);
    auto leaderConsumerRows = std::move(consumerRows);

    TEnumIndexedArray<EObjectKind, TObjectMap> freshObjects;

    auto getReplicatedTableMappingRow = [&] (const TCrossClusterReference& ref) -> std::optional<TReplicatedTableMappingTableRow> {
        if (auto* rowPtr = replicatedTableMapping.FindPtr(ref)) {
            return *rowPtr;
        }
        return {};
    };

    auto updateControllers = [&] (EObjectKind objectKind, const auto& rows, auto updateController, bool leading) {
        VERIFY_READER_SPINLOCK_AFFINITY(ObjectLock_);

        for (const auto& row : rows) {
            YT_LOG_TRACE("Processing row (Kind: %v, Row: %v)", objectKind, ConvertToYsonString(row, EYsonFormat::Text).ToString());
            auto& freshObject = freshObjects[objectKind][row.Ref];
            auto& controller = freshObject.Controller;

            bool reused = false;
            if (auto it = Objects_[objectKind].find(row.Ref); it != Objects_[objectKind].end()) {
                controller = it->second.Controller;
                reused = true;
            }

            // We either recreate controller from scratch, or keep existing controller.
            // If we keep existing controller, we notify it of (potential) row change.

            auto recreated = updateController(
                controller,
                leading,
                row,
                getReplicatedTableMappingRow(row.Ref),
                /*store*/ this,
                DynamicConfig_->Controller,
                QAClientDirectory_,
                ControllerThreadPool_->GetInvoker());

            YT_LOG_DEBUG(
                "Controller updated (Kind: %v, Object: %v, Reused: %v, Recreated: %v, Leading: %v)",
                objectKind,
                row.Ref,
                reused,
                recreated,
                leading);
        }
    };

    // Prepare fresh queue and consumer leading controllers.

    {
        auto guard = ReaderGuard(ObjectLock_);

        updateControllers(EObjectKind::Queue, leaderQueueRows, UpdateQueueController, /*leading*/ true);
        updateControllers(EObjectKind::Consumer, leaderConsumerRows, UpdateConsumerController, /*leading*/ true);
    }

    auto ledQueues = getHashTable(leaderQueueRows);
    auto ledConsumers = getHashTable(leaderConsumerRows);

    THashMap<TCrossClusterReference, TQueueTableRow> followedQueues;
    THashMap<TCrossClusterReference, TConsumerTableRow> followedConsumers;

    // Then, collect follower objects from registrations.
    // NB: Follower objects can be from stages other than ours, since consumers from one stage can be registered for queues from another.

    // Checks that the given object is known to the QA and skips objects which are led by this QA.
    auto addFollowedObject = [] (const TCrossClusterReference& ref, auto& allObjects, auto& ledObjects, auto& followedObjects) {
        auto allObjectsIt = allObjects.find(ref);
        auto ledObjectsIt = ledObjects.find(ref);
        if (allObjectsIt != allObjects.end() && ledObjectsIt == ledObjects.end()) {
            followedObjects.emplace(ref, allObjectsIt->second);
        }
    };

    // Add follower controllers for consumers and queues registered to led queues and consumers respectively.
    for (const auto& registration : registrationRows) {
        if (ledQueues.contains(registration.Queue)) {
            addFollowedObject(registration.Consumer, allConsumers, ledConsumers, followedConsumers);
        }
        if (ledConsumers.contains(registration.Consumer)) {
            addFollowedObject(registration.Queue, allQueues, ledQueues, followedQueues);
        }
    }

    auto addFollowedReplicas = [&] (const std::vector<TCrossClusterReference>& queueRefs) {
        for (const auto& queue : queueRefs) {
            if (auto replicatedTableMappingRow = getReplicatedTableMappingRow(queue)) {
                for (const auto& queueReplica : replicatedTableMappingRow->GetReplicas()) {
                    addFollowedObject(TCrossClusterReference::FromRichYPath(queueReplica), allQueues, ledQueues, followedQueues);
                }
            }
        }
    };

    // Add follower controllers for replicas of all relevant queues.
    // NB: It is important that we do this *after* we add followed queues from registrations.
    // NB: We dont add replicas for all queues, since some of those queues are from stages other than ours.
    addFollowedReplicas(GetKeys(ledQueues));
    addFollowedReplicas(GetKeys(followedQueues));

    // Then, create following-controllers for objects referenced by queues and consumers this queue agent is responsible for.

    {
        auto guard = ReaderGuard(ObjectLock_);

        updateControllers(EObjectKind::Queue, GetValues(followedQueues), UpdateQueueController, /*leading*/ false);
        updateControllers(EObjectKind::Consumer, GetValues(followedConsumers), UpdateConsumerController, /*leading*/ false);
    }

    // Then, put fresh registrations into fresh objects (both leading and following).

    for (const auto& registration : registrationRows) {
        auto appendRegistration = [&] (TObjectMap& objectMap, const NQueueClient::TCrossClusterReference& objectRef) {
            if (auto it = objectMap.find(objectRef); it != objectMap.end()) {
                it->second.Registrations.push_back(registration);
            }
        };
        appendRegistration(freshObjects[EObjectKind::Queue], registration.Queue);
        appendRegistration(freshObjects[EObjectKind::Consumer], registration.Consumer);
    }

    // Then, replace old objects with fresh ones.

    {
        auto guard = WriterGuard(ObjectLock_);

        for (auto objectKind : {EObjectKind::Queue, EObjectKind::Consumer}) {
            Objects_[objectKind].swap(freshObjects[objectKind]);
        }

        LeadingObjectCount_[EObjectKind::Queue] = std::ssize(leaderQueueRows);
        LeadingObjectCount_[EObjectKind::Consumer] = std::ssize(leaderConsumerRows);

        ObjectToHost_.swap(objectMapping);
    }

    // Finally, update rows in the controllers. As best effort to prevent some inconsistencies (like enabling trimming
    // with obsolete list of vital registrations), we do that strictly after registration update.

    auto updateRows = [&] (EObjectKind objectKind, const auto& rows) {
        for (const auto& row : rows) {
            // Existence of a key in the map is guaranteed by updateControllers.
            const auto& object = GetOrCrash(Objects_[objectKind], row.Ref);
            object.Controller->OnRowUpdated(row);
            object.Controller->OnReplicatedTableMappingRowUpdated(getReplicatedTableMappingRow(row.Ref));
        }
    };

    {
        auto guard = ReaderGuard(ObjectLock_);

        updateRows(EObjectKind::Queue, leaderQueueRows);
        updateRows(EObjectKind::Consumer, leaderConsumerRows);

        updateRows(EObjectKind::Queue, GetValues(followedQueues));
        updateRows(EObjectKind::Consumer, GetValues(followedConsumers));
    }

    PassError_ = TError();
    Alerts_.clear();

    Profile();
}

void TQueueAgent::Profile()
{
    struct TTaggedCounters
    {
        int QueueCount;
        int ConsumerCount;
        int PartitionCount;
        int TrimmedQueueCount;
        int ErroneousQueueCount;
        int ErroneousConsumerCount;
    };

    THashMap<TProfilingTags, TTaggedCounters> tagsToCounters;

    auto getLeadingStatus = [&] (const IObjectControllerPtr& controller) {
        return controller->IsLeading() ? "leader" : "follower";
    };

    {
        auto guard = ReaderGuard(ObjectLock_);

        for (const auto& [queueRef, queue] : Objects_[EObjectKind::Queue]) {
            const auto& snapshot = DynamicPointerCast<TQueueSnapshot>(queue.Controller->GetLatestSnapshot());

            TProfilingTags profilingTags = {
                .Cluster = queueRef.Cluster,
                .LeadingStatus = getLeadingStatus(queue.Controller),
                .QueueAgentStage = snapshot->Row.QueueAgentStage.value_or(NoneQueueAgentStage),
                .ObjectType = snapshot->Row.ObjectType ? ToString(*snapshot->Row.ObjectType) : NoneObjectType,
            };

            auto& taggedCounters = tagsToCounters[profilingTags];
            ++taggedCounters.QueueCount;
            taggedCounters.PartitionCount += snapshot->PartitionCount;

            const auto& autoTrimConfig = snapshot->Row.AutoTrimConfig;
            if (autoTrimConfig && autoTrimConfig->Enable) {
                ++taggedCounters.TrimmedQueueCount;
            }

            if (!snapshot->Error.IsOK()) {
                ++taggedCounters.ErroneousQueueCount;
            }
        }
        for (const auto& [consumerRef, consumer] : Objects_[EObjectKind::Consumer]) {
            const auto& snapshot = DynamicPointerCast<TConsumerSnapshot>(consumer.Controller->GetLatestSnapshot());

            TProfilingTags profilingTags = {
                .Cluster = consumerRef.Cluster,
                .LeadingStatus = getLeadingStatus(consumer.Controller),
                .QueueAgentStage = snapshot->Row.QueueAgentStage.value_or(NoneQueueAgentStage),
                .ObjectType = snapshot->Row.ObjectType ? ToString(*snapshot->Row.ObjectType) : NoneObjectType,
            };

            auto& taggedCounters = tagsToCounters[profilingTags];
            ++taggedCounters.ConsumerCount;
            for (const auto& [_, subConsumer] : snapshot->SubSnapshots) {
                taggedCounters.PartitionCount += subConsumer->PartitionCount;
            }

            if (!snapshot->Error.IsOK()) {
                ++taggedCounters.ErroneousConsumerCount;
            }
        }
    }

    for (const auto& [tag, taggedCounters] : tagsToCounters) {
        auto& profilingCounters = GetOrCreateTaggedProfilingCounters(tag);
        profilingCounters.Queues.Update(taggedCounters.QueueCount);
        profilingCounters.Consumers.Update(taggedCounters.ConsumerCount);
        profilingCounters.Partitions.Update(taggedCounters.PartitionCount);
        profilingCounters.TrimmedQueues.Update(taggedCounters.TrimmedQueueCount);
        profilingCounters.ErroneousQueues.Update(taggedCounters.ErroneousQueueCount);
        profilingCounters.ErroneousConsumers.Update(taggedCounters.ErroneousConsumerCount);
    }
}

NYTree::IYPathServicePtr TQueueAgent::RedirectYPathRequest(const TString& host, TStringBuf queryRoot, TStringBuf key) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Redirecting orchid request (QueueAgentHost: %v, QueryRoot: %v, Key: %v)", host, queryRoot, key);
    auto leaderChannel = QueueAgentChannelFactory_->CreateChannel(host);
    auto remoteRoot = Format("%v/%v", queryRoot, ToYPathLiteral(key));
    return CreateOrchidYPathService({
        .Channel = std::move(leaderChannel),
        .RemoteRoot = std::move(remoteRoot),
    });
}

TTaggedProfilingCounters& TQueueAgent::GetOrCreateTaggedProfilingCounters(const TProfilingTags& profilingTags)
{
    auto it = TaggedProfilingCounters_.find(profilingTags);
    if (it == TaggedProfilingCounters_.end()) {
        auto profilingCounters =
            TTaggedProfilingCounters(QueueAgentProfiler
                .WithTag("yt_cluster", profilingTags.Cluster)
                .WithTag("leading_status", profilingTags.LeadingStatus)
                .WithTag("queue_agent_stage", profilingTags.QueueAgentStage)
                .WithTag("object_type", profilingTags.ObjectType));
        it = TaggedProfilingCounters_.insert({profilingTags, profilingCounters}).first;
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
