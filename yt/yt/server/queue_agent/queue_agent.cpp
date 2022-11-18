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

#include <yt/yt/ytlib/auth/native_authenticating_channel.h>

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
using namespace NApi;
using namespace NConcurrency;
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

void BuildErrorYson(TError error, TFluentMap fluent)
{
    fluent
        .Item("status").BeginMap()
            .Item("error").Value(error)
        .EndMap()
        .Item("pass_index").Value(0)
        .Item("partitions").BeginList().EndList();
}

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

        const auto& objectMap = Owner_->Objects_[ObjectKind_];

        return std::ssize(objectMap);
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        auto guard = ReaderGuard(Owner_->ObjectLock_);

        const auto& objectMap = Owner_->Objects_[ObjectKind_];

        std::vector<TString> keys;
        keys.reserve(std::min(std::ssize(objectMap), limit));
        for (const auto& [key, _] : objectMap) {
            keys.push_back(ToString(key));
            if (std::ssize(keys) == limit) {
                break;
            }
        }
        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        if (auto remoteYPathService = Owner_->RedirectYPathRequestToLeader(QueryRoot_, key)) {
            return remoteYPathService;
        }

        {
            auto guard = ReaderGuard(Owner_->ObjectLock_);

            auto objectRef = TCrossClusterReference::FromString(key);

            const auto& objectMap = Owner_->Objects_[ObjectKind_];

            auto it = objectMap.find(objectRef);
            if (it == objectMap.end()) {
                THROW_ERROR_EXCEPTION("Object %Qv is missing", objectRef);
            }

            return IYPathService::FromProducer(BIND(&IObjectController::BuildOrchid, it->second.Controller));
        }
    }

private:
    // The queue agent is not supposed to be destroyed, so raw pointer is fine.
    const TQueueAgent* Owner_;
    EObjectKind ObjectKind_;
    TString QueryRoot_;
};

TClusterProfilingCounters::TClusterProfilingCounters(TProfiler profiler)
    : Queues(profiler.Gauge("/queues"))
    , Consumers(profiler.Gauge("/consumers"))
    , Partitions(profiler.Gauge("/partitions"))
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
    , ControlInvoker_(std::move(controlInvoker))
    , DynamicState_(std::move(dynamicState))
    , ElectionManager_(std::move(electionManager))
    , ControllerThreadPool_(CreateThreadPool(DynamicConfig_->ControllerThreadCount, "Controller"))
    , PollExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TQueueAgent::Poll, MakeWeak(this)),
        DynamicConfig_->PollPeriod))
    , AgentId_(std::move(agentId))
    , GlobalProfilingCounters_(QueueAgentProfiler)
    , QueueAgentChannelFactory_(
        NAuth::CreateNativeAuthenticationInjectingChannelFactory(
            CreateCachingChannelFactory(CreateBusChannelFactory(Config_->BusClient)),
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

    Active_ = true;

    PollExecutor_->Start();
}

void TQueueAgent::Stop()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Stopping queue agent");

    ControlInvoker_->Invoke(BIND(&TQueueAgent::DoStop, MakeWeak(this)));

    Active_ = false;
}

void TQueueAgent::DoStop()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Stopping polling");

    // Returning without waiting here is a bit racy in case of a concurrent Start(),
    // but it does not seem like a big issue.
    PollExecutor_->Stop();

    YT_LOG_INFO("Clearing state");

    {
        auto guard = WriterGuard(ObjectLock_);

        Objects_[EObjectKind::Queue].clear();
        Objects_[EObjectKind::Consumer].clear();
    }

    YT_LOG_INFO("Queue agent stopped");
}

void TQueueAgent::PopulateAlerts(std::vector<TError>* alerts) const
{
    WaitFor(
        BIND(&TQueueAgent::DoPopulateAlerts, MakeStrong(this), alerts)
        .AsyncVia(ControlInvoker_)
        .Run())
        .ThrowOnError();
}

void TQueueAgent::DoPopulateAlerts(std::vector<TError>* alerts) const
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    alerts->insert(alerts->end(), Alerts_.begin(), Alerts_.end());
}

IMapNodePtr TQueueAgent::GetOrchidNode() const
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_DEBUG("Executing orchid request (PollIndex: %v)", PollIndex_ - 1);

    auto virtualScalarNode = [] (auto callable) {
        return CreateVirtualNode(IYPathService::FromProducer(BIND([callable] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer).Value(callable());
        })));
    };

    auto node = GetEphemeralNodeFactory()->CreateMap();
    node->AddChild("active", virtualScalarNode([&] { return Active_.load(); }));
    node->AddChild("poll_instant", virtualScalarNode([&] { return PollInstant_; }));
    node->AddChild("poll_index", virtualScalarNode([&] { return PollIndex_; }));
    node->AddChild("poll_error", virtualScalarNode([&] { return PollError_; }));
    node->AddChild("queues", ObjectServiceNodes_[EObjectKind::Queue]);
    node->AddChild("consumers", ObjectServiceNodes_[EObjectKind::Consumer]);

    return node;
}

void TQueueAgent::OnDynamicConfigChanged(
    const TQueueAgentDynamicConfigPtr& oldConfig,
    const TQueueAgentDynamicConfigPtr& newConfig)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    // NB: We do this in the beginning, so that we use the new config if a context switch happens below.
    DynamicConfig_ = newConfig;

    PollExecutor_->SetPeriod(newConfig->PollPeriod);

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

void TQueueAgent::Poll()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    PollInstant_ = TInstant::Now();
    ++PollIndex_;

    auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("QueueAgent"));

    auto Logger = QueueAgentLogger.WithTag("PollIndex: %v", PollIndex_);

    // Collect queue and consumer rows.

    YT_LOG_INFO("State poll started");
    auto logFinally = Finally([&] {
        YT_LOG_INFO("State poll finished");
    });

    auto where = Format("[queue_agent_stage] = \"%v\"", Config_->Stage);
    auto asyncQueueRows = DynamicState_->Queues->Select("*", where);
    auto asyncConsumerRows = DynamicState_->Consumers->Select("*", where);
    auto asyncRegistrationRows = DynamicState_->Registrations->Select();

    std::vector<TFuture<void>> futures{
        asyncQueueRows.AsVoid(),
        asyncConsumerRows.AsVoid(),
        asyncRegistrationRows.AsVoid(),
    };

    if (auto error = WaitFor(AllSucceeded(futures)); !error.IsOK()) {
        PollError_ = error;
        YT_LOG_ERROR(error, "Error polling queue state");
        auto alert = TError(
            NAlerts::EErrorCode::QueueAgentPassFailed,
            "Error polling queue state")
            << error;
        Alerts_ = {alert};
        return;
    }
    const auto& queueRows = asyncQueueRows.Get().Value();
    const auto& consumerRows = asyncConsumerRows.Get().Value();
    const auto& registrationRows = asyncRegistrationRows.Get().Value();

    YT_LOG_INFO(
        "State table rows collected (QueueRowCount: %v, ConsumerRowCount: %v, RegistrationRowCount: %v)",
        queueRows.size(),
        consumerRows.size(),
        registrationRows.size());

    TEnumIndexedVector<EObjectKind, TObjectMap> freshObjects;

    // First, prepare fresh queue and consumer controllers.

    auto updateControllers = [&] (EObjectKind objectKind, const auto& rows, auto updateController) {
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
                row,
                /*store*/ this,
                DynamicConfig_->Controller,
                ClientDirectory_,
                ControllerThreadPool_->GetInvoker());

            YT_LOG_DEBUG(
                "Controller updated (Kind: %v, Object: %Qv, Reused: %v, Recreated: %v)",
                objectKind,
                row.Ref,
                reused,
                recreated);
        }
    };

    {
        auto guard = ReaderGuard(ObjectLock_);

        updateControllers(EObjectKind::Queue, queueRows, UpdateQueueController);
        updateControllers(EObjectKind::Consumer, consumerRows, UpdateConsumerController);
    }

    // Then, put fresh registrations into fresh objects.

    for (const auto& registration : registrationRows) {
        auto appendRegistration = [&] (TObjectMap& objectMap, NQueueClient::TCrossClusterReference objectRef) {
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
    }

    // Finally, update rows in the controllers. As best effort to prevent some inconsistencies (like enabling trimming
    // with obsolete list of vital registrations), we do that strictly after registration update.

    auto updateRows = [&] (EObjectKind objectKind, const auto& rows) {
        for (const auto& row : rows) {
            // Existence of a key in the map is guaranteed by updateControllers.
            const auto& object = GetOrCrash(Objects_[objectKind], row.Ref);
            object.Controller->OnRowUpdated(row);
        }
    };

    {
        auto guard = ReaderGuard(ObjectLock_);

        updateRows(EObjectKind::Queue, queueRows);
        updateRows(EObjectKind::Consumer, consumerRows);
    }

    PollError_ = TError();
    Alerts_.clear();

    Profile();
}

void TQueueAgent::Profile()
{
    struct TClusterCounters {
        int QueueCount;
        int ConsumerCount;
        int PartitionCount;
    };

    THashMap<TString, TClusterCounters> clusterToCounters;

    {
        auto guard = ReaderGuard(ObjectLock_);

        for (const auto& [queueRef, queue] : Objects_[EObjectKind::Queue]) {
            auto& clusterCounters = clusterToCounters[queueRef.Cluster];
            ++clusterCounters.QueueCount;
            const auto& snapshot = DynamicPointerCast<TQueueSnapshot>(queue.Controller->GetLatestSnapshot());
            clusterCounters.PartitionCount += snapshot->PartitionCount;
        }
        for (const auto& [consumerRef, consumer] : Objects_[EObjectKind::Consumer]) {
            auto& clusterCounters = clusterToCounters[consumerRef.Cluster];
            ++clusterCounters.ConsumerCount;
            const auto& snapshot = DynamicPointerCast<TConsumerSnapshot>(consumer.Controller->GetLatestSnapshot());
            for (const auto& [_, subConsumer] : snapshot->SubSnapshots) {
                clusterCounters.PartitionCount += subConsumer->PartitionCount;
            }
        }
    }

    for (const auto& [cluster, clusterCounters] : clusterToCounters) {
        auto& profilingCounters = GetOrCreateClusterProfilingCounters(cluster);
        profilingCounters.Queues.Update(clusterCounters.QueueCount);
        profilingCounters.Consumers.Update(clusterCounters.ConsumerCount);
        profilingCounters.Partitions.Update(clusterCounters.PartitionCount);
    }
}

NYTree::IYPathServicePtr TQueueAgent::RedirectYPathRequestToLeader(TStringBuf queryRoot, TStringBuf key) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (Active_) {
        return nullptr;
    }

    auto leaderTransactionAttributes = ElectionManager_->GetCachedLeaderTransactionAttributes();
    if (!leaderTransactionAttributes) {
        THROW_ERROR_EXCEPTION(
            "Unable to fetch %Qv, instance is not active and leader information is not available",
            key);
    }

    auto host = leaderTransactionAttributes->Get<TString>("host");
    if (host == AgentId_) {
        THROW_ERROR_EXCEPTION(
            "Unable to fetch %v, instance is not active and leader information is stale",
            key);
    }

    YT_LOG_DEBUG("Redirecting orchid request (LeaderHost: %v, QueryRoot: %v, Key: %v)", host, queryRoot, key);
    auto leaderChannel = QueueAgentChannelFactory_->CreateChannel(host);
    auto remoteRoot = Format("%v/%v", queryRoot, ToYPathLiteral(key));
    return CreateOrchidYPathService({
        .Channel = std::move(leaderChannel),
        .RemoteRoot = std::move(remoteRoot),
    });
}

TClusterProfilingCounters& TQueueAgent::GetOrCreateClusterProfilingCounters(TString cluster)
{
    auto it = ClusterProfilingCounters_.find(cluster);
    if (it == ClusterProfilingCounters_.end()) {
        it = ClusterProfilingCounters_.insert(
            {cluster, TClusterProfilingCounters(QueueAgentProfiler.WithTag("yt_cluster", cluster))}).first;
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
