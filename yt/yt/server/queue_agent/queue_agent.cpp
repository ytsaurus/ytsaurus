#include "queue_agent.h"

#include "config.h"
#include "helpers.h"
#include "queue_controller.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>

namespace NYT::NQueueAgent {

using namespace NYTree;
using namespace NObjectClient;
using namespace NOrchid;
using namespace NApi;
using namespace NConcurrency;
using namespace NYson;
using namespace NHydra;
using namespace NTracing;
using namespace NHiveClient;
using namespace NCypressElection;
using namespace NYPath;
using namespace NRpc::NBus;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueueAgentLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

void BuildErrorYson(TError error, TFluentMap fluent)
{
    fluent
        .Item("status").BeginMap()
            .Item("error").Value(error)
        .EndMap()
        .Item("pass_index").Value(0)
        .Item("partitions").BeginList().EndList();
}

template <class TValue>
class TCollectionBoundService
    : public TVirtualMapBase
{
public:
    using TCollection = THashMap<TCrossClusterReference, TValue>;
    //! Produces the orchid for the given object reference and object into the consumer.
    using TProducerCallback = TCallback<void(const TCrossClusterReference&, const TValue&, IYsonConsumer*)>;
    //! Returns a remote service to resolve the given key, or nullptr to resolve the key on this instance.
    using TRedirectorCallback = TCallback<IYPathServicePtr(TStringBuf)>;

    explicit TCollectionBoundService(
        const TCollection& collection,
        TProducerCallback producer,
        TRedirectorCallback redirector,
        IInvokerPtr invoker)
        : Collection_(collection)
        , Producer_(std::move(producer))
        , Redirector_(std::move(redirector))
        , Invoker_(std::move(invoker))
    {
        SetOpaque(false);
    }

    i64 GetSize() const override
    {
        return std::ssize(Collection_);
    }

    std::vector<TString> GetKeys(i64 limit) const override
    {
        std::vector<TString> keys;
        keys.reserve(std::min(std::ssize(Collection_), limit));
        for (const auto& [key, value] : Collection_) {
            keys.push_back(ToString(key));
            if (std::ssize(keys) == limit) {
                break;
            }
        }
        return keys;
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        if (auto remoteYPathService = Redirector_.Run(key)) {
            return remoteYPathService;
        }
        auto objectRef = TCrossClusterReference::FromString(key);
        auto buildQueueYsonCallback = BIND(&TCollectionBoundService::BuildYson, MakeStrong(this), objectRef);
        return IYPathService::FromProducer(buildQueueYsonCallback)->Via(Invoker_);
    }

private:
    // Neither the queue agent nor the ypath services are supposed to be destroyed, so references are fine.
    const TCollection& Collection_;
    TProducerCallback Producer_;
    TRedirectorCallback Redirector_;
    IInvokerPtr Invoker_;

    void BuildYson(const TCrossClusterReference& objectRef, IYsonConsumer* ysonConsumer) const
    {
        auto it = Collection_.find(objectRef);
        if (it == Collection_.end()) {
            THROW_ERROR_EXCEPTION("Object %v is missing", objectRef);
        }
        // NB: This object should not be used after context switches.
        const auto& object = it->second;
        Producer_.Run(objectRef, object, ysonConsumer);
    }
};

} // namespace

TQueueAgent::TQueueAgent(
    TQueueAgentConfigPtr config,
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
    , ControllerThreadPool_(New<TThreadPool>(DynamicConfig_->ControllerThreadCount, "Controller"))
    , PollExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TQueueAgent::Poll, MakeWeak(this)),
        DynamicConfig_->PollPeriod))
    , AgentId_(std::move(agentId))
    , QueueAgentChannelFactory_(
        CreateCachingChannelFactory(CreateBusChannelFactory(Config_->BusClient)))
{
    QueueObjectServiceNode_ = CreateVirtualNode(
        New<TCollectionBoundService<TQueue>>(
            Queues_,
            BIND(&TQueueAgent::BuildQueueYson, MakeStrong(this)),
            BIND(&TQueueAgent::RedirectYPathRequestToLeader, MakeStrong(this), "//queue_agent/queues"),
            ControlInvoker_));
    ConsumerObjectServiceNode_ = CreateVirtualNode(
        New<TCollectionBoundService<TConsumer>>(
            Consumers_,
            BIND(&TQueueAgent::BuildConsumerYson, MakeStrong(this)),
            BIND(&TQueueAgent::RedirectYPathRequestToLeader, MakeStrong(this), "//queue_agent/consumers"),
            ControlInvoker_));
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

    // Returning without waiting here is a bit racy in case of a conurrent Start(),
    // but it does not seem like a big issue.
    PollExecutor_->Stop();

    YT_LOG_INFO("Resetting all controllers");

    std::vector<TFuture<void>> stopFutures;
    for (const auto& [_, queue]: Queues_) {
        if (queue.Controller) {
            stopFutures.emplace_back(queue.Controller->Stop());
        }
    }
    YT_VERIFY(WaitFor(AllSucceeded(stopFutures)).IsOK());

    YT_LOG_INFO("Clearing state");

    Queues_.clear();
    Consumers_.clear();

    YT_LOG_INFO("Queue agent stopped");
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
    node->AddChild("queues", QueueObjectServiceNode_);
    node->AddChild("consumers", ConsumerObjectServiceNode_);

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

    std::vector<TFuture<void>> asyncQueueControllerUpdates;
    for (const auto& [queueRef, queue] : Queues_) {
        asyncQueueControllerUpdates.push_back(
            BIND(
                &IQueueController::OnDynamicConfigChanged,
                queue.Controller,
                oldConfig->Controller,
                newConfig->Controller)
                .AsyncVia(queue.Controller->GetInvoker())
                .Run());
    }
    WaitFor(AllSucceeded(asyncQueueControllerUpdates))
        .ThrowOnError();

    YT_LOG_DEBUG(
        "Updated queue agent dynamic config (OldConfig: %v, NewConfig: %v)",
        ConvertToYsonString(oldConfig, EYsonFormat::Text),
        ConvertToYsonString(newConfig, EYsonFormat::Text));
}

void TQueueAgent::Poll()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    PollInstant_ = TInstant::Now();
    ++PollIndex_;

    auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("QueueAgent"));

    const auto& Logger = QueueAgentLogger;

    // Collect queue and consumer rows.

    YT_LOG_INFO("State poll started (PollIndex: %v)", PollIndex_);
    auto logFinally = Finally([&] {
        YT_LOG_INFO("State poll finished (PollIndex: %v)", PollIndex_);
    });

    auto asyncQueueRows = DynamicState_->Queues->Select();
    auto asyncConsumerRows = DynamicState_->Consumers->Select();

    std::vector<TFuture<void>> futures{asyncQueueRows.AsVoid(), asyncConsumerRows.AsVoid()};

    if (auto error = WaitFor(AllSucceeded(futures)); !error.IsOK()) {
        PollError_ = error;
        YT_LOG_ERROR(error, "Error polling queue state");
        return;
    }
    const auto& queueRows = asyncQueueRows.Get().Value();
    const auto& consumerRows = asyncConsumerRows.Get().Value();

    YT_LOG_INFO(
        "State table rows collected (QueueRowCount: %v, ConsumerRowCount: %v)",
        queueRows.size(),
        consumerRows.size());

    // Prepare ref -> row mappings for queues and rows.

    THashMap<TCrossClusterReference, TQueueTableRow> queueRefToRow;
    THashMap<TCrossClusterReference, TConsumerTableRow> consumerRefToRow;

    for (const auto& queueRow : queueRows) {
        queueRefToRow[queueRow.Queue] = queueRow;
    }
    for (const auto& consumerRow : consumerRows) {
        consumerRefToRow[consumerRow.Consumer] = consumerRow;
    }

    // Prepare fresh queue objects and fresh consumer objects.

    TQueueMap freshQueues;
    TConsumerMap freshConsumers;

    for (const auto& row : queueRows) {
        YT_LOG_TRACE("Processing queue row (Row: %v)", ConvertToYsonString(row, EYsonFormat::Text).ToString());

        auto& freshQueue = freshQueues[row.Queue];

        auto logFinally = Finally([&] {
            YT_LOG_TRACE(
                "Fresh queue prepared (Queue: %Qv, Error: %v, RowRevision: %v, QueueFamily: %v)",
                row.Queue,
                freshQueue.Error,
                freshQueue.RowRevision,
                freshQueue.QueueFamily);
        });

        if (!row.RowRevision || !row.ObjectType) {
            freshQueue.Error = TError("Queue is not in-sync yet");
            continue;
        }

        freshQueue.RowRevision = *row.RowRevision;

        if (auto queueFamilyOrError = DeduceQueueFamily(row); queueFamilyOrError.IsOK()) {
            freshQueue.QueueFamily = queueFamilyOrError.Value();
        } else {
            freshQueue.Error = static_cast<TError>(queueFamilyOrError);
        }
    }

    for (const auto& row : consumerRows) {
        YT_LOG_TRACE("Processing consumer row (Row: %v)", ConvertToYsonString(row, EYsonFormat::Text).ToString());

        auto& freshConsumer = freshConsumers[row.Consumer];

        auto logFinally = Finally([&] {
            YT_LOG_TRACE(
                "Fresh consumer prepared (Consumer: %Qv, Error: %v, RowRevision: %v, Target: %Qv)",
                row.Consumer,
                freshConsumer.Error,
                freshConsumer.RowRevision,
                freshConsumer.Target);
        });

        if (!row.RowRevision) {
            freshConsumer.Error = TError("Consumer is not in-sync yet");
            continue;
        }

        freshConsumer.RowRevision = *row.RowRevision;

        if (!row.TargetQueue) {
            freshConsumer.Error = TError("Consumer is missing target");
            continue;
        }

        freshConsumer.Target = row.TargetQueue;

        auto it = freshQueues.find(*row.TargetQueue);
        if (it == freshQueues.end()) {
            freshConsumer.Error = TError("Target queue %Qv is not registered", *row.TargetQueue);
            continue;
        }

        auto& freshTargetQueue = it->second;
        freshTargetQueue.ConsumerRowRevisions[row.Consumer] = *row.RowRevision;
    }

    // Replace old consumers with fresh consumers.

    Consumers_.swap(freshConsumers);

    // Now carefully replace old queues with new queues if row revision of at least queue itself
    // or one of its consumers has changed.

    for (auto& [queueRef, freshQueue] : freshQueues) {
        auto oldIt = Queues_.find(queueRef);
        if (oldIt == Queues_.end()) {
            // This is a newly registered queue.
            YT_LOG_INFO("Queue registered (Queue: %Qv)", queueRef);
            continue;
        }

        auto& oldQueue = oldIt->second;

        if (!freshQueue.Error.IsOK()) {
            YT_LOG_TRACE("Queue has non-trivial error; resetting queue controller");
            oldQueue.Reset();
        } else if (
            oldQueue.RowRevision == freshQueue.RowRevision &&
            oldQueue.ConsumerRowRevisions == freshQueue.ConsumerRowRevisions)
        {
            // We may use the controller of the old queue.
            YT_LOG_TRACE("Queue row revisions remain the same; re-using its controller (Queue: %Qv)", queueRef);
            freshQueue.Controller = std::move(oldQueue.Controller);
        } else {
            // Old queue controller must be reset.
            YT_LOG_DEBUG(
                "Queue row revisions changed (Queue: %Qv, RowRevision: %v -> %v, ConsumerRowRevisions: %v -> %v)",
                queueRef,
                oldQueue.RowRevision,
                freshQueue.RowRevision,
                oldQueue.ConsumerRowRevisions,
                freshQueue.ConsumerRowRevisions);
            oldQueue.Reset();
        }

        Queues_.erase(oldIt);
    }

    // Remaining items in #Queues_ are the queues to be unregistered.

    for (auto& [queueRef, oldQueue] : Queues_) {
        YT_LOG_INFO("Queue unregistered (Queue: %Qv)", queueRef);
        oldQueue.Reset();
    }

    // Replace old queues with fresh ones.

    Queues_ = std::move(freshQueues);

    // Finally, create controllers for queues without errors (either for fresh ones, or for old ones that
    // were reset due to row revision promotion).

    for (auto& [queueRef, queue] : Queues_) {
        if (queue.Error.IsOK() && !queue.Controller) {
            TConsumerRowMap consumerRowMap;
            for (const auto& consumerRef : GetKeys(queue.ConsumerRowRevisions)) {
                consumerRowMap[consumerRef] = GetOrCrash(consumerRefToRow, consumerRef);
            }

            queue.Controller = CreateQueueController(
                DynamicConfig_->Controller,
                ClientDirectory_,
                queueRef,
                queue.QueueFamily,
                GetOrCrash(queueRefToRow, queueRef),
                std::move(consumerRowMap),
                CreateSerializedInvoker(ControllerThreadPool_->GetInvoker()));
            queue.Controller->Start();
        }
    }

    PollError_ = TError();
}

NYTree::IYPathServicePtr TQueueAgent::RedirectYPathRequestToLeader(TStringBuf queryRoot, TStringBuf key)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    if (Active_) {
        return nullptr;
    }

    auto leaderTransactionAttributes = ElectionManager_->GetCachedLeaderTransactionAttributes();
    if (!leaderTransactionAttributes) {
        THROW_ERROR_EXCEPTION(
            "Unable to fetch %v, instance is not active and leader information is not available",
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

void TQueueAgent::BuildQueueYson(const TCrossClusterReference& /*queueRef*/, const TQueue& queue, IYsonConsumer* ysonConsumer)
{
    BuildYsonFluently(ysonConsumer).BeginMap()
        .Item("row_revision").Value(queue.RowRevision)
        .Do([&] (TFluentMap fluent) {
            if (!queue.Error.IsOK()) {
                BuildErrorYson(queue.Error, fluent);
                return;
            }

            YT_VERIFY(queue.Controller);

            auto error = WaitFor(
                BIND(&IQueueController::BuildOrchid, queue.Controller, fluent)
                    .AsyncVia(queue.Controller->GetInvoker())
                    .Run());
            YT_VERIFY(error.IsOK());
        })
        .EndMap();
}

void TQueueAgent::BuildConsumerYson(const TCrossClusterReference& consumerRef, const TConsumer& consumer, IYsonConsumer* ysonConsumer)
{
    BuildYsonFluently(ysonConsumer).BeginMap()
        .Item("row_revision").Value(consumer.RowRevision)
        .Do([&] (TFluentMap fluent) {
            if (!consumer.Error.IsOK()) {
                BuildErrorYson(consumer.Error, fluent);
                return;
            }
            YT_VERIFY(consumer.Target);
            auto queue = GetOrCrash(Queues_, *consumer.Target);
            if (!queue.Error.IsOK()) {
                BuildErrorYson(TError("Target queue %Qv is in error state", consumer.Target) << queue.Error, fluent);
                return;
            }
            YT_VERIFY(queue.Controller);

            auto error = WaitFor(
                BIND(&IQueueController::BuildConsumerOrchid, queue.Controller, consumerRef, fluent)
                    .AsyncVia(queue.Controller->GetInvoker())
                    .Run());
            YT_VERIFY(error.IsOK());
        })
        .EndMap();
}

void TQueueAgent::TQueue::Reset()
{
    if (Controller) {
        YT_VERIFY(WaitFor(Controller->Stop()).IsOK());
        Controller = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
