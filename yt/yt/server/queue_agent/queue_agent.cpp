#include "queue_agent.h"

#include "config.h"
#include "helpers.h"
#include "queue_controller.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueAgent {

using namespace NYTree;
using namespace NObjectClient;
using namespace NApi;
using namespace NConcurrency;
using namespace NYson;
using namespace NHydra;
using namespace NTracing;
using namespace NHiveClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueueAgentLogger;

TQueueAgent::TQueueAgent(
    TQueueAgentConfigPtr config,
    TClusterDirectoryPtr clusterDirectory,
    IInvokerPtr controlInvoker,
    TDynamicStatePtr dynamicState)
    : OrchidService_(IYPathService::FromProducer(BIND(&TQueueAgent::BuildOrchid, MakeWeak(this)))->Via(controlInvoker))
    , Config_(std::move(config))
    , ClusterDirectory_(std::move(clusterDirectory))
    , ControlInvoker_(std::move(controlInvoker))
    , DynamicState_(std::move(dynamicState))
    , ControllerThreadPool_(New<TThreadPool>(Config_->ControllerThreadCount, "Controller"))
    , PollExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TQueueAgent::Poll, MakeWeak(this)),
        Config_->PollPeriod))
{ }

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

    YT_VERIFY(WaitFor(BIND(&TQueueAgent::DoStop, MakeWeak(this))
        .AsyncVia(ControlInvoker_)
        .Run()).IsOK());

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

void TQueueAgent::BuildOrchid(IYsonConsumer* consumer) const
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    // NB: without taking copy we may end up with invalidated iterators due to yielding.
    auto queuesCopy = Queues_;
    auto consumersCopy = Consumers_;

    auto buildErrorYson = [&] (TError error, TFluentMap fluent) {
        fluent
            .Item("status").BeginMap()
                .Item("error").Value(error)
            .EndMap()
            .Item("partitions").BeginList().EndList();
    };

    BuildYsonFluently(consumer).BeginMap()
        .Item("active").Value(Active_.load())
        .Item("queues").DoMapFor(queuesCopy, [&] (TFluentMap fluent, auto pair) {
            auto queueRef = pair.first;
            auto queue = pair.second;
            fluent
                .Item(ToString(queueRef)).BeginMap()
                    .Item("row_revision").Value(queue.RowRevision)
                    .Do([&] (TFluentMap fluent) {
                        if (!queue.Error.IsOK()) {
                            buildErrorYson(queue.Error, fluent);
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
        })
        .Item("consumers").DoMapFor(consumersCopy, [&] (TFluentMap fluent, auto pair) {
            auto consumerRef = pair.first;
            auto consumer = pair.second;

            fluent
                .Item(ToString(consumerRef)).BeginMap()
                    .Item("row_revision").Value(consumer.RowRevision)
                    .Do([&] (TFluentMap fluent) {
                        if (!consumer.Error.IsOK()) {
                            buildErrorYson(consumer.Error, fluent);
                            return;
                        }
                        YT_VERIFY(consumer.Target);
                        auto queue = GetOrCrash(queuesCopy, *consumer.Target);
                        YT_VERIFY(queue.Controller);

                        auto error = WaitFor(
                            BIND(&IQueueController::BuildConsumerOrchid, queue.Controller, consumerRef, fluent)
                                .AsyncVia(queue.Controller->GetInvoker())
                                .Run());
                        YT_VERIFY(error.IsOK());
                    })
                .EndMap();
        })
        .Item("poll_instant").Value(PollInstant_)
        .Item("poll_index").Value(PollIndex_)
        .Item("latest_poll_error").Value(LatestPollError_)
    .EndMap();
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
        LatestPollError_ = error
            << TErrorAttribute("poll_index", PollIndex_);
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

        if (!row.Target) {
            freshConsumer.Error = TError("Consumer is missing target");
            continue;
        }

        freshConsumer.Target = row.Target;

        auto it = freshQueues.find(*row.Target);
        if (it == freshQueues.end()) {
            freshConsumer.Error = TError("Target queue %Qv is not registered", *row.Target);
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

        if (oldQueue.RowRevision == freshQueue.RowRevision &&
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
                Config_->Controller,
                ClusterDirectory_,
                queueRef,
                queue.QueueFamily,
                GetOrCrash(queueRefToRow, queueRef),
                std::move(consumerRowMap),
                CreateSerializedInvoker(ControllerThreadPool_->GetInvoker()));
            queue.Controller->Start();
        }
    }
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
