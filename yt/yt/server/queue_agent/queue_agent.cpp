#include "queue_agent.h"

#include "config.h"
#include "helpers.h"
#include "queue_controller.h"

#include <yt/yt/ytlib/api/native/client.h>

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
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueueAgentLogger;

TQueueAgent::TQueueAgent(
    TQueueAgentConfigPtr config,
    IInvokerPtr controlInvoker,
    NApi::NNative::IClientPtr client,
    TAgentId agentId)
    : OrchidService_(IYPathService::FromProducer(BIND(&TQueueAgent::BuildOrchid, MakeWeak(this)))->Via(controlInvoker))
    , Config_(std::move(config))
    , ControlInvoker_(std::move(controlInvoker))
    , ControllerThreadPool_(New<TThreadPool>(Config_->ControllerThreadCount, "Controller"))
    , Client_(std::move(client))
    , AgentId_(std::move(agentId))
    , PollExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TQueueAgent::Poll, MakeWeak(this)),
        Config_->PollPeriod))
    , QueueTable_(New<TQueueTable>(Config_->Root, Client_))
{
    UpdateOrchidNode();
}

void TQueueAgent::Start()
{
    PollExecutor_->Start();
}

void TQueueAgent::BuildOrchid(IYsonConsumer* consumer) const
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    BuildYsonFluently(consumer).BeginMap()
        .Item("queues").DoMapFor(Queues_, [&] (TFluentMap fluent, auto pair) {
            auto queueId = pair.first;
            auto queue = pair.second;
            fluent
                .Item(ToString(queueId)).BeginMap()
                    .Do([&] (TFluentMap fluent) {
                        if (queue.Error.IsOK()) {
                            YT_VERIFY(queue.Controller);
                            YT_VERIFY(
                                WaitFor(
                                    BIND(&IQueueController::BuildOrchid, queue.Controller, fluent)
                                        .AsyncVia(queue.Controller->GetInvoker())
                                        .Run())
                                .IsOK());
                        } else {
                            fluent
                                .Item("error").Value(queue.Error);
                        }
                    })
                    .Item("revision").Value(queue.Revision)
                .EndMap();
        })
        .Item("poll_instant").Value(PollInstant_)
        .Item("poll_index").Value(PollIndex_)
        .Item("latest_poll_error").Value(LatestPollError_)
    .EndMap();
}

void TQueueAgent::UpdateOrchidNode()
{
    TCreateNodeOptions options;
    options.Force = true;
    options.Recursive = true;
    options.Attributes = ConvertToAttributes(
        BuildYsonStringFluently().BeginMap()
            .Item("remote_addresses").BeginMap()
                .Item("default").Value(AgentId_)
            .EndMap()
        .EndMap());

    auto orchidPath = Format("%v/instances/%v/orchid", Config_->Root, ToYPathLiteral(AgentId_));

    YT_LOG_INFO("Updating orchid node (Path: %Qv)", orchidPath);

    WaitFor(Client_->CreateNode(orchidPath, EObjectType::Orchid, options))
        .ThrowOnError();

    YT_LOG_INFO("Orchid node updated");
}

void TQueueAgent::Poll()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    PollInstant_ = TInstant::Now();
    ++PollIndex_;

    YT_LOG_INFO("Polling queue state (PollIndex: %v)", PollIndex_);

    auto queueRowsOrError = WaitFor(QueueTable_->Select());

    if (!queueRowsOrError.IsOK()) {
        LatestPollError_ = static_cast<TError>(queueRowsOrError)
            << TErrorAttribute("poll_index", PollIndex_);
        YT_LOG_ERROR(queueRowsOrError, "Error polling state");
        return;
    }
    const auto& queueRows = queueRowsOrError.Value();

    YT_LOG_INFO("Queue rows collected (RowCount: %v)", queueRows.size());

    auto queueIds = GetKeys(Queues_);
    THashSet<TQueueId> missingQueueIds(queueIds.begin(), queueIds.end());

    for (const auto& row : queueRows) {
        missingQueueIds.erase(row.QueueId);
        ProcessQueueRow(row);
    }

    for (const auto& queueId : missingQueueIds) {
        UnregisterQueue(queueId);
    }
}

void TQueueAgent::ProcessQueueRow(const TQueueTableRow& row)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    const auto& queueId = row.QueueId;
    auto Logger = QueueAgentLogger.WithTag("QueueId: %Qv", queueId);

    YT_LOG_TRACE("Processing row (Row: %v)", ConvertToYsonString(row, EYsonFormat::Text).AsStringBuf());

    auto revision = row.Revision.value_or(NullRevision);

    // Ensure queue object existence.
    TQueueMap::insert_ctx context;
    auto iterator = Queues_.find(queueId, context);
    if (iterator == Queues_.end()) {
        YT_LOG_INFO("Queue registered");
        iterator = Queues_.emplace_direct(context, queueId, TQueue{
            .Error = TError("Queue is not in-sync yet")
        });
    }
    auto& queue = iterator->second;

    const auto& queueTypeOrError = DeduceQueueType(row);

    if (queue.Revision == revision) {
        // We already processed this row at given revision, skip it. Still,
        // let's validate that controller queue type is the same as deduced queue type.
        if (queue.Controller) {
            YT_VERIFY(queueTypeOrError.IsOK());
            YT_VERIFY(queue.Controller->GetQueueType() == queueTypeOrError.Value());
        }
        return;
    }

    YT_LOG_INFO(
        "Queue revision promoted (Revision: %v -> %v)",
        queue.Revision,
        revision);

    if (queue.Controller) {
        YT_VERIFY(WaitFor(queue.Controller->Stop()).IsOK());
        queue.Controller = nullptr;
    }
    queue.Error = TError();

    // Check if we must fill the error.
    if (queueTypeOrError.IsOK()) {
        auto queueType = queueTypeOrError.Value();
        YT_LOG_INFO("Creating queue controller (Type: %v)", queueType);
        queue.Controller = CreateQueueController(queueId, queueType, row, CreateSerializedInvoker(ControllerThreadPool_->GetInvoker()));
        queue.Controller->Start();
    } else {
        YT_LOG_INFO("Setting queue error (Error: %v)", static_cast<TError>(queueTypeOrError));
        queue.Error = static_cast<TError>(queueTypeOrError);
    }

    queue.Revision = revision;
}

void TQueueAgent::UnregisterQueue(const TQueueId& queueId)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    auto Logger = QueueAgentLogger.WithTag("QueueId: %Qv", queueId);

    auto iterator = Queues_.find(queueId);
    YT_VERIFY(iterator != Queues_.end());
    auto& queue = iterator->second;

    if (queue.Controller) {
        YT_VERIFY(WaitFor(queue.Controller->Stop()).IsOK());
        queue.Controller = nullptr;
    }

    Queues_.erase(iterator);

    YT_LOG_INFO("Queue unregistered");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
