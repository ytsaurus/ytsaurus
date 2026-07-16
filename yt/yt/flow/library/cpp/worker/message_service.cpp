#include "message_service.h"

#include "input_buffer.h"
#include "input_manager.h"

#include "private.h"

#include <yt/yt/flow/library/cpp/common/message_batch.h>

#include <yt/yt/flow/library/cpp/common/worker/message_id_batch.h>
#include <yt/yt/flow/library/cpp/common/worker/message_service_proxy.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/build/build.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/iterator/zip.h>

#include <util/generic/adaptor.h>

namespace NYT::NFlow::NWorker {

using namespace NRpc;
using namespace NConcurrency;
using namespace NThreading;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TMessageService
    : public TServiceBase
{
public:
    TMessageService(
        IInputManagerPtr inputManager,
        IAuthenticatorPtr authenticator,
        TStreamSpecStoragePtr streamSpecStorage,
        IInvokerPtr invoker)
        : NRpc::TServiceBase(
            invoker,
            TMessageServiceProxy::GetDescriptor(),
            WorkerLogger(),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
        , InputManager_(std::move(inputManager))
        , StreamSpecStorage_(std::move(streamSpecStorage))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PushMessages));
    }

    void Init()
    {
        Cleaner_ = New<TPeriodicExecutor>(
            GetDefaultInvoker(),
            BIND(&TMessageService::CleanConnections, MakeWeak(this)),
            TPeriodicExecutorOptions::WithJitter(TDuration::Seconds(30)));
        Cleaner_->Start();
    }

private:
    struct TProcessedMessage
    {
        TMessageId MessageId;
        TJobId JobId;
    };

    struct TConnectionState
    {
        TInstant UpdateTimestamp;
        i64 LastAckedOffset;
        std::deque<TProcessedMessage> ProcessedTasks;
        IInputBuffer::TOnProcessedCallback OnProcessed;
    };

private:
    const IInputManagerPtr InputManager_;
    const TStreamSpecStoragePtr StreamSpecStorage_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, ConnectionStateLock_);
    THashMap<TGuid, TConnectionState> ConnectionIdToConnectionState_;
    TPeriodicExecutorPtr Cleaner_;

private:
    void CleanConnections()
    {
        auto guard = Guard(ConnectionStateLock_);
        auto now = TInstant::Now();
        for (auto iter = ConnectionIdToConnectionState_.begin(); iter != ConnectionIdToConnectionState_.end();) {
            if (iter->second.UpdateTimestamp + TDuration::Minutes(5) < now) {
                YT_TLOG_DEBUG("Dropping old connection")
                    .With("ConnectionId", iter->first)
                    .With("UpdateTimestamp", iter->second.UpdateTimestamp);
                ConnectionIdToConnectionState_.erase(iter++);
            } else {
                ++iter;
            }
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PushMessages)
    {
        int totalMessageCount = 0;
        for (const auto& jobData : request->jobs()) {
            totalMessageCount += jobData.message_count();
        }

        context->SetRequestInfo("Count: %v",
            totalMessageCount);

        auto streamSpecs = StreamSpecStorage_->GetStreamSpecs();
        THROW_ERROR_EXCEPTION_UNLESS(streamSpecs, "Message service is not ready for processing messages. StreamSpecStorage is not configured");

        response->mutable_message_states()->Resize(totalMessageCount, ToProto(EMessageDeliveryState::Declined));

        auto committedOffset = request->committed_offset();

        TGuid connectionId;
        if (request->has_connection_id()) {
            connectionId = FromProto<TGuid>(request->connection_id());
        } else {
            connectionId = TGuid::Create();
        }

        const auto now = TInstant::Now();
        const i64 maxProcessedBatchSize = request->max_processed_batch_size();
        YT_VERIFY(maxProcessedBatchSize > 0);

        IInputBuffer::TOnProcessedCallback onProcessed;

        {
            auto guard = Guard(ConnectionStateLock_);
            auto connectionStateIt = ConnectionIdToConnectionState_.find(connectionId);
            if (connectionStateIt == ConnectionIdToConnectionState_.end()) {
                if (request->has_connection_id()) {
                    YT_TLOG_DEBUG("Resetting old connection")
                        .With("ConnectionId", connectionId);
                    response->set_reset(true);
                    // NB: Recreate connectionId to avoid race during processing of two simultaneous requests
                    // with same connection ids after message service restart.
                    connectionId = TGuid::Create();
                }

                YT_TLOG_DEBUG("Received new connection id in PushMessages")
                    .With("ConnectionId", connectionId);

                connectionStateIt = EmplaceOrCrash(
                    ConnectionIdToConnectionState_,
                    connectionId,
                    TConnectionState{
                        .UpdateTimestamp = now,
                        .LastAckedOffset = 0,
                        .ProcessedTasks = {},
                        .OnProcessed = BIND(&TMessageService::OnMessageProcessed, MakeWeak(this), connectionId),
                    });
            } else {
                auto& connectionState = connectionStateIt->second;

                YT_VERIFY(committedOffset <= connectionState.LastAckedOffset + std::ssize(connectionState.ProcessedTasks));
                THROW_ERROR_EXCEPTION_UNLESS(
                    connectionState.LastAckedOffset <= committedOffset,
                    "Invalid committed_offset, probably the order in which requests were processed is broken "
                    "(CommittedOffset: %v, LastAckedOffset: %v, ProcessedTasks: %v)",
                    committedOffset,
                    connectionState.LastAckedOffset,
                    std::ssize(connectionState.ProcessedTasks));

                for (; connectionState.LastAckedOffset < committedOffset; ++connectionState.LastAckedOffset) {
                    connectionState.ProcessedTasks.pop_front();
                }
                connectionState.UpdateTimestamp = now;
            }

            ToProto(response->mutable_connection_id(), connectionId);

            auto& connectionState = connectionStateIt->second;
            onProcessed = connectionState.OnProcessed;

            // Processed messages are grouped by job; their ids ride in the response attachment.
            // Cap the batch so a growing ProcessedTasks cannot inflate the response past the RPC timeout.
            absl::flat_hash_map<TJobId, std::vector<const TMessageId*>, ::THash<TJobId>> processedMessageIdsByJob;
            i64 reported = 0;
            for (const auto& processedMessage : connectionState.ProcessedTasks) {
                if (reported >= maxProcessedBatchSize) {
                    break;
                }
                ++reported;
                YT_TLOG_DEBUG("MessageLifeCycle.InputMessageService: notifying distributor that message is processed")
                    .With("MessageId", processedMessage.MessageId)
                    .With("DestinationJobId", processedMessage.JobId)
                    .With("ConnectionId", connectionId);

                processedMessageIdsByJob[processedMessage.JobId].push_back(&processedMessage.MessageId);
            }

            std::deque<const TMessageId*> processedMessageIds;
            for (const auto& [jobId, ids] : processedMessageIdsByJob) {
                auto* jobProcessed = response->add_processed_jobs();
                ToProto(jobProcessed->mutable_job_id(), jobId);
                jobProcessed->set_message_count(ids.size());
                for (const auto* messageId : ids) {
                    processedMessageIds.push_back(messageId);
                }
            }
            response->Attachments().push_back(SerializeMessageIdBatch(processedMessageIds));

            response->set_offset(connectionState.LastAckedOffset + reported);
        }

        THashMap<TJobId, IInputBuffer::TConnectionOffer> jobIdToWorkerOffer;
        for (const auto& jobData : request->jobs()) {
            const auto jobId = FromProto<TJobId>(jobData.job_id());
            auto& workerOffer = jobIdToWorkerOffer[jobId];
            for (const auto& streamOffer : jobData.offers()) {
                const auto streamId = streamSpecs->GetStreamId(FromProto<TStreamSpecId>(streamOffer.stream_spec_id()));
                THROW_ERROR_EXCEPTION_UNLESS(IsSorted(streamOffer.min_order_timestamps().begin(), streamOffer.min_order_timestamps().end()),
                    "Buckets of stream offer must be sorted (JobId: %v, StreamId: %v, Offer: %v)",
                    jobId,
                    streamId,
                    NYson::ConvertToYsonString(streamOffer, NYson::EYsonFormat::Text));
                THROW_ERROR_EXCEPTION_UNLESS(streamOffer.min_order_timestamps_size() == streamOffer.inflated_byte_sizes_size(),
                    "Got invalid request, sizes mismatch (JobId: %v, StreamId: %v, Offer: %v)",
                    jobId,
                    streamId,
                    NYson::ConvertToYsonString(streamOffer, NYson::EYsonFormat::Text));

                auto& offer = workerOffer[streamId];
                offer.reserve(streamOffer.min_order_timestamps_size());
                for (const auto& [minOrderTimestamp, inflatedByteSize] : Zip(Reversed(streamOffer.min_order_timestamps()), Reversed(streamOffer.inflated_byte_sizes()))) {
                    offer.emplace_back(FromProto<TSystemTimestamp>(minOrderTimestamp), inflatedByteSize);
                }
            }
        }

        for (auto& [jobId, workerJobOffer] : jobIdToWorkerOffer) {
            auto inputBuffer = InputManager_->GetInputBuffer(jobId);
            if (inputBuffer) {
                inputBuffer->AddConnectionOffer(connectionId, std::move(workerJobOffer));
            }
        }

        const auto& requestAttachments = request->Attachments();
        // Messages of all jobs are concatenated in attachments[0], in the order jobs appear in
        // the request; we walk them in that same order as we walk jobs and their counts.
        auto inboundMessages = ParseMessageBatch(
            requestAttachments.empty() ? TRef() : TRef(requestAttachments[0]),
            streamSpecs);

        // Collect per-job AddMessages futures and await them all at once — saves N-1
        // fiber yields per RPC vs. one WaitFor per job.
        std::vector<TFuture<std::vector<EMessageDeliveryState>>> addMessagesFutures;
        std::vector<int> addMessagesGlobalIndices;
        addMessagesFutures.reserve(request->jobs_size());
        addMessagesGlobalIndices.reserve(request->jobs_size());

        int globalIndex = 0;
        for (const auto& jobData : request->jobs()) {
            const auto jobId = FromProto<TJobId>(jobData.job_id());
            const int messageCount = jobData.message_count();
            // Every job consumes messageCount message-states regardless of how this iteration exits.
            auto advanceGuard = Finally([&] {
                globalIndex += messageCount;
            });

            auto inputBuffer = InputManager_->GetInputBuffer(jobId);
            auto computationStreamSpecStorage = inputBuffer
                ? StreamSpecStorage_->GetComputationStreamSpecStorage(inputBuffer->GetComputationId())
                : nullptr;

            std::vector<TInputMessageConstPtr> inputMessages;
            inputMessages.reserve(messageCount);
            for (int index = 0; index < messageCount; ++index) {
                YT_VERIFY(!inboundMessages.empty());
                TMessage message = std::move(inboundMessages.front());
                inboundMessages.pop_front();

                if (!inputBuffer) {
                    YT_TLOG_DEBUG("MessageLifeCycle.InputMessageService: message was declined because job is unknown")
                        .With("JobId", jobId)
                        .With("MessageId", message.MessageId)
                        .With("ConnectionId", connectionId);
                    continue;
                }

                auto key = computationStreamSpecStorage->ComputeKey(message);
                inputMessages.emplace_back(New<TInputMessage>(std::move(message), std::move(key)));
            }

            if (!inputBuffer) {
                continue;
            }

            for (const auto& message : inputMessages) {
                YT_TLOG_DEBUG("MessageLifeCycle.InputMessageService: message was received")
                    .With("JobId", jobId)
                    .With("MessageId", message->MessageId)
                    .With("ConnectionId", connectionId);
            }

            addMessagesFutures.push_back(inputBuffer->AddMessages(
                connectionId,
                std::move(inputMessages),
                onProcessed));
            addMessagesGlobalIndices.push_back(globalIndex);
        }

        // The message records must match the per-job counts exactly.
        YT_VERIFY(inboundMessages.empty());

        // Dispatch GetConnectionLimits BEFORE awaiting AddMessages so both run in parallel on
        // each TInputBuffer's SerializedInvoker. Per-buffer execution still serializes Add then
        // GetLimits (correct ordering), but limits dispatch no longer waits for the AddMessages
        // round-trip — saves one fiber-yield cycle of latency per RPC at the handler level.
        std::vector<TFuture<THashMap<TStreamId, i64>>> limitsFutures;
        std::vector<TJobId> limitsJobIds;
        for (const auto& [jobId, workerJobOffer] : jobIdToWorkerOffer) {
            auto inputBuffer = InputManager_->GetInputBuffer(jobId);
            if (inputBuffer) {
                limitsFutures.push_back(inputBuffer->GetConnectionLimits(connectionId));
                limitsJobIds.push_back(jobId);
            }
        }

        if (!addMessagesFutures.empty()) {
            auto allResults = WaitFor(AllSucceeded(addMessagesFutures)).ValueOrThrow();
            for (int i = 0; i < std::ssize(allResults); ++i) {
                const auto& results = allResults[i];
                int gIdx = addMessagesGlobalIndices[i];
                for (int localIndex = 0; localIndex < std::ssize(results); ++localIndex) {
                    response->set_message_states(gIdx + localIndex, ToProto(results[localIndex]));
                }
            }
        }

        THashMap<TStreamId, NProto::TRspPushMessages::TStreamLimit*> streamLimits;
        if (!limitsFutures.empty()) {
            auto allLimits = WaitFor(AllSucceeded(limitsFutures)).ValueOrThrow();
            for (int i = 0; i < std::ssize(allLimits); ++i) {
                const auto& jobLimits = allLimits[i];
                const auto& jobId = limitsJobIds[i];
                for (const auto& [streamId, inflatedByteLimit] : jobLimits) {
                    auto& limitPtr = streamLimits[streamId];
                    if (!limitPtr) {
                        limitPtr = response->add_stream_limits();
                        ToProto(limitPtr->mutable_stream_id(), streamId);
                    }
                    ToProto(limitPtr->add_job_ids(), jobId);
                    limitPtr->add_inflated_next_batch_byte_limits(inflatedByteLimit);
                }
            }
        }

        TEnumIndexedArray<EMessageDeliveryState, int> countPerState;
        for (auto protoState : response->message_states()) {
            ++countPerState[FromProto<EMessageDeliveryState>(protoState)];
        }

        context->SetResponseInfo("CountPerState: %v",
            countPerState);

        context->Reply();
    }

    void OnMessageProcessed(TGuid connectionId, TJobId jobId, std::vector<TMessageId> messageIds)
    {
        auto guard = Guard(ConnectionStateLock_);

        auto iter = ConnectionIdToConnectionState_.find(connectionId);
        if (iter == ConnectionIdToConnectionState_.end()) {
            return;
        }
        auto& connectionState = iter->second;
        for (auto& messageId : messageIds) {
            YT_VERIFY(!messageId.Underlying().empty());
            connectionState.ProcessedTasks.push_back(TProcessedMessage{
                .MessageId = std::move(messageId),
                .JobId = jobId,
            });

            YT_TLOG_DEBUG("MessageLifeCycle.InputMessageService: message was processed, scheduled notification")
                .With("MessageId", connectionState.ProcessedTasks.back().MessageId)
                .With("DestinationJobId", jobId)
                .With("ConnectionId", connectionId)
                .With("CurrentOffset", connectionState.LastAckedOffset + connectionState.ProcessedTasks.size());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateMessageService(
    IInputManagerPtr inputManager,
    IAuthenticatorPtr authenticator,
    TStreamSpecStoragePtr streamSpecStorage,
    IInvokerPtr invoker)
{
    auto service = New<TMessageService>(
        std::move(inputManager),
        std::move(authenticator),
        std::move(streamSpecStorage),
        std::move(invoker));
    service->Init();
    return service;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
