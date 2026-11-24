#include "writer.h"

#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/library/arcadia_future_interop/interop.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <ydb-cpp-sdk/client/topic/errors.h>

namespace NYT::NFlow {

using namespace NYdb::NTopic;
using NConcurrency::WaitFor;
using NConcurrency::WaitForFast;
using NConcurrency::Yield;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TYdbWriter
    : public IYdbWriter
{
public:
    TYdbWriter(
        IResourcePtr resource,
        std::shared_ptr<TTopicClient> topicClient,
        TWriteSessionSettings settings,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler)
        : Resource_(std::move(resource))
        , Writer_(New<TRetryableYdbWriter>(
            std::move(topicClient),
            std::move(settings),
            std::move(logger),
            std::move(statusProfiler)))
        , Executor_(BIND([resource = Resource_, writer = Writer_] {
            // Keep the SDK driver alive until cancellation has unwound Run().
            YT_VERIFY(resource);
            writer->Run();
        })
                .AsyncVia(std::move(invoker))
                .Run())
    { }

    ~TYdbWriter() override
    {
        if (Executor_) {
            Executor_.Cancel(TError("Sink closed"));
        }
    }

    TFuture<void> Write(TYdbMessageToWrite message) override
    {
        return Writer_->Write(std::move(message));
    }

private:
    const IResourcePtr Resource_;
    const TRetryableYdbWriterPtr Writer_;
    const TFuture<void> Executor_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IYdbWriterPtr CreateYdbWriter(
    IResourcePtr resource,
    std::shared_ptr<NYdb::NTopic::TTopicClient> topicClient,
    NYdb::NTopic::TWriteSessionSettings settings,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    IStatusProfilerPtr statusProfiler)
{
    return New<TYdbWriter>(
        std::move(resource),
        std::move(topicClient),
        std::move(settings),
        std::move(invoker),
        std::move(logger),
        std::move(statusProfiler));
}

////////////////////////////////////////////////////////////////////////////////

TRetryableYdbWriter::TRetryableYdbWriter(
    std::shared_ptr<TTopicClient> topicClient,
    NYdb::NTopic::TWriteSessionSettings settings,
    NLogging::TLogger logger,
    IStatusProfilerPtr statusProfiler)
    : TopicClient_(std::move(topicClient))
    , Settings_(std::move(settings))
    , Logger(std::move(logger))
    , ErrorState_(statusProfiler->ErrorState("writer"))
{ }

TFuture<void> TRetryableYdbWriter::Write(TRetryableYdbWriter::TMessageToWrite message)
{
    auto promise = NewPromise<void>();
    {
        auto guard = Guard(Lock_);
        EmplaceOrCrash(Promises_, message.SeqNo, promise);
    }
    Queue_.Enqueue(std::move(message));
    return promise.ToFuture();
}

TWriteMessage TRetryableYdbWriter::MakeWriteMessage(const TMessageToWrite& message) const
{
    auto writeMessage = TWriteMessage(message.Data)
        .SeqNo(message.SeqNo);
    if (!message.MessageMeta.empty()) {
        writeMessage.MessageMeta(message.MessageMeta);
    }
    if (message.DeferredPublication) {
        writeMessage.DeferredPublication(*message.DeferredPublication);
    }
    return writeMessage;
}

void TRetryableYdbWriter::Run()
{
    std::map<i64, TMessageToWrite> inflight;

    auto messageFuture = Queue_.Dequeue();
    while (true) {
        try {
            auto session = TopicClient_->CreateWriteSession(Settings_);

            std::optional<TContinuationToken> token;
            for (const auto& [seqNo, message] : inflight) {
                while (!token) {
                    YT_UNUSED_FUTURE(WaitForFast(FromArcadiaFuture(session->WaitEvent())));
                    ProcessEvent(session->GetEvent(/*block*/ false), &token, &inflight);
                }
                session->WriteEncoded(
                    std::move(*token),
                    MakeWriteMessage(message));
                token = {};
            }

            if (inflight.empty()) {
                ErrorState_->ClearError(); // No inflight - no error.
            }

            while (true) {
                YT_UNUSED_FUTURE(WaitFor(AnySet<void>(
                    {messageFuture.AsVoid(), FromArcadiaFuture(session->WaitEvent())},
                    TFutureCombinerOptions{
                        .PropagateCancelationToInput = false,
                        .CancelInputOnShortcut = false,
                    })));
                if (messageFuture.IsSet()) {
                    auto message = messageFuture.GetOrCrash().ValueOrThrow();
                    const auto seqNo = message.SeqNo;
                    auto messageIt = EmplaceOrCrash(inflight, seqNo, std::move(message));
                    messageFuture = Queue_.Dequeue();

                    while (!token) {
                        YT_UNUSED_FUTURE(WaitFor(FromArcadiaFuture(session->WaitEvent())));
                        ProcessEvent(session->GetEvent(/*block*/ false), &token, &inflight);
                    }

                    session->WriteEncoded(
                        std::move(*token),
                        MakeWriteMessage(messageIt->second));
                    token = {};
                }

                for (auto event : session->GetEvents(/*block*/ false)) {
                    ProcessEvent(event, &token, &inflight);
                }
            }
        } catch (const std::exception& ex) {
            static constexpr auto Message = "Error while running session"_sb;
            ErrorState_->SetError(TError(Message)
                    .With(ex));
            YT_TLOG_ERROR(Message)
                .With(ex);
            Yield();
        }
    }
}

void TRetryableYdbWriter::ProcessEvent(
    std::optional<TWriteSessionEvent::TEvent> event,
    std::optional<TContinuationToken>* token,
    std::map<i64, TRetryableYdbWriter::TMessageToWrite>* inflight)
{
    if (!event) {
        return;
    }
    if (auto* readyEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
        *token = std::move(readyEvent->ContinuationToken);
    } else if (auto* ackEvent = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
        std::optional<TError> legacyRetryError;
        bool deferredWriteFailed = false;
        for (const auto& ack : ackEvent->Acks) {
            if (ack.State == TWriteSessionEvent::TWriteAck::EES_WRITTEN ||
                ack.State == TWriteSessionEvent::TWriteAck::EES_ALREADY_WRITTEN ||
                ack.State == TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX)
            {
                YT_TLOG_DEBUG("Got ack")
                    .With("SeqNo", ack.SeqNo);

                CompleteWrite(ack.SeqNo, TError(), inflight);
            } else {
                auto error = TError("Topic permanently rejected write")
                    .With("producer_seq_no", ack.SeqNo)
                    .With("ack_state", static_cast<int>(ack.State));
                auto message = inflight->find(ack.SeqNo);
                if (message == inflight->end()) {
                    THROW_ERROR_EXCEPTION("Got ack for unknown SeqNo: %v", ack.SeqNo);
                }
                if (message->second.DeferredPublication) {
                    CompleteWrite(ack.SeqNo, error, inflight);
                    ErrorState_->SetError(std::move(error));
                    deferredWriteFailed = true;
                } else if (!legacyRetryError) {
                    legacyRetryError = std::move(error);
                }
            }
        }
        if (legacyRetryError) {
            legacyRetryError->ThrowOnError();
        }
        if (deferredWriteFailed) {
            return;
        }
    } else if (auto* closedEvent = std::get_if<TSessionClosedEvent>(&*event)) {
        auto error = TError("Topic write session closed")
            .With("status", closedEvent->GetStatus());
        if (GetRetryErrorClass(closedEvent->GetStatus()) == ERetryErrorClass::NoRetry) {
            FailDeferredInflightWrites(error, inflight);
        }
        error.ThrowOnError();
    } else {
        THROW_ERROR_EXCEPTION("Unknown event %v", DebugString(*event));
    }
    // Non‑trivial session event processed: session is considered alive, clearing current error.
    ErrorState_->ClearError();
}

void TRetryableYdbWriter::CompleteWrite(
    i64 seqNo,
    TError error,
    std::map<i64, TMessageToWrite>* inflight)
{
    if (auto iter = inflight->find(seqNo); iter != inflight->end()) {
        inflight->erase(iter);
    } else {
        THROW_ERROR_EXCEPTION("Got ack for unknown SeqNo: %v", seqNo);
    }

    TPromise<void> promise;
    {
        auto guard = Guard(Lock_);
        if (auto iter = Promises_.find(seqNo); iter != Promises_.end()) {
            promise = iter->second;
            Promises_.erase(iter);
        } else {
            THROW_ERROR_EXCEPTION("Got ack for unknown SeqNo: %v", seqNo);
        }
    }
    promise.TrySet(std::move(error));
}

void TRetryableYdbWriter::FailDeferredInflightWrites(
    const TError& error,
    std::map<i64, TMessageToWrite>* inflight)
{
    for (auto iterator = inflight->begin(); iterator != inflight->end();) {
        if (!iterator->second.DeferredPublication) {
            ++iterator;
            continue;
        }
        const auto seqNo = iterator->first;
        ++iterator;
        CompleteWrite(seqNo, error, inflight);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
