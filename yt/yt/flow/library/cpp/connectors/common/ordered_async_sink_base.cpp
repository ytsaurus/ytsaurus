#include "ordered_async_sink_base.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TOrderedAsyncSinkState::Register(TRegistrar registrar)
{
    registrar.Parameter("producer_id", &TThis::ProducerId)
        .Default();
    registrar.Parameter("max_persisted_message_id", &TThis::MaxPersistedMessageId)
        .Default();
    registrar.Parameter("max_persisted_seq_no", &TThis::MaxPersistedSeqNo)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

TOrderedAsyncSinkBase::TOrderedAsyncSinkBase(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSinkBase(std::move(context), std::move(dynamicContext))
    , Logger(TSinkBase::Logger())
{
    if (GetSpec()->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Sink %Qv expects exactly one input stream but got %v",
            TypeName(*this),
            GetSpec()->InputStreamIds.size());
    }
}

void TOrderedAsyncSinkBase::Init(IInitContextPtr initContext)
{
    initContext->InitClient<TOrderedAsyncSinkState>(State_, "v0");
    if (State_->ProducerId.empty()) {
        State_->ProducerId = ToString(TGuid::Create());
    }
    LastDistributedMessageId_ = State_->MaxPersistedMessageId;
    LastDistributedSeqNo_ = State_->MaxPersistedSeqNo;
    DoInit(State_->ProducerId);
}

void TOrderedAsyncSinkBase::Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed)
{
    YT_TLOG_DEBUG("MessageLifeCycle.Sink: message was registered")
        .With("MessageId", message->MessageId)
        .With("StreamId", message->StreamId)
        .With("SystemTimestamp", message->SystemTimestamp)
        .With("EventTimestamp", message->EventTimestamp);
    auto guard = Guard(Lock_);
    if (message->MessageId <= State_->MaxPersistedMessageId) {
        // Already persisted — call callback immediately.
        onDistributed();
        return;
    }
    if (message->MessageId <= LastDistributedMessageId_) {
        THROW_ERROR_EXCEPTION("Expected message id to be greater than the last distributed message id")
            << TErrorAttribute("message_id", message->MessageId)
            << TErrorAttribute("last_distributed_message_id", LastDistributedMessageId_);
    }
    LastDistributedMessageId_ = std::max(LastDistributedMessageId_, message->MessageId);
    LastDistributedSeqNo_ += 1;
    auto seqNo = LastDistributedSeqNo_;

    RegisteredRequests_.push_back(TRequest{message, seqNo, std::move(onDistributed)});
}

void TOrderedAsyncSinkBase::Sync(NApi::IDynamicTableTransactionPtr /*transaction*/)
{
    {
        auto guard = Guard(Lock_);
        YT_VERIFY(PersistedRequests_.empty());
        PersistedRequests_ = std::exchange(DistributedRequests_, {});
    }
    for (const auto& request : PersistedRequests_) {
        YT_VERIFY(State_->MaxPersistedMessageId < request.Message->MessageId);
        YT_VERIFY(State_->MaxPersistedSeqNo < request.SeqNo);
        State_->MaxPersistedMessageId = std::max(State_->MaxPersistedMessageId, request.Message->MessageId);
        State_->MaxPersistedSeqNo = std::max(State_->MaxPersistedSeqNo, request.SeqNo);
    }
}

void TOrderedAsyncSinkBase::Commit()
{
    std::deque<TRequest> persistedRequests;
    std::deque<TRequest> requests;
    {
        auto guard = Guard(Lock_);
        std::swap(persistedRequests, PersistedRequests_);
        std::swap(requests, RegisteredRequests_);
    }
    for (auto& request : persistedRequests) {
        ObserveEventLag(request.Message->StreamId, request.Message->EventTimestamp);
        request.Callback();
    }
    for (auto& request : requests) {
        auto future = DoDistribute(request.Message, request.SeqNo);
        future.Subscribe(BIND([weakThis = MakeWeak(this), request = std::move(request)] (const TError& error) mutable {
            if (auto strongThis = weakThis.Lock(); strongThis && error.IsOK()) {
                auto guard = Guard(strongThis->Lock_);
                strongThis->DistributedRequests_.push_back(std::move(request));
            }
        }));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
