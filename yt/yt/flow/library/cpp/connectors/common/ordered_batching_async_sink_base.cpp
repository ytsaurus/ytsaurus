#include "ordered_batching_async_sink_base.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TOrderedBatchingAsyncSinkState::Register(TRegistrar registrar)
{
    registrar.Parameter("producer_id", &TThis::ProducerId)
        .Default();
    registrar.Parameter("max_persisted_message_id", &TThis::MaxPersistedMessageId)
        .Default();
    registrar.Parameter("max_persisted_seq_no", &TThis::MaxPersistedSeqNo)
        .Default(0);
    registrar.Parameter("batch_bounds", &TThis::BatchBounds)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TOrderedBatchingAsyncSinkBase::TExtendedDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("max_rows_per_batch", &TThis::MaxRowsPerBatch)
        .InRange(1, 2000)
        .Default(1000);
    registrar.Parameter("max_bytes_per_batch", &TThis::MaxBytesPerBatch)
        .InRange(1, 10_MB)
        .Default(5_MB);
}

////////////////////////////////////////////////////////////////////////////////

TOrderedBatchingAsyncSinkBase::TOrderedBatchingAsyncSinkBase(
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

void TOrderedBatchingAsyncSinkBase::Init(IInitContextPtr initContext)
{
    initContext->InitClient<TOrderedBatchingAsyncSinkState>(State_, "v0");
    if (State_->ProducerId.empty()) {
        State_->ProducerId = ToString(TGuid::Create());
    }
    LastMessageId_ = State_->MaxPersistedMessageId;
    LastSeqNo_ = State_->MaxPersistedSeqNo;
    OldBounds_ = State_->BatchBounds;
    NextBatch();
    DoInit(State_->ProducerId);
}

void TOrderedBatchingAsyncSinkBase::Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed)
{
    const auto byteSize = message->ByteSize;
    YT_LOG_DEBUG("MessageLifeCycle.Sink: message was registered (MessageId: %v, StreamId: %v, SystemTimestamp: %v, EventTimestamp: %v, ByteSize: %v)",
        message->MessageId,
        message->StreamId,
        message->SystemTimestamp,
        message->EventTimestamp,
        byteSize);
    auto guard = Guard(Lock_);
    if (message->MessageId <= State_->MaxPersistedMessageId) {
        // Already persisted — call callback immediately.
        onDistributed();
        return;
    }
    if (message->MessageId <= LastMessageId_) {
        THROW_ERROR_EXCEPTION("Ids of messages passed to Distribute() must strictly increase")
            << TErrorAttribute("message_id", message->MessageId)
            << TErrorAttribute("last_message_id", LastMessageId_);
    }
    LastMessageId_ = std::max(LastMessageId_, message->MessageId);

    Request_.Batch.push_back(message);
    Request_.ByteSize += byteSize;
    Request_.Callbacks.push_back(std::move(onDistributed));
    CheckBatch();
}

void TOrderedBatchingAsyncSinkBase::Sync(NApi::IDynamicTableTransactionPtr /*transaction*/)
{
    {
        auto guard = Guard(Lock_);
        FlushBatch();
        YT_VERIFY(PersistedRequests_.empty());
        PersistedRequests_ = std::exchange(DistributedRequests_, {});
        YT_VERIFY(Request_.Batch.empty());
    }
    for (const auto& request : PersistedRequests_) {
        YT_VERIFY(!request.Batch.empty());

        const auto minMessageId = request.Batch.front()->MessageId;
        const auto maxMessageId = request.Batch.back()->MessageId;
        YT_LOG_DEBUG("Distributing batch (SeqNo: %v, MinMessageId: %v, MaxMessageId: %v, Count: %v, ByteSize: %v)",
            request.SeqNo,
            minMessageId,
            maxMessageId,
            request.Batch.size(),
            request.ByteSize);

        YT_VERIFY(minMessageId <= maxMessageId);
        YT_VERIFY(State_->MaxPersistedMessageId < minMessageId);
        YT_VERIFY(State_->MaxPersistedMessageId < maxMessageId);
        YT_VERIFY(State_->MaxPersistedSeqNo < request.SeqNo);
        State_->MaxPersistedMessageId = std::max(State_->MaxPersistedMessageId, maxMessageId);
        State_->MaxPersistedSeqNo = std::max(State_->MaxPersistedSeqNo, request.SeqNo);
    }
    while (!State_->BatchBounds.empty() && State_->BatchBounds.front() <= State_->MaxPersistedMessageId) {
        State_->BatchBounds.pop_front();
    }
}

void TOrderedBatchingAsyncSinkBase::Commit()
{
    std::deque<TRequest> persistedRequests;
    std::deque<TRequest> requests;
    {
        auto guard = Guard(Lock_);
        std::swap(persistedRequests, PersistedRequests_);
        std::swap(requests, RegisteredRequests_);
    }
    for (auto& request : persistedRequests) {
        YT_VERIFY(request.Callbacks.size() == request.Batch.size());
        for (auto i = 0; i < std::ssize(request.Callbacks); ++i) {
            ObserveEventLag(request.Batch[i]->StreamId, request.Batch[i]->EventTimestamp);
            request.Callbacks[i]();
        }
    }
    for (auto& request : requests) {
        YT_VERIFY(request.Batch.size() > 0);
        const auto minMessageId = request.Batch.front()->MessageId;
        const auto maxMessageId = request.Batch.back()->MessageId;
        YT_LOG_DEBUG("Distributed batch (SeqNo: %v, MinMessageId: %v, MaxMessageId: %v, Count: %v, ByteSize: %v)",
            request.SeqNo,
            minMessageId,
            maxMessageId,
            request.Batch.size(),
            request.ByteSize);
        auto future = DoDistribute(request.Batch, request.SeqNo);
        future.Subscribe(BIND([weakThis = MakeWeak(this), request = std::move(request)] (const TError& error) mutable {
            if (auto strongThis = weakThis.Lock(); strongThis && error.IsOK()) {
                auto guard = Guard(strongThis->Lock_);
                strongThis->DistributedRequests_.push_back(std::move(request));
            }
        }));
    }
}

void TOrderedBatchingAsyncSinkBase::NextBatch()
{
    ++LastSeqNo_;
    Request_ = {
        .SeqNo = LastSeqNo_,
        .Batch = {},
        .ByteSize = 0,
        .Callbacks = {},
    };
}

void TOrderedBatchingAsyncSinkBase::FlushBatch()
{
    if (Request_.Batch.empty()) {
        return;
    }

    const auto bound = Request_.Batch.back()->MessageId;
    if (State_->BatchBounds.empty() || State_->BatchBounds.back() < bound) {
        State_->BatchBounds.push_back(bound);
    } else {
        YT_VERIFY(!OldBounds_.empty() && OldBounds_.front() == bound);
        OldBounds_.pop_front();
    }

    RegisteredRequests_.push_back(std::move(Request_));
    NextBatch();
}

void TOrderedBatchingAsyncSinkBase::CheckBatch()
{
    if (Request_.Batch.empty()) {
        return;
    }

    const auto bound = Request_.Batch.back()->MessageId;
    if (OldBounds_.empty()) {
        if (std::ssize(Request_.Batch) >= GetDynamicParameters()->MaxRowsPerBatch ||
            Request_.ByteSize >= GetDynamicParameters()->MaxBytesPerBatch) {
            FlushBatch();
        }
    } else {
        if (OldBounds_.front() <= bound) {
            FlushBatch();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
