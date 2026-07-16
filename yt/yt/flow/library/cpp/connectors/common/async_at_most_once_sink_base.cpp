#include "async_at_most_once_sink_base.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TAtMostOnceStrategyParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TAtMostOnceStrategyDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("suspend_destruction_duration", &TThis::SuspendDestructionDuration)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("total_queue_bytes_limit", &TThis::TotalQueueBytesLimit)
        .Default(NYTree::TSize(100_MB));
}

////////////////////////////////////////////////////////////////////////////////

TAsyncAtMostOnceSinkBase::TAsyncAtMostOnceSinkBase(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSinkBase(std::move(context), std::move(dynamicContext))
    , QueueSizeSemaphore_(New<NConcurrency::TAsyncSemaphore>(/*totalSlots*/ 0, /*enableOverdraft*/ true))
{
    if (GetSpec()->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Sink %Qv expects exactly one input stream but got %v",
            TypeName(*this),
            GetSpec()->InputStreamIds.size());
    }
}

void TAsyncAtMostOnceSinkBase::Init(IInitContextPtr /*initContext*/)
{
    ProducerId_ = ToString(TGuid::Create());
    DoInit(ProducerId_);
}

void TAsyncAtMostOnceSinkBase::Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed)
{
    YT_TLOG_DEBUG("MessageLifeCycle.Sink: message was registered")
        .With("MessageId", message->MessageId)
        .With("StreamId", message->StreamId)
        .With("SystemTimestamp", message->SystemTimestamp)
        .With("EventTimestamp", message->EventTimestamp);
    ObserveEventLag(message->StreamId, message->EventTimestamp);
    // The at-most-once sink is fire-and-forget: it does not hold the output buffer.
    // Signal distribution immediately.
    onDistributed();
    auto guard = Guard(Lock_);
    LastDistributedSeqNo_ += 1;
    auto seqNo = LastDistributedSeqNo_;
    RegisteredRequests_.push_back(TRequest{message, seqNo});
}

void TAsyncAtMostOnceSinkBase::Sync(NApi::IDynamicTableTransactionPtr /*transaction*/)
{ }

void TAsyncAtMostOnceSinkBase::Commit()
{
    std::deque<TRequest> requests;
    {
        auto guard = Guard(Lock_);
        std::swap(requests, RegisteredRequests_);
    }
    if (!QueueSizeSemaphore_->IsReady()) {
        YT_TLOG_WARNING("Async sink queue is full, dropping messages")
            .With("MessageCount", requests.size())
            .With("UsedBytes", QueueSizeSemaphore_->GetUsed())
            .With("TotalBytes", QueueSizeSemaphore_->GetTotal());
        return;
    }
    ui64 rowsSize = 0;
    std::vector<TFuture<void>> futures;
    for (auto& request : requests) {
        auto [future, rowSize] = DoDistribute(request.Message, request.SeqNo);
        futures.push_back(std::move(future));
        rowsSize += rowSize;
        if (!QueueSizeSemaphore_->Acquire(rowSize)) {
            break;
        }
    }
    if (futures.size() < requests.size()) {
        YT_TLOG_WARNING("Async sink queue size limit exceeded, some messages will be dropped")
            .With("DroppedMessageCount", requests.size() - futures.size())
            .With("TotalMessageCount", requests.size())
            .With("UsedBytes", QueueSizeSemaphore_->GetUsed())
            .With("TotalBytes", QueueSizeSemaphore_->GetTotal());
    }
    TFuture<void> combinedFuture = AllSucceeded(
        std::move(futures),
        TFutureCombinerOptions{
            .PropagateCancelationToInput = false,
            .CancelInputOnShortcut = false,
        });
    combinedFuture.Subscribe(BIND([weakThis = MakeWeak(this), rowsSize] (const TError& /*error*/) {
        if (auto strongThis = weakThis.Lock(); strongThis) {
            strongThis->QueueSizeSemaphore_->Release(rowsSize);
        }
    }));
}

void TAsyncAtMostOnceSinkBase::Reconfigure(TAtMostOnceStrategyDynamicParametersPtr parameters)
{
    THROW_ERROR_EXCEPTION_UNLESS(parameters, "AtMostOnceStrategy is required with at most once strategy");
    {
        auto guard = Guard(Lock_);
        DynamicParameters_ = std::move(parameters);
    }
    QueueSizeSemaphore_->SetTotal(DynamicParameters_->TotalQueueBytesLimit);
}

void TAsyncAtMostOnceSinkBase::SuspendDestructionGuarded(std::vector<TIntrusivePtr<TRefCounted>> prevent)
{
    TDuration suspendDuration;
    {
        auto guard = Guard(Lock_);
        suspendDuration = DynamicParameters_->SuspendDestructionDuration;
    }
    if (suspendDuration == TDuration::Zero() || QueueSizeSemaphore_->IsFree()) {
        return;
    }

    YT_TLOG_INFO("Called SuspendDestructionGuarded")
        .With("SuspendDurationMs", suspendDuration.MilliSeconds())
        .With("UsedBytes", QueueSizeSemaphore_->GetUsed());

    // AsyncAcquire(totalSlots) returns a future that becomes set when all slots
    // are free (FreeSlots_ >= TotalSlots_), which is equivalent to IsFree().
    auto acquireFuture = QueueSizeSemaphore_->AsyncAcquire(QueueSizeSemaphore_->GetTotal());
    auto timeoutFuture = NConcurrency::TDelayedExecutor::MakeDelayed(suspendDuration);

    AnySucceeded(
        std::vector<TFuture<void>>{acquireFuture.AsVoid(), timeoutFuture},
        TFutureCombinerOptions{
            .PropagateCancelationToInput = false,
            .CancelInputOnShortcut = false,
        })
        .Subscribe(BIND([Logger = Logger, acquireFuture, timeoutFuture, prevent = std::move(prevent)] (const TError& error) {
            if (!error.IsOK()) {
                YT_TLOG_WARNING("Error while waiting for in-flight distribute futures during destruction")
                    .With(error);
            }
            if (!acquireFuture.IsSet()) {
                YT_TLOG_WARNING("Destructor timeout expired, some distribute futures are still in-flight")
                    .With(error);
            }
        }).Via(GetContext()->SerializedInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
