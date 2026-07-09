#include "sync_sink_base.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TSyncSinkState::Register(TRegistrar registrar)
{
    registrar.Parameter("max_persisted_message_id", &TThis::MaxPersistedMessageId)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TSyncSinkBase::TSyncSinkBase(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSinkBase(std::move(context), std::move(dynamicContext))
    , Logger(TSinkBase::Logger())
{
    if (this->GetSpec()->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Sink %Qv expects exactly one input stream but got %v",
            TypeName(*this),
            this->GetSpec()->InputStreamIds.size());
    }
}

void TSyncSinkBase::Init(IInitContextPtr initContext)
{
    initContext->InitClient<TSyncSinkState>(State_, "v0");
    DoInit();
}

void TSyncSinkBase::Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed)
{
    YT_LOG_DEBUG("MessageLifeCycle.Sink: message was registered (MessageId: %v, StreamId: %v, SystemTimestamp: %v, EventTimestamp: %v)",
        message->MessageId,
        message->StreamId,
        message->SystemTimestamp,
        message->EventTimestamp);
    ObserveEventLag(message->StreamId, message->EventTimestamp);
    auto guard = Guard(Lock_);
    if (message->MessageId <= State_->MaxPersistedMessageId) {
        // Already persisted — call callback immediately.
        onDistributed();
        return;
    }
    State_->MaxPersistedMessageId = std::max(State_->MaxPersistedMessageId, message->MessageId);
    RegisteredMessages_.push_back(message);
    // Sync sink delivers synchronously in Sync(); callback is called immediately after registration.
    onDistributed();
}

void TSyncSinkBase::Sync(NApi::IDynamicTableTransactionPtr transaction)
{
    std::deque<TOutputMessageConstPtr> messages;
    {
        auto guard = Guard(Lock_);
        std::swap(messages, RegisteredMessages_);
    }
    DoDistribute(transaction, messages);
}

void TSyncSinkBase::Commit()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
