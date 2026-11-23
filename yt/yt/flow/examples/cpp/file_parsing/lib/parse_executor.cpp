#include "parse_executor.h"

#include "messages.h"

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NFlow::NFileParsingExample {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TParseExecutorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_count", &TThis::SlotCount)
        .Default(2)
        .GreaterThan(0);
    registrar.Parameter("max_queue_wait", &TThis::MaxQueueWait)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

bool TParseExecutor::TOldestFirst::operator()(const TEntryPtr& lhs, const TEntryPtr& rhs) const
{
    return std::tie(lhs->Task->FirstSeenAt, lhs->SequenceNumber) < std::tie(rhs->Task->FirstSeenAt, rhs->SequenceNumber);
}

////////////////////////////////////////////////////////////////////////////////

TParseExecutor::TParseExecutor(TParseExecutorConfigPtr config, IInvokerPtr invoker, TParseCallback parse)
    : Config_(std::move(config))
    , Invoker_(std::move(invoker))
    , Parse_(std::move(parse))
{ }

TFuture<TParseResultPtr> TParseExecutor::Submit(TParseTaskPtr task)
{
    auto entry = New<TEntry>();
    entry->Task = std::move(task);
    auto future = entry->SlotGranted.ToFuture().Apply(
        BIND(&TParseExecutor::OnSlotDecided, MakeStrong(this), entry->Task));

    bool granted = false;
    {
        auto guard = Guard(Lock_);
        entry->SequenceNumber = NextSequenceNumber_++;
        granted = RunningCount_ < Config_->SlotCount;
        if (granted) {
            entry->HasSlot = true;
            ++RunningCount_;
        } else {
            entry->Deadline = TInstant::Now() + Config_->MaxQueueWait;
            WaitingEntries_.insert(entry);
        }
    }

    future.Subscribe(BIND(&TParseExecutor::OnCompleted, MakeStrong(this), entry));
    if (granted) {
        entry->SlotGranted.TrySet(true);
    } else {
        // Not under the lock: the delayed executor may run the callback right away during shutdown.
        // The callback of a task that has left the queue by then does nothing.
        TDelayedExecutor::Submit(
            BIND([weakThis = MakeWeak(this), weakEntry = MakeWeak(entry)] {
                auto this_ = weakThis.Lock();
                auto entry = weakEntry.Lock();
                if (this_ && entry) {
                    this_->OnWaitExpired(entry);
                }
            }),
            Config_->MaxQueueWait);
    }
    return future;
}

TFuture<TParseResultPtr> TParseExecutor::OnSlotDecided(const TParseTaskPtr& task, bool granted)
{
    if (!granted) {
        auto result = New<TParseResult>();
        result->Rejected = true;
        return MakeFuture(std::move(result));
    }
    return BIND(Parse_, task)
        .AsyncVia(Invoker_)
        .Run();
}

void TParseExecutor::OnWaitExpired(const TEntryPtr& entry)
{
    {
        auto guard = Guard(Lock_);
        if (!WaitingEntries_.erase(entry)) {
            return;
        }
    }
    entry->SlotGranted.TrySet(false);
}

void TParseExecutor::OnCompleted(const TEntryPtr& entry, const TErrorOr<TParseResultPtr>& /*result*/)
{
    std::vector<TEntryPtr> grantedEntries;
    std::vector<TEntryPtr> expiredEntries;
    {
        auto guard = Guard(Lock_);
        // The task is rejected or canceled while waiting.
        if (!entry->HasSlot) {
            WaitingEntries_.erase(entry);
            return;
        }
        --RunningCount_;
        auto now = TInstant::Now();
        while (!WaitingEntries_.empty() && RunningCount_ < Config_->SlotCount) {
            auto nextEntry = WaitingEntries_.extract(WaitingEntries_.begin()).value();
            // The rejection timer of an expired task may be late; the task must not start anyway.
            if (nextEntry->Deadline <= now) {
                expiredEntries.push_back(std::move(nextEntry));
                continue;
            }
            nextEntry->HasSlot = true;
            ++RunningCount_;
            grantedEntries.push_back(std::move(nextEntry));
        }
    }

    for (const auto& expiredEntry : expiredEntries) {
        expiredEntry->SlotGranted.TrySet(false);
    }
    for (const auto& grantedEntry : grantedEntries) {
        grantedEntry->SlotGranted.TrySet(true);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileParsingExample
