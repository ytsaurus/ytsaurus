#include "progress_counter.h"

#include "private.h"

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NControllerAgent {

using namespace NYTree;
using namespace NYson;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TProgressCounter::TProgressCounter()
    : TotalEnabled_(false)
    , Total_(0)
    , Running_(0)
    , Pending_(0)
    , Failed_(0)
    , Lost_(0)
{ }

TProgressCounter::TProgressCounter(i64 total)
    : TProgressCounter()
{
    Set(total);
}

void TProgressCounter::Set(i64 total)
{
    YCHECK(!TotalEnabled_);
    TotalEnabled_ = true;
    Increment(total);
}

bool TProgressCounter::IsTotalEnabled() const
{
    return TotalEnabled_;
}

void TProgressCounter::Increment(i64 value)
{
    YCHECK(TotalEnabled_);
    Total_ += value;
    YCHECK(Total_ >= 0);
    Pending_ += value;
    YCHECK(Pending_ >= 0);

    if (Parent_) {
        Parent_->Increment(value);
    }
}

void TProgressCounter::Decrement(i64 value)
{
    YCHECK(TotalEnabled_);
    Total_ -= value;
    YCHECK(Total_ >= 0);
    Pending_ -= value;
    YCHECK(Pending_ >= 0);

    if (Parent_) {
        Parent_->Decrement(value);
    }
}

i64 TProgressCounter::GetTotal() const
{
    YCHECK(TotalEnabled_);
    return Total_;
}

i64 TProgressCounter::GetRunning() const
{
    return Running_;
}

i64 TProgressCounter::GetCompletedTotal() const
{
    return std::accumulate(Completed_.begin(), Completed_.end(), 0LL);
}

i64 TProgressCounter::GetCompleted(EInterruptReason reason) const
{
    return Completed_[reason];
}

i64 TProgressCounter::GetInterruptedTotal() const
{
    return GetCompletedTotal() - Completed_[EInterruptReason::None];
}

i64 TProgressCounter::GetPending() const
{
    YCHECK(TotalEnabled_);
    return Pending_;
}

i64 TProgressCounter::GetFailed() const
{
    return Failed_;
}

i64 TProgressCounter::GetAbortedTotal() const
{
    return std::accumulate(Aborted_.begin(), Aborted_.end(), 0LL);
}

i64 TProgressCounter::GetAbortedScheduled() const
{
    return GetAbortedTotal() - GetAbortedNonScheduled();
}

i64 TProgressCounter::GetAbortedNonScheduled() const
{
    i64 sum = 0;
    for (auto reason : TEnumTraits<EAbortReason>::GetDomainValues()) {
        if (IsSchedulingReason(reason)) {
            sum += Aborted_[reason];
        }
    }
    return sum;
}

i64 TProgressCounter::GetAborted(EAbortReason reason) const
{
    return Aborted_[reason];
}

i64 TProgressCounter::GetLost() const
{
    return Lost_;
}

void TProgressCounter::Start(i64 count)
{
    if (TotalEnabled_) {
        YCHECK(Pending_ >= count);
        Pending_ -= count;
    }
    Running_ += count;

    if (Parent_) {
        Parent_->Start(count);
    }
}

void TProgressCounter::Completed(i64 count, EInterruptReason reason)
{
    YCHECK(Running_ >= count);
    Running_ -= count;
    Completed_[reason] += count;

    if (Parent_) {
        Parent_->Completed(count, reason);
    }
}

void TProgressCounter::Failed(i64 count)
{
    YCHECK(Running_ >= count);
    Running_ -= count;
    Failed_ += count;
    if (TotalEnabled_) {
        Pending_ += count;
    }

    if (Parent_) {
        Parent_->Failed(count);
    }
}

void TProgressCounter::Aborted(i64 count, EAbortReason reason)
{
    YCHECK(!IsSentinelReason(reason));
    YCHECK(Running_ >= count);
    Running_ -= count;
    Aborted_[reason] += count;
    if (TotalEnabled_) {
        Pending_ += count;
    }

    if (Parent_) {
        Parent_->Aborted(count, reason);
    }
}

void TProgressCounter::Lost(i64 count)
{
    YCHECK(Completed_[EInterruptReason::None] >= count);
    Completed_[EInterruptReason::None] -= count;
    Lost_ += count;
    if (TotalEnabled_) {
        Pending_ += count;
    }

    if (Parent_) {
        Parent_->Lost(count);
    }
}

void TProgressCounter::SetParent(const TProgressCounterPtr& parent)
{
    YCHECK(!Parent_);
    Parent_ = parent;
    if (TotalEnabled_) {
        Parent_->Increment(Total_);
    }

    // Running jobs.
    Parent_->Start(Running_);

    // NB: all modifications below require starting fictional jobs before accounting them in the proper category.
    // This may lead to the situation when parent's Total_ is smaller than his Running_ (like if there were
    // lots of aborted jobs, but total job count is relatively small). To overcome this issue we calculate total
    // number of jobs we are going to start fictionally, increment parent's total job count by this value,
    // performs all the modifications and finally decrement it back.
    i64 totalDelta = GetCompletedTotal() + Failed_ + Lost_ + GetAbortedTotal();
    Parent_->Increment(totalDelta);

    // Completed jobs.
    for (const auto& interruptReason : TEnumTraits<EInterruptReason>::GetDomainValues()) {
        if (auto value = Completed_[interruptReason]) {
            Parent_->Start(value);
            Parent_->Completed(value, interruptReason);
        }
    }

    // Failed jobs.
    Parent_->Start(Failed_);
    Parent_->Failed(Failed_);

    // Lost jobs (yes, job losing is a rather complicated process).
    Parent_->Start(Lost_);
    Parent_->Completed(Lost_, EInterruptReason::None);
    Parent_->Lost(Lost_);

    // Aborted jobs.
    for (const auto& abortReason : TEnumTraits<EAbortReason>::GetDomainValues()) {
        if (auto value = Aborted_[abortReason]) {
            Parent_->Start(value);
            Parent_->Aborted(value, abortReason);
        }
    }

    Parent_->Increment(-totalDelta);
}

const TProgressCounterPtr& TProgressCounter::Parent() const
{
    return Parent_;
}

void TProgressCounter::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, TotalEnabled_);
    Persist(context, Total_);
    Persist(context, Running_);
    Persist(context, Completed_);
    Persist(context, Pending_);
    Persist(context, Failed_);
    Persist(context, Lost_);
    Persist(context, Aborted_);
    Persist(context, Parent_);
}

////////////////////////////////////////////////////////////////////////////////

const TProgressCounterPtr NullProgressCounter = New<TProgressCounter>();

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TProgressCounterPtr& counter)
{
    return counter->IsTotalEnabled()
        ? Format("{T: %v, R: %v, C: %v, P: %v, F: %v, A: %v, L: %v, I: %v}",
            counter->GetTotal(),
            counter->GetRunning(),
            counter->GetCompletedTotal(),
            counter->GetPending(),
            counter->GetFailed(),
            counter->GetAbortedTotal(),
            counter->GetLost(),
            counter->GetInterruptedTotal())
        : Format("{R: %v, C: %v, F: %v, A: %v, L: %v, I: %v}",
            counter->GetRunning(),
            counter->GetCompletedTotal(),
            counter->GetFailed(),
            counter->GetAbortedTotal(),
            counter->GetLost(),
            counter->GetInterruptedTotal());
}

void Serialize(const TProgressCounterPtr& counter, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(counter->IsTotalEnabled(), [&] (TFluentMap fluent) {
                fluent
                    .Item("total").Value(counter->GetTotal())
                    .Item("pending").Value(counter->GetPending());
            })
            .Item("running").Value(counter->GetRunning())
            .Item("completed").BeginMap()
                .Item("interrupted").BeginMap()
                    .DoFor(TEnumTraits<EInterruptReason>::GetDomainValues(), [&] (TFluentMap fluent, EInterruptReason reason) {
                        if (reason != EInterruptReason::None) {
                            fluent
                                .Item(FormatEnum(reason)).Value(counter->GetCompleted(reason));
                        }
                    })
                .EndMap()
                .Item("non-interrupted").Value(counter->GetCompleted(EInterruptReason::None))
                .Item("total").Value(counter->GetCompletedTotal())
            .EndMap()
            .Item("failed").Value(counter->GetFailed())
            .Item("aborted").BeginMap()
                // NB(ignat): temporaly output total aborted job count as scheduled aborted jobs count.
                // Fix it when UI will start using scheduled aborted job count.
                .Item("total").Value(counter->GetAbortedScheduled())
                //.Item("total").Value(counter->GetAbortedTotal())
                .Item("non_scheduled").BeginMap()
                    .DoFor(TEnumTraits<EAbortReason>::GetDomainValues(), [&] (TFluentMap fluent, EAbortReason reason) {
                        if (IsSchedulingReason(reason) || reason == EAbortReason::GetSpecFailed) {
                            fluent.Item(FormatEnum(reason)).Value(counter->GetAborted(reason));
                        }
                    })
                .EndMap()
                .Item("scheduled").BeginMap()
                    .DoFor(TEnumTraits<EAbortReason>::GetDomainValues(), [&] (TFluentMap fluent, EAbortReason reason) {
                        if (IsNonSchedulingReason(reason) && reason != EAbortReason::GetSpecFailed) {
                            fluent.Item(FormatEnum(reason)).Value(counter->GetAborted(reason));
                        }
                    })
                .EndMap()
            .EndMap()
            .Item("lost").Value(counter->GetLost())
        .EndMap();
}

void SerializeBriefVersion(const TProgressCounterPtr& counter, NYTree::TFluentAny fluent)
{
    fluent
        .BeginMap()
            .DoIf(counter->IsTotalEnabled(), [&] (TFluentMap fluent) {
                fluent
                    .Item("total").Value(counter->GetTotal())
                    .Item("pending").Value(counter->GetPending());
            })
            .Item("running").Value(counter->GetRunning())
            .Item("completed").Value(counter->GetCompletedTotal())
            .Item("failed").Value(counter->GetFailed())
            .Item("aborted").Value(counter->GetAbortedScheduled())
            .Item("lost").Value(counter->GetLost())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

