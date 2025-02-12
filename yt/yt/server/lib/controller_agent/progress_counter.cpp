#include "progress_counter.h"

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NControllerAgent {

using namespace NYTree;
using namespace NYson;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

i64 TProgressCounter::GetTotal() const
{
    return
        GetRunning() +
        GetCompletedTotal() +
        GetPending() +
        GetBlocked() +
        GetSuspended() +
        GetUncategorized();
}

i64 TProgressCounter::GetRunning() const
{
    return Running_;
}

i64 TProgressCounter::GetCompletedTotal() const
{
    return std::accumulate(Completed_.begin(), Completed_.end(), 0LL);
}

i64 TProgressCounter::GetCompleted(EInterruptionReason reason) const
{
    return Completed_[reason];
}

i64 TProgressCounter::GetInterruptedTotal() const
{
    return GetCompletedTotal() - Completed_[EInterruptionReason::None];
}

i64 TProgressCounter::GetPending() const
{
    return Pending_;
}

i64 TProgressCounter::GetSuspended() const
{
    return Suspended_;
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
        if (NScheduler::IsSchedulingReason(reason)) {
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

i64 TProgressCounter::GetInvalidated() const
{
    return Invalidated_;
}

i64 TProgressCounter::GetUncategorized() const
{
    return Uncategorized_;
}

i64 TProgressCounter::GetBlocked() const
{
    return Blocked_;
}

void TProgressCounter::AddRunning(i64 value)
{
    Running_ += value;
    for (const auto& parent : Parents_) {
        parent->AddRunning(value);
    }
}

void TProgressCounter::AddCompleted(i64 value, EInterruptionReason reason)
{
    Completed_[reason] += value;
    for (const auto& parent : Parents_) {
        parent->AddCompleted(value, reason);
    }
}

void TProgressCounter::AddFailed(i64 value)
{
    Failed_ += value;
    for (const auto& parent : Parents_) {
        parent->AddFailed(value);
    }
}

void TProgressCounter::AddPending(i64 value)
{
    Pending_ += value;
    for (const auto& parent : Parents_) {
        parent->AddPending(value);
    }
    PendingUpdated_.Fire();
}

void TProgressCounter::SetPending(i64 value)
{
    AddPending(value - Pending_);
}

void TProgressCounter::AddSuspended(i64 value)
{
    Suspended_ += value;
    for (const auto& parent : Parents_) {
        parent->AddSuspended(value);
    }
}

void TProgressCounter::SetSuspended(i64 value)
{
    AddSuspended(value - Suspended_);
}

void TProgressCounter::AddAborted(i64 value, EAbortReason reason)
{
    Aborted_[reason] += value;
    for (const auto& parent : Parents_) {
        parent->AddAborted(value, reason);
    }
}

void TProgressCounter::AddLost(i64 value)
{
    Lost_ += value;
    for (const auto& parent : Parents_) {
        parent->AddLost(value);
    }
}

void TProgressCounter::AddInvalidated(i64 value)
{
    Invalidated_ += value;
    for (const auto& parent : Parents_) {
        parent->AddInvalidated(value);
    }
}

void TProgressCounter::AddUncategorized(i64 value)
{
    Uncategorized_ += value;
    for (const auto& parent : Parents_) {
        parent->AddUncategorized(value);
    }
}

void TProgressCounter::AddBlocked(i64 value)
{
    Blocked_ += value;
    for (const auto& parent : Parents_) {
        parent->AddBlocked(value);
    }
    BlockedUpdated_.Fire();
}

void TProgressCounter::SetBlocked(i64 value)
{
    AddBlocked(value - Blocked_);
}

void TProgressCounter::AddParent(TProgressCounterPtr parent)
{
    Propagate(parent, +1);

    Parents_.push_back(std::move(parent));
}

bool TProgressCounter::RemoveParent(TProgressCounterPtr parent)
{
    auto parentIt = std::find(Parents_.begin(), Parents_.end(), parent);
    if (parentIt == Parents_.end()) {
        return false;
    }

    Propagate(parent, -1);

    Parents_.erase(parentIt);

    return true;
}

void TProgressCounter::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Running_);
    PHOENIX_REGISTER_FIELD(2, Completed_);
    PHOENIX_REGISTER_FIELD(3, Failed_);
    PHOENIX_REGISTER_FIELD(4, Pending_);
    PHOENIX_REGISTER_FIELD(5, Suspended_);
    PHOENIX_REGISTER_FIELD(6, Aborted_);
    PHOENIX_REGISTER_FIELD(7, Lost_);
    PHOENIX_REGISTER_FIELD(8, Invalidated_);
    PHOENIX_REGISTER_FIELD(9, Uncategorized_);
    PHOENIX_REGISTER_FIELD(10, Blocked_);
    PHOENIX_REGISTER_FIELD(11, Parents_);
}

void TProgressCounter::Propagate(TProgressCounterPtr parent, int multiplier)
{
    parent->AddRunning(Running_ * multiplier);
    for (auto reason : TEnumTraits<EInterruptionReason>::GetDomainValues()) {
        parent->AddCompleted(Completed_[reason] * multiplier, reason);
    }
    parent->AddFailed(Failed_ * multiplier);
    parent->AddPending(Pending_ * multiplier);
    parent->AddSuspended(Suspended_ * multiplier);
    for (auto reason : TEnumTraits<EAbortReason>::GetDomainValues()) {
        parent->AddAborted(Aborted_[reason] * multiplier, reason);
    }
    parent->AddLost(Lost_ * multiplier);
    parent->AddInvalidated(Invalidated_ * multiplier);
    parent->AddUncategorized(Uncategorized_ * multiplier);
    parent->AddBlocked(Blocked_ * multiplier);
}

PHOENIX_DEFINE_TYPE(TProgressCounter);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TProgressCounterPtr& counter, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{T: %v, R: %v, C: %v, F: %v, P: %v, S: %v, A: %v, L: %v, I: %v, B: %v}",
        counter->GetTotal(),
        counter->GetRunning(),
        counter->GetCompletedTotal(),
        counter->GetFailed(),
        counter->GetPending(),
        counter->GetSuspended(),
        counter->GetAbortedTotal(),
        counter->GetLost(),
        counter->GetInvalidated(),
        counter->GetBlocked());
}

void Serialize(const TProgressCounterPtr& counter, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").Value(counter->GetTotal())
            .Item("running").Value(counter->GetRunning())
            .Item("completed").BeginMap()
                .Item("interrupted").BeginMap()
                    .DoFor(TEnumTraits<EInterruptionReason>::GetDomainValues(), [&] (TFluentMap fluent, EInterruptionReason reason) {
                        if (reason != EInterruptionReason::None) {
                            fluent
                                .Item(FormatEnum(reason)).Value(counter->GetCompleted(reason));
                        }
                    })
                .EndMap()
                .Item("non-interrupted").Value(counter->GetCompleted(EInterruptionReason::None))
                .Item("total").Value(counter->GetCompletedTotal())
            .EndMap()
            .Item("failed").Value(counter->GetFailed())
            .Item("pending").Value(counter->GetPending())
            .Item("suspended").Value(counter->GetSuspended())
            .Item("aborted").BeginMap()
                .Item("total").Value(counter->GetAbortedTotal())
                .Item("non_scheduled").BeginMap()
                    .DoFor(TEnumTraits<EAbortReason>::GetDomainValues(), [&] (TFluentMap fluent, EAbortReason reason) {
                        if (WasAbortedAfterStart(reason)) {
                            fluent.Item(FormatEnum(reason)).Value(counter->GetAborted(reason));
                        }
                    })
                .EndMap()
                .Item("scheduled").BeginMap()
                    .DoFor(TEnumTraits<EAbortReason>::GetDomainValues(), [&] (TFluentMap fluent, EAbortReason reason) {
                        if (!WasAbortedAfterStart(reason)) {
                            fluent.Item(FormatEnum(reason)).Value(counter->GetAborted(reason));
                        }
                    })
                .EndMap()
            .EndMap()
            .Item("lost").Value(counter->GetLost())
            .Item("invalidated").Value(counter->GetInvalidated())
            .Item("blocked").Value(counter->GetBlocked())
        .EndMap();
}

void SerializeBriefVersion(const TProgressCounterPtr& counter, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").Value(counter->GetTotal())
            .Item("running").Value(counter->GetRunning())
            .Item("completed").Value(counter->GetCompletedTotal())
            .Item("failed").Value(counter->GetFailed())
            .Item("pending").Value(counter->GetPending())
            .Item("suspended").Value(counter->GetSuspended())
            .Item("aborted").Value(counter->GetAbortedScheduled())
            .Item("lost").Value(counter->GetLost())
            .Item("invalidated").Value(counter->GetInvalidated())
            .Item("blocked").Value(counter->GetBlocked())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TProgressCounterGuard::TProgressCounterGuard(TProgressCounterPtr progressCounter, i64 value)
    : ProgressCounter_(std::move(progressCounter))
    , Value_(value)
{
    UpdateProgressCounter(+1);
}

i64 TProgressCounterGuard::GetValue() const
{
    return Value_;
}

void TProgressCounterGuard::SetValue(i64 newValue)
{
    UpdateProgressCounter(-1);
    Value_ = newValue;
    UpdateProgressCounter(+1);
}

void TProgressCounterGuard::UpdateValue(i64 delta)
{
    auto newValue = Value_ + delta;
    SetValue(newValue);
}

void TProgressCounterGuard::SetCategory(EProgressCategory newCategory)
{
    UpdateProgressCounter(-1);
    Category_ = newCategory;
    UpdateProgressCounter(+1);
}

void TProgressCounterGuard::SetCompletedCategory(EInterruptionReason interruptionReason)
{
    UpdateProgressCounter(-1);
    Category_ = EProgressCategory::Completed;
    InterruptionReason_ = interruptionReason;
    UpdateProgressCounter(+1);
}

void TProgressCounterGuard::OnFailed()
{
    ProgressCounter_->AddFailed(+1);
}

void TProgressCounterGuard::OnAborted(EAbortReason abortReason)
{
    ProgressCounter_->AddAborted(+1, abortReason);
}

void TProgressCounterGuard::OnLost()
{
    ProgressCounter_->AddLost(+1);
}

void TProgressCounterGuard::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, ProgressCounter_);
    PHOENIX_REGISTER_FIELD(2, Value_);
    PHOENIX_REGISTER_FIELD(3, Category_);
    PHOENIX_REGISTER_FIELD(4, InterruptionReason_);
}

void TProgressCounterGuard::UpdateProgressCounter(i64 multiplier)
{
    if (Category_ != EProgressCategory::None) {
        YT_VERIFY(ProgressCounter_);
    }

    auto value = multiplier * Value_;
    switch (Category_) {
        case EProgressCategory::None:
            break;
        case EProgressCategory::Running:
            ProgressCounter_->AddRunning(value);
            break;
        case EProgressCategory::Completed:
            ProgressCounter_->AddCompleted(value, InterruptionReason_);
            break;
        case EProgressCategory::Pending:
            ProgressCounter_->AddPending(value);
            break;
        case EProgressCategory::Suspended:
            ProgressCounter_->AddSuspended(value);
            break;
        case EProgressCategory::Invalidated:
            ProgressCounter_->AddInvalidated(value);
            break;
        case EProgressCategory::Blocked:
            ProgressCounter_->AddBlocked(value);
            break;
        default:
            YT_ABORT();
    }
}

PHOENIX_DEFINE_TYPE(TProgressCounterGuard);

////////////////////////////////////////////////////////////////////////////////

const TProgressCounterPtr NullProgressCounter = New<TProgressCounter>();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

