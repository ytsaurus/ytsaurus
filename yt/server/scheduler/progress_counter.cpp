#include "stdafx.h"
#include "progress_counter.h"
#include "private.h"

#include <core/ytree/fluent.h>

#include <numeric>

#include <numeric>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////

TProgressCounter::TProgressCounter()
    : TotalEnabled_(false)
    , Total_(0)
    , Running_(0)
    , Completed_(0)
    , Pending_(0)
    , Failed_(0)
    , Lost_(0)
    , Aborted_(EAbortReason::GetDomainSize(), 0)
{ }

TProgressCounter::TProgressCounter(i64 total)
    : Aborted_(EAbortReason::GetDomainSize(), 0)
{

    Set(total);
}

void TProgressCounter::Set(i64 total)
{
    YCHECK(total >= 0);
    TotalEnabled_ = true;
    Total_ = total;
    Running_ = 0;
    Completed_ = 0;
    Pending_ = total;
    Failed_ = 0;
    Lost_ = 0;

    std::fill(Aborted_.begin(), Aborted_.end(), 0);
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

i64 TProgressCounter::GetCompleted() const
{
    return Completed_;
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

i64 TProgressCounter::GetAborted() const
{
    return std::accumulate(Aborted_.begin(), Aborted_.end(), 0LL);
}

i64 TProgressCounter::GetAborted(EAbortReason reason) const
{
    return Aborted_[static_cast<int>(reason)];
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
}

void TProgressCounter::Completed(i64 count)
{
    YCHECK(Running_ >= count);
    Running_ -= count;
    Completed_ += count;
}

void TProgressCounter::Failed(i64 count)
{
    YCHECK(Running_ >= count);
    Running_ -= count;
    Failed_ += count;
    if (TotalEnabled_) {
        Pending_ += count;
    }
}

void TProgressCounter::Aborted(i64 count, EAbortReason reason)
{
    YCHECK(Running_ >= count);
    Running_ -= count;
    Aborted_[static_cast<int>(reason)] += count;
    if (TotalEnabled_) {
        Pending_ += count;
    }
}

void TProgressCounter::Lost(i64 count)
{
    YCHECK(Completed_ >= count);
    Completed_ -= count;
    Lost_ += count;
    if (TotalEnabled_) {
        Pending_ += count;
    }
}

void TProgressCounter::Finalize()
{
    if (TotalEnabled_) {
        Total_ = Completed_;
        Pending_ = 0;
        Running_ = 0;
    }
}

void TProgressCounter::Persist(TStreamPersistenceContext& context)
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
}

////////////////////////////////////////////////////////////////////

Stroka ToString(const TProgressCounter& counter)
{
    return
        counter.IsTotalEnabled()
        ? Sprintf("T: %" PRId64 ", R: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 ", F: %" PRId64 ", A: %" PRId64 ", L: %" PRId64,
        counter.GetTotal(),
        counter.GetRunning(),
        counter.GetCompleted(),
        counter.GetPending(),
        counter.GetFailed(),
        counter.GetAborted(),
        counter.GetLost())
        : Sprintf("R: %" PRId64 ", C: %" PRId64 ", F: %" PRId64 ", A: %" PRId64 ", L: %" PRId64,
        counter.GetRunning(),
        counter.GetCompleted(),
        counter.GetFailed(),
        counter.GetAborted(),
        counter.GetLost());
}

void Serialize(const TProgressCounter& counter, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(counter.IsTotalEnabled(), [&] (TFluentMap fluent) {
                fluent
                    .Item("total").Value(counter.GetTotal())
                    .Item("pending").Value(counter.GetPending());
            })
            .Item("running").Value(counter.GetRunning())
            .Item("completed").Value(counter.GetCompleted())
            .Item("failed").Value(counter.GetFailed())
            .Item("aborted").BeginMap()
                .Item("total").Value(counter.GetAborted())
                .DoFor(EAbortReason::GetDomainValues(), [&] (TFluentMap fluent, int value) {
                    auto abortReason = EAbortReason(value);
                    fluent
                        .Item(FormatEnum(abortReason))
                        .Value(counter.GetAborted(abortReason));
                })
            .EndMap()
            .Item("lost").Value(counter.GetLost())
        .EndMap();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

