#include "stdafx.h"
#include "progress_counter.h"
#include "private.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;

////////////////////////////////////////////////////////////////////

TProgressCounter::TProgressCounter()
    : Total_(0)
    , Running_(0)
    , Completed_(0)
    , Pending_(0)
    , Failed_(0)
{ }

void TProgressCounter::Set(i64 value)
{
    Total_ = value;
    Pending_ = value;
}

void TProgressCounter::Increment(i64 value)
{
    Total_ += value;
    Pending_ += value;
}

i64 TProgressCounter::GetTotal() const
{
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
    return Pending_;
}

i64 TProgressCounter::GetFailed() const
{
    return Failed_;
}

void TProgressCounter::Start(i64 count)
{
    YASSERT(Pending_ >= count);
    Running_ += count;
    Pending_ -= count;
}

void TProgressCounter::Completed(i64 count)
{
    YASSERT(Running_ >= count);
    Running_ -= count;
    Completed_ += count;
}

void TProgressCounter::Failed(i64 count)
{
    YASSERT(Running_ >= count);
    Running_ -= count;
    Pending_ += count;
    Failed_ += count;
}

void TProgressCounter::ToYson(NYTree::IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").Scalar(Total_)
            .Item("running").Scalar(Running_)
            .Item("completed").Scalar(Completed_)
            .Item("pending").Scalar(Pending_)
            .Item("failed").Scalar(Failed_)
        .EndMap();
}

Stroka ToString(const TProgressCounter& counter)
{
    return Sprintf("T: %" PRId64 ", R: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 ", F: %" PRId64,
        counter.GetTotal(),
        counter.GetRunning(),
        counter.GetCompleted(),
        counter.GetPending(),
        counter.GetFailed());
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

