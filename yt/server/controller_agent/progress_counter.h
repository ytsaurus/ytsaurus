#pragma once

#include "public.h"

#include <yt/core/yson/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! Represents an abstract numeric progress counter for jobs, chunks, weights etc.
class TProgressCounter
{
public:
    TProgressCounter();
    explicit TProgressCounter(i64 total);

    void Set(i64 total);
    bool IsTotalEnabled() const;

    void Increment(i64 value);
    void Finalize();

    i64 GetTotal() const;
    i64 GetRunning() const;
    i64 GetCompletedTotal() const;
    i64 GetCompleted(EInterruptReason reason) const;
    i64 GetInterruptedTotal() const;
    i64 GetPending() const;
    i64 GetFailed() const;
    i64 GetAbortedTotal() const;
    i64 GetAbortedScheduled() const;
    i64 GetAbortedNonScheduled() const;
    i64 GetAborted(EAbortReason reason) const;
    i64 GetLost() const;

    void Start(i64 count);
    void Completed(i64 count, EInterruptReason reason = EInterruptReason::None);
    void Failed(i64 count);
    void Aborted(i64 count, EAbortReason reason = EAbortReason::Other);
    void Lost(i64 count);

    void Persist(const TStreamPersistenceContext& context);

private:
    bool TotalEnabled_;
    i64 Total_;
    i64 Running_;
    TEnumIndexedVector<i64, EInterruptReason> Completed_;
    i64 Pending_;
    i64 Failed_;
    i64 Lost_;
    TEnumIndexedVector<i64, EAbortReason> Aborted_;
};

TString ToString(const TProgressCounter& counter);

void Serialize(const TProgressCounter& counter, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

extern const TProgressCounter NullProgressCounter;

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
