#pragma once

#include "public.h"

#include <core/misc/serialize.h>

#include <core/yson/public.h>

namespace NYT {
namespace NScheduler {

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
    i64 GetCompleted() const;
    i64 GetPending() const;
    i64 GetFailed() const;
    i64 GetAborted() const;
    i64 GetAborted(EAbortReason reason) const;
    i64 GetLost() const;

    void Start(i64 count);
    void Completed(i64 count);
    void Failed(i64 count);
    void Aborted(i64 count, EAbortReason reason = EAbortReason::Other);
    void Lost(i64 count);

    void Persist(TStreamPersistenceContext& context);

private:
    bool TotalEnabled_;
    i64 Total_;
    i64 Running_;
    i64 Completed_;
    i64 Pending_;
    i64 Failed_;
    i64 Lost_;
    TEnumIndexedVector<i64, EAbortReason> Aborted_;

};

Stroka ToString(const TProgressCounter& counter);

void Serialize(const TProgressCounter& counter, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
