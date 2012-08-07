#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Represents an abstract numeric progress counter for jobs, chunks, weights etc.
class TProgressCounter
{
public:
    explicit TProgressCounter(bool totalEnabled = true);

    bool IsTotalEnabled() const;

    void Set(i64 value);
    void Increment(i64 value);
    void Finalize();

    i64 GetTotal() const;
    i64 GetRunning() const;
    i64 GetCompleted() const;
    i64 GetPending() const;
    i64 GetFailed() const;

    void Start(i64 count);
    void Completed(i64 count);
    void Failed(i64 count);

    void ToYson(NYTree::IYsonConsumer* consumer) const;

private:
    bool TotalEnabled;
    i64 Total_;
    i64 Running_;
    i64 Completed_;
    i64 Pending_;
    i64 Failed_;

};

Stroka ToString(const TProgressCounter& counter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
