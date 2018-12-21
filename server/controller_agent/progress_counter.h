#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/core/yson/public.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! Represents an abstract numeric progress counter for jobs, chunks, weights etc.
//! Can be a part of counter hierarchy: change in a counter affects its parent, grandparent and so on.
class TProgressCounter
    : public TRefCounted
{
public:
    TProgressCounter();
    explicit TProgressCounter(i64 total);

    void Set(i64 total);
    bool IsTotalEnabled() const;

    void Decrement(i64 value);
    void Increment(i64 value);

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

    // NB: this method does not check that counter hierarchy does not contain loops.
    void SetParent(const TProgressCounterPtr& parent);
    const TProgressCounterPtr& Parent() const;

    void Persist(const TPersistenceContext& context);

private:
    bool TotalEnabled_;
    i64 Total_;
    i64 Running_;
    TEnumIndexedVector<i64, EInterruptReason> Completed_;
    i64 Pending_;
    i64 Failed_;
    i64 Lost_;
    TEnumIndexedVector<i64, EAbortReason> Aborted_;
    TProgressCounterPtr Parent_;
};

DEFINE_REFCOUNTED_TYPE(TProgressCounter)

TString ToString(const TProgressCounterPtr& counter);

void Serialize(const TProgressCounterPtr& counter, NYson::IYsonConsumer* consumer);

void SerializeBriefVersion(const TProgressCounterPtr& counter, NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

extern const TProgressCounterPtr NullProgressCounter;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
