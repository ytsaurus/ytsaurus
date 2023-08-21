#pragma once

#include "persistence.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NControllerAgent {

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProgressCategory,
    ((None)               (0))
    ((Running)            (1))
    ((Completed)          (2))
    ((Pending)            (3))
    ((Suspended)          (4))
    ((Invalidated)        (5))
    ((Uncategorized)      (6))
    ((Blocked)            (7)));

///////////////////////////////////////////////////////////////////////////////

//! Represents an abstract numeric progress counter for jobs, chunks, weights etc.
//! Can be a part of counter hierarchy: change in a counter affects its parents, grandparents and so on.
//! Counters have the following semantics:
//! Total corresponds to all the jobs that are still interesting for controller, Total = Running + Completed + Pending + Blocked + Suspended + Uncategorized.
//! Running corresponds to a job that is executing right now.
//! CompletedTotal corresponds to a job that has successfully finished.
//! Interrupted corresponds to a job has been interrupted, i.e. forcefully completed before input processing finish.
//! Interrupted category is contained inside CompletedTotal.
//! Pending corresponds to a job that can be started right now.
//! Suspended corresponds to a job that cannot be started because of unavailable input chunks. After chunks become available
//! job will become pending.
//! Failed corresponds to jobs that did not complete successfully.
//! Aborted corresponds to jobs that were aborted by user, scheduler or node.
//! Lost corresponds to completed jobs whose output was lost and should be recalculated.
//! Invalidated corresponds to completed jobs whose output is not actual anymore.
//! Blocked corresponds to jobs that are not going to be scheduled in typical case, but can be scheduled under certain circumstances.
//! For example, small AutoMerge job is blocked until it will become large enough. It will be scheduled if quota is exhausted.
class TProgressCounter
    : public TRefCounted
{
public:
    TProgressCounter() = default;

    i64 GetTotal() const;
    i64 GetRunning() const;
    i64 GetCompletedTotal() const;
    i64 GetCompleted(EInterruptReason reason) const;
    i64 GetInterruptedTotal() const;
    i64 GetPending() const;
    i64 GetSuspended() const;
    i64 GetFailed() const;
    i64 GetAbortedTotal() const;
    i64 GetAbortedScheduled() const;
    i64 GetAbortedNonScheduled() const;
    i64 GetAborted(EAbortReason reason) const;
    i64 GetLost() const;
    i64 GetInvalidated() const;
    i64 GetUncategorized() const;
    i64 GetBlocked() const;

    void AddRunning(i64 value);
    void AddCompleted(i64 value, EInterruptReason reason = EInterruptReason::None);
    void AddFailed(i64 value);
    void AddPending(i64 value);
    void SetPending(i64 value);
    void AddSuspended(i64 value);
    void SetSuspended(i64 value);
    void AddAborted(i64 value, EAbortReason reason = EAbortReason::Other);
    void AddLost(i64 value);
    void AddInvalidated(i64 value);
    void AddUncategorized(i64 value);
    void AddBlocked(i64 value);
    void SetBlocked(i64 value);

    // NB: this method does not check that counter hierarchy does not contain loops.
    void AddParent(TProgressCounterPtr parent);

    // NB: removes at most one occurrence of parent in parents list.
    bool RemoveParent(TProgressCounterPtr parent);

    void Persist(const TPersistenceContext& context);

    //! Raises when pending counter changes.
    DEFINE_SIGNAL(void(), PendingUpdated);

    //! Raises when blocked counter changes.
    DEFINE_SIGNAL(void(), BlockedUpdated);

private:
    i64 Running_ = 0;
    TEnumIndexedVector<EInterruptReason, i64> Completed_;
    i64 Failed_ = 0;
    i64 Pending_ = 0;
    i64 Suspended_ = 0;
    TEnumIndexedVector<EAbortReason, i64> Aborted_;
    i64 Lost_ = 0;
    i64 Invalidated_ = 0;
    i64 Uncategorized_ = 0;
    i64 Blocked_ = 0;

    std::vector<TProgressCounterPtr> Parents_;

    void Propagate(TProgressCounterPtr parent, int multiplier);
};

DEFINE_REFCOUNTED_TYPE(TProgressCounter)

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TProgressCounterPtr& counter);

void Serialize(const TProgressCounterPtr& counter, NYson::IYsonConsumer* consumer);

void SerializeBriefVersion(const TProgressCounterPtr& counter, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TProgressCounterGuard
{
public:
    TProgressCounterGuard() = default;

    TProgressCounterGuard(TProgressCounterPtr progressCounter, i64 value = 1);

    i64 GetValue() const;

    void SetValue(i64 newValue);

    void UpdateValue(i64 delta);

    void SetCategory(EProgressCategory newState);

    void SetCompletedCategory(EInterruptReason interruptReason);

    void OnFailed();

    void OnAborted(EAbortReason abortReason);

    void OnLost();

    void Unregister();

    void Persist(const TPersistenceContext& context);

private:
    TProgressCounterPtr ProgressCounter_;

    i64 Value_ = -1;

    EProgressCategory Category_ = EProgressCategory::None;

    EInterruptReason InterruptReason_ = EInterruptReason::None;

    void UpdateProgressCounter(i64 multiplier);
};

///////////////////////////////////////////////////////////////////////////////

extern const TProgressCounterPtr NullProgressCounter;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
