#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/system/spin_lock.h>

#include <set>

namespace NYT::NFlow::NFileParsingExample {

////////////////////////////////////////////////////////////////////////////////

struct TParseExecutorConfig
    : public NYTree::TYsonStruct
{
    //! The number of parses that run at once.
    int SlotCount = 0;
    //! How long a task may wait for a slot before it is rejected.
    TDuration MaxQueueWait;

    REGISTER_YSON_STRUCT(TParseExecutorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TParseExecutorConfig);

////////////////////////////////////////////////////////////////////////////////

//! Parses |task|; runs in a fiber of the executor's invoker, which is canceled with the task.
using TParseCallback = TCallback<TParseResultPtr(const TParseTaskPtr& task)>;

//! Runs the parses of one worker in a fixed number of slots, the oldest files first.
/*!
 *  At most #TParseExecutorConfig::SlotCount parses run at once; the other tasks wait. A freed slot
 *  takes the waiting task with the smallest #TParseTask::FirstSeenAt. A task that does not get a slot
 *  within #TParseExecutorConfig::MaxQueueWait completes with #TParseResult::Rejected and never starts.
 *
 *  Canceling the future of #Submit() drops a waiting task and cancels the fiber of a running parse.
 *  The slot of a running task is freed when its future is set. A parse that does not reach
 *  a cancelation point keeps its slot until it returns, and its result is delivered as is.
 *
 *  Thread affinity: any.
 */
class TParseExecutor
    : public TRefCounted
{
public:
    //! Runs |parse| on |invoker|, which must have a thread per slot if the parses block.
    TParseExecutor(TParseExecutorConfigPtr config, IInvokerPtr invoker, TParseCallback parse);

    TFuture<TParseResultPtr> Submit(TParseTaskPtr task);

private:
    struct TEntry
        : public TRefCounted
    {
        TParseTaskPtr Task;
        //! Orders the tasks with equal #TParseTask::FirstSeenAt by submission.
        i64 SequenceNumber = 0;
        //! When a waiting task is rejected.
        TInstant Deadline;
        //! Set to true when the task gets a slot and to false when it is rejected.
        TPromise<bool> SlotGranted = NewPromise<bool>();
        //! Guarded by #Lock_.
        bool HasSlot = false;
    };

    using TEntryPtr = TIntrusivePtr<TEntry>;

    struct TOldestFirst
    {
        bool operator()(const TEntryPtr& lhs, const TEntryPtr& rhs) const;
    };

    const TParseExecutorConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TParseCallback Parse_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    i64 NextSequenceNumber_ = 0;
    int RunningCount_ = 0;
    std::set<TEntryPtr, TOldestFirst> WaitingEntries_;

    TFuture<TParseResultPtr> OnSlotDecided(const TParseTaskPtr& task, bool granted);
    void OnWaitExpired(const TEntryPtr& entry);
    //! Frees the slot of |entry|, whose future is set, and gives the free slots to the waiting tasks.
    void OnCompleted(const TEntryPtr& entry, const TErrorOr<TParseResultPtr>& result);
};

DEFINE_REFCOUNTED_TYPE(TParseExecutor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileParsingExample
