#pragma once

#include <yql/tools/yqlworker/config/config.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#ifdef STATUS_PENDING
#undef STATUS_PENDING
#endif
#include <yql/tools/yqlworker/interface/proto/task.pb.h>

#include <library/cpp/messagebus/handler.h>
#include <util/generic/ptr.h>
#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>


namespace NYql {

using TTaskDataPtr = std::shared_ptr<NProto::TTaskData>;
using TTaskResultPtr = std::shared_ptr<NProto::TTaskResult>;
using TTaskProgressPtr = std::shared_ptr<NProto::TTaskProgress>;

struct TTaskResultChanges {
    using TMask = ui32;

    enum {
        // status change
        STATUS_IDLE = NProto::ETaskResultChange::STATUS_IDLE,
        STATUS_PENDING = NProto::ETaskResultChange::STATUS_PENDING,
        STATUS_RUNNING = NProto::ETaskResultChange::STATUS_RUNNING,
        STATUS_COMPLETED = NProto::ETaskResultChange::STATUS_COMPLETED,
        STATUS_ABORTED = NProto::ETaskResultChange::STATUS_ABORTED,
        STATUS_ERROR = NProto::ETaskResultChange::STATUS_ERROR,

        // mask to get status from changes
        STATUS_MASK = STATUS_IDLE | STATUS_PENDING | STATUS_RUNNING |
                      STATUS_COMPLETED | STATUS_ABORTED | STATUS_ERROR,

        // properties changes
        AST = NProto::ETaskResultChange::AST,
        PLAN = NProto::ETaskResultChange::PLAN,
        RESULTS = NProto::ETaskResultChange::RESULTS,
        ERRORS = NProto::ETaskResultChange::ERRORS,
        PROGRESS = NProto::ETaskResultChange::PROGRESS,
        REVISION = NProto::ETaskResultChange::REVISION,
        STATISTICS = NProto::ETaskResultChange::STATISTICS,

        // mask to get data from changes
        DATA_MASK = AST | PLAN | RESULTS | ERRORS,

        // max value
        MAX_VALUE = NProto::ETaskResultChange::MAX_VALUE
    };

    static bool IsAnyStatus(TMask mask) {
        return (mask & STATUS_MASK) != 0;
    }

    static bool IsAnyData(TMask mask) {
        return (mask & DATA_MASK) != 0;
    }

    static TMask FromStatus(NProto::ETaskStatus status) {
        switch (status) {
        case NProto::ETaskStatus::IDLE: return STATUS_IDLE;
        case NProto::ETaskStatus::PENDING: return STATUS_PENDING;
        case NProto::ETaskStatus::RUNNING: return STATUS_RUNNING;
        case NProto::ETaskStatus::COMPLETED: return STATUS_COMPLETED;
        case NProto::ETaskStatus::ABORTED: return STATUS_ABORTED;
        case NProto::ETaskStatus::ERROR: return STATUS_ERROR;
        default:
            ythrow yexception() << "unknown task status: "
                                << static_cast<int>(status);
        }
    }

    static void Copy(NProto::TTaskResult* to, const NProto::TTaskResult& from,
                     TMask changesMask);

    explicit TTaskResultChanges(TMask mask) : Mask(mask) {}

public:
    TMask Mask;
};

//////////////////////////////////////////////////////////////////////////////
// ITaskWatcher
//////////////////////////////////////////////////////////////////////////////
struct ITaskWatcher: public TAtomicRefCount<ITaskWatcher> {
    virtual ~ITaskWatcher() = default;

    virtual void OnStart() = 0;

    virtual void OnFinish(NProto::ETaskStatus status) = 0;

    virtual void OnResultChange(
            TTaskResultChanges::TMask changesMask,
            TTaskResultPtr changes,
            int workerPid = 0,
            NBus::TOnMessageContext* taskWatchCtx = nullptr) = 0;

    virtual bool WillWatch(ui32 changesMask) const = 0;
};

using ITaskWatcherPtr = TIntrusivePtr<ITaskWatcher>;

//////////////////////////////////////////////////////////////////////////////
// IYqlRunnableTask
//////////////////////////////////////////////////////////////////////////////
class IYqlRunnableTask: public TThrRefBase {
    enum EState {
        IDLE = 0,
        RUNNED = 1,
        ABORTED = 2,
        DONE = 3,
    };

public:
    virtual ~IYqlRunnableTask() = default;

    bool Run() {
        if (AtomicCas(&State_, RUNNED, IDLE)) {
            bool isOk = DoRun();
            AtomicCas(&State_, DONE, RUNNED);
            return isOk;
        }
        return false;
    }

    void Abort() {
        if (AtomicCas(&State_, ABORTED, RUNNED)) {
            DoAbort();
        }
    }

    virtual const NProto::TTaskResult& GetResult() const = 0;
    virtual ITaskWatcherPtr GetWatcher() const = 0;

    bool IsIdle() const { return AtomicGet(State_) == IDLE; }
    bool IsRunned() const { return AtomicGet(State_) == RUNNED; }
    bool IsAborted() const { return AtomicGet(State_) == ABORTED; }
    bool IsDone() const { return AtomicGet(State_) == DONE; }

private:
    virtual bool DoRun() = 0;
    virtual void DoAbort() = 0;

private:
    TAtomic State_ = IDLE;
};

using IYqlRunnableTaskPtr = TIntrusivePtr<IYqlRunnableTask>;

void AddErrorMessageToTaskResult(NProto::TTaskResult* taskResult, const TString& str);
void AddFatalMessageToTaskResult(NProto::TTaskResult* taskResult, const TString& str);

}

template <>
void Out<NYql::TTaskResultChanges>(IOutputStream& out, const NYql::TTaskResultChanges& changes);
