#pragma once
#include "abstract.h"
#include "config.h"
#include <contrib/ydb/library/actors/core/event_local.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/conclusion/result.h>
#include <contrib/ydb/core/base/events.h>

namespace NKikimr::NConveyor {

struct TEvExecution {
    enum EEv {
        EvNewTask = EventSpaceBegin(TKikimrEvents::ES_CONVEYOR),
        EvRegisterProcess,
        EvUnregisterProcess,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CONVEYOR), "expected EvEnd < EventSpaceEnd");

    class TEvNewTask: public NActors::TEventLocal<TEvNewTask, EvNewTask> {
    private:
        YDB_READONLY_DEF(ITask::TPtr, Task);
        YDB_READONLY(ui64, ProcessId, 0);
        YDB_READONLY(TMonotonic, ConstructInstant, TMonotonic::Now());
    public:
        TEvNewTask() = default;

        explicit TEvNewTask(ITask::TPtr task);
        explicit TEvNewTask(ITask::TPtr task, const ui64 processId);
    };

    class TEvRegisterProcess: public NActors::TEventLocal<TEvRegisterProcess, EvRegisterProcess> {
    private:
        YDB_READONLY(ui64, ProcessId, 0);
        YDB_READONLY_DEF(TCPULimitsConfig, CPULimits);
    public:
        explicit TEvRegisterProcess(const ui64 processId, const TCPULimitsConfig& cpuLimits)
            : ProcessId(processId)
            , CPULimits(cpuLimits) {
        }

    };

    class TEvUnregisterProcess: public NActors::TEventLocal<TEvUnregisterProcess, EvUnregisterProcess> {
    private:
        YDB_READONLY(ui64, ProcessId, 0);
    public:
        explicit TEvUnregisterProcess(const ui64 processId)
            : ProcessId(processId) {
        }

    };
};

}
