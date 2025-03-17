#pragma once

#include "task.h"
#include <contrib/ydb/core/tx/columnshard/columnshard_private_events.h>
#include <contrib/ydb/library/accessor/accessor.h>

#include <contrib/ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {

class TEvStartTask: public NActors::TEventLocal<TEvStartTask, NColumnShard::TEvPrivate::EEv::EvStartResourceUsageTask> {
private:
    YDB_READONLY_DEF(std::shared_ptr<ITask>, Task);
public:

    explicit TEvStartTask(std::shared_ptr<ITask> task)
        : Task(task) {
    }

};


}
