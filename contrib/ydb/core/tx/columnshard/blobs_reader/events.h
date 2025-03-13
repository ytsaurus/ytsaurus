#pragma once

#include <contrib/ydb/core/tx/columnshard/columnshard_private_events.h>
#include <contrib/ydb/library/accessor/accessor.h>

#include <contrib/ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class ITask;

class TEvStartReadTask: public NActors::TEventLocal<TEvStartReadTask, NColumnShard::TEvPrivate::EEv::EvStartReadTask> {
private:
    YDB_READONLY_DEF(std::shared_ptr<ITask>, Task);
public:

    explicit TEvStartReadTask(std::shared_ptr<ITask> task)
        : Task(task) {
    }

};


}
