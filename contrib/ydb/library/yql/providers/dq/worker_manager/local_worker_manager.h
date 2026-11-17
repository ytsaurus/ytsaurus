#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include <contrib/ydb/library/yql/providers/dq/worker_manager/interface/local_worker_manager_options.h>

namespace NYql::NDqs {

    NActors::IActor* CreateLocalWorkerManager(const TLocalWorkerManagerOptions& options);

} // namespace NYql::NDqs
