#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/event_local.h>
#include <contrib/ydb/library/actors/core/event_pb.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <contrib/ydb/library/yql/dq/actors/task_runner/events.h>
#include <contrib/ydb/library/yql/dq/actors/task_runner/task_runner_actor.h>
#include <contrib/ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <contrib/ydb/library/yql/dq/common/dq_common.h>
#include <contrib/ydb/library/yql/dq/proto/dq_transport.pb.h>
#include <contrib/ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <contrib/ydb/library/yql/providers/dq/task_runner/task_runner_invoker.h>
#include <contrib/ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>

namespace NYql {

struct TWorkerRuntimeData;

namespace NDq {

namespace NTaskRunnerActor {

ITaskRunnerActorFactory::TPtr CreateTaskRunnerActorFactory(
    const NTaskRunnerProxy::IProxyFactory::TPtr& proxyFactory,
    const NDqs::ITaskRunnerInvokerFactory::TPtr& invokerFactory,
    TWorkerRuntimeData* runtimeData = nullptr);

} // namespace NTaskRunnerActor

} // namespace NDq

} // namespace NYql
