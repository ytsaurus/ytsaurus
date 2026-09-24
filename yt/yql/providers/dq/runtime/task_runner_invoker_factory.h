#pragma once

#include <util/generic/ptr.h>

#include <functional>

#include <contrib/ydb/library/yql/providers/dq/task_runner/task_runner_invoker.h>

namespace NYql::NDqs {

ITaskRunnerInvokerFactory::TPtr CreateConcurrentInvokerFactory(int capacity);

} // namespace NYql::NDqs
