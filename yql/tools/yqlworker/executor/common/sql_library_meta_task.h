#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include "task.h"

namespace NYql {
IYqlRunnableTaskPtr CreateSqlLibraryMetaTask(
        TTaskDataPtr taskData,
        ITaskWatcherPtr taskWatcher,
        IMetricsRegistryPtr metrics,
        IModuleResolver::TPtr moduleResolver);
}
