#pragma once

#include "task.h"
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yt/yql/providers/dq/control/yql_dq_control.h>
#include <contrib/ydb/library/yql/providers/dq/common/yql_dq_common.h>


namespace NYql {
IYqlRunnableTaskPtr CreateUdfSnapshotHandlerTask(
        TTaskDataPtr taskData,
        ITaskWatcherPtr taskWatcher,
        IMetricsRegistryPtr metrics,
        IDqControlFactoryPtr dqControlFactory,
        TFileStoragePtr fileStorage);
}
