#pragma once

#include "task.h"
#include <yql/essentials/core/url_preprocessing/interface/url_preprocessing.h>
#include <yql/essentials/core/yql_udf_resolver.h>

namespace NYql {
IYqlRunnableTaskPtr CreateUdfLibraryMetaTask(
        TTaskDataPtr taskData,
        ITaskWatcherPtr taskWatcher,
        IMetricsRegistryPtr metrics,
        IUdfResolver::TPtr udfResolver,
        TFileStoragePtr fileStorage,
        IUrlPreprocessing::TPtr urlPreprocessing
        );
}
