#pragma once

#include <yt/yql/providers/dq/config/config.pb.h>

#include <contrib/ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe.h>

namespace NYql::NDq {

// Returns the copied portoctl path to monitor for file-cache removal, or empty.
TString PrepareTaskRunnerPorto(
    const NProto::TDqConfig::TYtBackend& config,
    const TString& fileCacheDir,
    NTaskRunnerProxy::TPipeFactoryOptions* options,
    const TString& jobRootPortoCtlPath = "/usr/bin/portoctl");

void ConfigureTaskRunnerPorto(
    const NProto::TDqConfig::TYtBackend& config,
    const NTaskRunnerProxy::TPipeFactoryOptions& options);

} // namespace NYql::NDq
