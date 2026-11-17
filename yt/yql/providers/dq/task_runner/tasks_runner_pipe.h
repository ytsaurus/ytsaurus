#pragma once

#include "file_cache.h"
#include "local_job_proxy.h"

#include <contrib/ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYql::NTaskRunnerProxy {

extern const TString WorkingDirectoryParamName;
extern const TString WorkingDirectoryDontInitParamName; // COMPAT(aozeritsky)
extern const TString UseMetaParamName; // COMPAT(aozeritsky)

i64 SaveRopeToPipe(IOutputStream& output, const TChunkedBuffer& rope);
void LoadRopeFromPipe(IInputStream& input, TChunkedBuffer& rope);

struct TPipeFactoryOptions {
    TString ExecPath;
    IFileCache::TPtr FileCache;
    THashMap<TString, TString> Env;
    TMaybe<TLocalJobProxySettings> LocalJobProxy;
    bool EnablePorto = false;
    bool EnablePortoAnonLimitRaiseBeforeDestroy = true;
    TString PortoLayer;
    bool UseJobRoot = false;
    int MaxProcesses = 1;
    TString ContainerName;
    TString PortoCtlPath = "/usr/bin/porto";
    TMaybe<TString> Revision; // revision override for tests
    NMonitoring::TDynamicCounterPtr Counters;
};

IProxyFactory::TPtr CreatePipeFactory(const TPipeFactoryOptions& options);

} // namespace NYql::NTaskRunnerProxy
