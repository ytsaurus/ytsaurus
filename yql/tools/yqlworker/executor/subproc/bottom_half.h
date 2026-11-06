#pragma once

#include <yql/tools/yqlworker/config/config.h>
#include <yql/tools/yqlworker/misc/duplex_ipc_channel.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>

#include <util/generic/ptr.h>


namespace NYql {
namespace NSubprocessTaskExecutor {

// this signal have default handler which terminates process
// and will not create coredump
const int ABORT_SIGNAL = SIGINT;

class TForkerProcess
{
public:
    TForkerProcess(const TMainConfig& config,
        const TMaybe<NProto::TDqConfig>& dqConfig,
        TIpcChannelEndpoint& coreProcessCh,
        IMetricsRegistryPtr metrics);
    ~TForkerProcess();

    int Run();

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NSubprocessTaskExecutor
} // namespace NYql
