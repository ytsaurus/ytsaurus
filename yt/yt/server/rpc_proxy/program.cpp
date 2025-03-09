#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/core/logging/config.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TProxyProgram
    : public TServerProgram<NRpcProxy::TProxyProgramConfig>
{
public:
    TProxyProgram()
    {
        SetMainThreadName("RpcProxyProg");
    }

private:
    void DoStart() final
    {
        // TODO(babenko): refactor
        ConfigureAllocator({.SnapshotUpdatePeriod = GetConfig()->HeapProfiler->SnapshotUpdatePeriod});

        auto bootstrap = CreateRpcProxyBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunRpcProxyProgram(int argc, const char** argv)
{
    TProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
