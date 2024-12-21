#include "program.h"

#include "private.h"
#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/core/logging/config.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyProgram
    : public TServerProgram<NRpcProxy::TProxyConfig>
{
public:
    TRpcProxyProgram()
    {
        SetMainThreadName("RpcProxy");
    }

private:
    void DoStart() final
    {
        // TODO(babenko): refactor
        ConfigureAllocator({.SnapshotUpdatePeriod = GetConfig()->HeapProfiler->SnapshotUpdatePeriod});

        auto* bootstrap = new NRpcProxy::TBootstrap(GetConfig(), GetConfigNode());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunRpcProxyProgram(int argc, const char** argv)
{
    TRpcProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
