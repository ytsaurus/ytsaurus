#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NTcpProxy {

////////////////////////////////////////////////////////////////////////////////

class TProxyProgram
    : public TServerProgram<TProxyProgramConfig>
{
public:
    TProxyProgram()
    {
        SetMainThreadName("TcpProxyProg");
    }

private:
    void DoStart() final
    {
        auto bootstrap = CreateTcpProxyBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunTcpProxyProgram(int argc, const char** argv)
{
    TProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
