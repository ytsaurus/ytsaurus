#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

class TOffshoreNodeProxyProgram
    : public TServerProgram<TOffshoreNodeProxyProgramConfig>
{
public:
    TOffshoreNodeProxyProgram()
    {
        SetMainThreadName("OffshoreNodeProxyProg");
    }

private:
    void DoStart() final
    {
        auto bootstrap = CreateOffshoreNodeProxyBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunOffshoreNodeProxyProgram(int argc, const char** argv)
{
    TOffshoreNodeProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
