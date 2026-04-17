#include "program.h"

#include "bootstrap.h"

#include <yt/yt/server/lib/cypress_proxy/config.h>

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyProgram
    : public TServerProgram<TCypressProxyProgramConfig>
{
public:
    TCypressProxyProgram()
    {
        SetMainThreadName("CypProxyProg");
    }

protected:
    void DoStart() final
    {
        auto bootstrap = CreateCypressProxyBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .BlockingGet()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunCypressProxyProgram(int argc, const char** argv)
{
    TCypressProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
