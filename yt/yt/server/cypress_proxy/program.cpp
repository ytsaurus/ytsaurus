#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyProgram
    : public TServerProgram<TCypressProxyProgramConfig>
{
public:
    TCypressProxyProgram()
    {
        SetMainThreadName("CypressProxyProg");
    }

protected:
    void DoStart() final
    {
        auto bootstrap = CreateCypressProxyBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
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
