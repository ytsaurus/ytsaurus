#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyProgram
    : public TServerProgram<TCypressProxyConfig>
{
public:
    TCypressProxyProgram()
    {
        SetMainThreadName("CypressProxy");
    }

protected:
    void DoStart() final
    {
        auto* bootstrap = CreateBootstrap(GetConfig()).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();
        bootstrap->Run();
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
