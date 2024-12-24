#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NTcpProxy {

////////////////////////////////////////////////////////////////////////////////

class TTcpProxyProgram
    : public TServerProgram<TTcpProxyConfig>
{
public:
    TTcpProxyProgram()
    {
        SetMainThreadName("TcpProxy");
    }

private:
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

void RunTcpProxyProgram(int argc, const char** argv)
{
    TTcpProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
