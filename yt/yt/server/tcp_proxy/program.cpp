#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

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
        auto config = GetConfig();

        ConfigureSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = CreateBootstrap(std::move(config)).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunTcpProxyProgram(int argc, const char** argv)
{
    TTcpProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
