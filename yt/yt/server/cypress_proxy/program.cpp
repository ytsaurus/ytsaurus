#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/program_config_mixin.h>

#include <yt/yt/ytlib/program/native_singletons.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyProgram
    : public TServerProgram
    , public TProgramConfigMixin<TCypressProxyConfig>
{
public:
    TCypressProxyProgram()
        : TProgramConfigMixin(Opts_)
    {
        SetMainThreadName("CypressProxy");
    }

protected:
    void DoStart() final
    {
        auto config = GetConfig();

        ConfigureNativeSingletons(config);

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

void RunCypressProxyProgram(int argc, const char** argv)
{
    TCypressProxyProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
