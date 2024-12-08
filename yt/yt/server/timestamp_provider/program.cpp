#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/program_config_mixin.h>

#include <yt/yt/library/program/helpers.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

class TTimestampProviderProgram
    : public TServerProgram
    , public TProgramConfigMixin<TTimestampProviderConfig>
{
public:
    TTimestampProviderProgram()
        : TProgramConfigMixin(Opts_)
    {
        SetMainThreadName("TSProvider");
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

void RunTimestampProviderProgram(int argc, const char** argv)
{
    TTimestampProviderProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
