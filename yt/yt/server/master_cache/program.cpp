#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/program_config_mixin.h>

#include <yt/yt/ytlib/program/native_singletons.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheProgram
    : public TServerProgram
    , public TProgramConfigMixin<TMasterCacheConfig>
{
public:
    TMasterCacheProgram()
        : TProgramConfigMixin(Opts_)
    {
        SetMainThreadName("MasterCache");
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

void RunMasterCacheProgram(int argc, const char** argv)
{
    TMasterCacheProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
