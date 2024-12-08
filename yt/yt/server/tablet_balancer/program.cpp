#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/ytlib/program/native_singletons.h>

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/program_config_mixin.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerProgram
    : public TServerProgram
    , public TProgramConfigMixin<TTabletBalancerServerConfig>
{
public:
    TTabletBalancerProgram()
        : TProgramConfigMixin(Opts_)
    {
        SetMainThreadName("TabletBalancer");
    }

protected:
    void DoStart() final
    {
        auto config = GetConfig();

        ConfigureNativeSingletons(config);

        auto configNode = GetConfigNode();

        auto* bootstrap = CreateBootstrap(std::move(config), std::move(configNode)).release();
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunTabletBalancerProgram(int argc, const char** argv)
{
    TTabletBalancerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
