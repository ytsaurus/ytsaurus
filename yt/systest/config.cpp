
#include <yt/systest/config.h>

namespace NYT::NTest {

TConfig::TConfig() {
}

void TNetworkConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddLongOption("ipv4")
        .StoreResult(&Ipv4)
        .DefaultValue(false);
}

void TConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddLongOption("home")
        .StoreResult(&HomeDirectory)
        .DefaultValue("//home");

    opts->AddLongOption("pool")
        .StoreResult(&Pool)
        .DefaultValue("systest");


    Network.RegisterOptions(opts);
    RunnerConfig.RegisterOptions(opts);
    ValidatorConfig.RegisterOptions(opts);
}

}  // namespace NYT::NTest
