
#include <yt/systest/config.h>

namespace NYT::NTest {

void TNetworkConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddLongOption("ipv4")
        .StoreResult(&Ipv4)
        .DefaultValue(false);
}

///////////////////////////////////////////////////////////////////////////////

void TTestConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddCharOption('n')
        .StoreResult(&NumPhases)
        .DefaultValue(4);

    opts->AddLongOption("num-bootstrap-records")
        .StoreResult(&NumBootstrapRecords)
        .DefaultValue(10000);

    opts->AddLongOption("seed")
        .StoreResult(&Seed)
        .DefaultValue(42);

    opts->AddLongOption("enable-reduce")
        .StoreResult(&EnableReduce)
        .DefaultValue(false);

    opts->AddLongOption("enable-renames")
        .StoreResult(&EnableRenames)
        .DefaultValue(false);

    opts->AddLongOption("enable-deletes")
        .StoreResult(&EnableDeletes)
        .DefaultValue(false);
}

///////////////////////////////////////////////////////////////////////////////

TConfig::TConfig() {
}

void TConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddLongOption("home")
        .StoreResult(&HomeDirectory)
        .DefaultValue("//home");

    opts->AddLongOption("pool")
        .StoreResult(&Pool)
        .DefaultValue("systest");

    TestConfig.RegisterOptions(opts);
    NetworkConfig.RegisterOptions(opts);
    ValidatorConfig.RegisterOptions(opts);
}

}  // namespace NYT::NTest
