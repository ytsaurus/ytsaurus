
#include <yt/systest/config.h>

namespace NYT::NTest {

TConfig::TConfig() {
}

void TConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddCharOption('n')
        .StoreResult(&RunnerConfig.NumOperations)
        .DefaultValue(4);

    opts->AddLongOption("home")
        .StoreResult(&RunnerConfig.HomeDirectory)
        .DefaultValue("//home");

    opts->AddLongOption("num-bootstrap-records")
        .StoreResult(&RunnerConfig.NumBootstrapRecords)
        .DefaultValue(10000);

    opts->AddLongOption("ipv4")
        .StoreResult(&Ipv4)
        .DefaultValue(false);

    opts->AddLongOption("seed")
        .StoreResult(&RunnerConfig.Seed)
        .DefaultValue(42);

    opts->AddLongOption("enable-reduce")
        .StoreResult(&RunnerConfig.EnableReduce)
        .DefaultValue(false);

    opts->AddLongOption("enable-renames")
        .StoreResult(&RunnerConfig.EnableRenames)
        .DefaultValue(false);

    opts->AddLongOption("enable-deletes")
        .StoreResult(&RunnerConfig.EnableDeletes)
        .DefaultValue(false);
}

}  // namespace NYT::NTest
