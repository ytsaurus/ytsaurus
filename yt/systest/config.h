#pragma once

#include <library/cpp/getopt/last_getopt.h>

#include <yt/systest/runner.h>

namespace NYT::NTest {

struct TNetworkConfig {
    bool Ipv4;
    void RegisterOptions(NLastGetopt::TOpts* opts);
};

struct TConfig {
    TRunnerConfig RunnerConfig;
    TValidatorConfig ValidatorConfig;
    TNetworkConfig Network;

    TString HomeDirectory;
    TString Pool;

    TConfig();

    void RegisterOptions(NLastGetopt::TOpts* opts);
};

}  // namespace NYT::NTest
