#pragma once

#include <library/cpp/getopt/last_getopt.h>

#include <yt/systest/runner.h>

namespace NYT::NTest {

struct TConfig {
    TRunnerConfig RunnerConfig;
    TValidatorConfig ValidatorConfig;
    TString HomeDirectory;
    bool Ipv4;
    TString Pool;

    TConfig();

    void RegisterOptions(NLastGetopt::TOpts* opts);
};

}  // namespace NYT::NTest
