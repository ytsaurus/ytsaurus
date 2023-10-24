#pragma once

#include <library/cpp/getopt/last_getopt.h>

#include <yt/systest/runner.h>

namespace NYT::NTest {

struct TConfig {
    TRunnerConfig RunnerConfig;
    bool Ipv4;

    TConfig();

    void RegisterOptions(NLastGetopt::TOpts* opts);
};

}  // namespace NYT::NTest
