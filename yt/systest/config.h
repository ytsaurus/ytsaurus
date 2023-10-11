#pragma once

#include <yt/systest/runner.h>

namespace NYT::NTest {

struct TConfig {
    TRunnerConfig RunnerConfig;
    bool Ipv6;

    TConfig();
    void ParseCommandline(int argc, char* argv[]);
};

}  // namespace NYT::NTest
