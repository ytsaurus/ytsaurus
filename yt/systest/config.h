#pragma once

#include <library/cpp/getopt/last_getopt.h>

#include <yt/systest/validator.h>

namespace NYT::NTest {

struct TNetworkConfig {
    bool Ipv4;
    void RegisterOptions(NLastGetopt::TOpts* opts);
};

struct TTestConfig {
    int NumPhases;
    int NumChains;
    int Seed;
    int Multiplier;
    int64_t NumBootstrapRecords;

    bool EnableReduce;
    bool EnableRenames;
    bool EnableDeletes;

    void RegisterOptions(NLastGetopt::TOpts* opts);
};

struct TConfig {
    TTestConfig TestConfig;
    TValidatorConfig ValidatorConfig;
    TNetworkConfig NetworkConfig;

    int RunnerThreads;
    TString HomeDirectory;
    TString Pool;

    TDuration Ttl;

    TConfig();

    void RegisterOptions(NLastGetopt::TOpts* opts);
};

}  // namespace NYT::NTest
