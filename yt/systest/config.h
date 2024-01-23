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
    int Seed;
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

    TString HomeDirectory;
    TString Pool;

    TConfig();

    void RegisterOptions(NLastGetopt::TOpts* opts);
};

}  // namespace NYT::NTest
