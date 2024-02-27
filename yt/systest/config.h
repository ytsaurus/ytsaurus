#pragma once

#include <library/cpp/getopt/last_getopt.h>

#include <util/datetime/base.h>

namespace NYT::NTest {

struct TNetworkConfig {
    bool Ipv4;
    void RegisterOptions(NLastGetopt::TOpts* opts);
};

struct TTestConfig {
    TString Preset;
    int N;
    int Seed;
    int Multiplier;
    int64_t NumBootstrapRecords;
    int LengthA, LengthB, LengthC, LengthD;

    bool EnableReduce;
    bool EnableRenames;
    bool EnableDeletes;

    void RegisterOptions(NLastGetopt::TOpts* opts);
    void Validate();
};

struct THomeConfig
{
    TString HomeDirectory;
    TDuration Ttl;
    int IntervalShards;
    void RegisterOptions(NLastGetopt::TOpts* opts);
};

struct TValidatorConfig
{
    int NumJobs;
    int64_t IntervalBytes;
    int64_t MemoryLimit;
    int64_t SortVerificationLimit;
    TDuration PollDelay;
    TDuration WorkerFailureBackoffDelay;
    TDuration BaseTimeout;
    TDuration IntervalTimeout;

    void RegisterOptions(NLastGetopt::TOpts* opts);
};

struct TConfig {
    TTestConfig TestConfig;
    TValidatorConfig ValidatorConfig;
    TNetworkConfig NetworkConfig;
    THomeConfig HomeConfig;

    int RunnerThreads;
    TString Pool;

    TConfig();

    void RegisterOptions(NLastGetopt::TOpts* opts);
    void Validate();
};

}  // namespace NYT::NTest
