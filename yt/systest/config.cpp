
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
        .StoreResult(&N)
        .DefaultValue(4);

    opts->AddLongOption("preset")
        .StoreResult(&Preset)
        .DefaultValue("short");

    opts->AddLongOption("length-a")
        .StoreResult(&LengthA)
        .DefaultValue(4);

    opts->AddLongOption("length-b")
        .StoreResult(&LengthB)
        .DefaultValue(4);

    opts->AddLongOption("length-c")
        .StoreResult(&LengthC)
        .DefaultValue(4);

    opts->AddLongOption("length-d")
        .StoreResult(&LengthD)
        .DefaultValue(4);

    opts->AddLongOption("num-bootstrap-records")
        .StoreResult(&NumBootstrapRecords)
        .DefaultValue(10000);

    opts->AddLongOption("multiplier")
        .StoreResult(&Multiplier)
        .DefaultValue(4);

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

void TTestConfig::Validate()
{
    if (Preset != "full" && Preset != "short") {
        fprintf(stderr, "--preset must either be 'full' or 'short'\n");
        exit(EXIT_FAILURE);
    }
    if (Preset == "full" && (!EnableReduce || !EnableRenames)) {
        fprintf(stderr, "If --preset full, --enable-reduce and --enable-renames must be set\n");
        exit(EXIT_FAILURE);
    }
}

///////////////////////////////////////////////////////////////////////////////

void THomeConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddLongOption("home")
        .StoreResult(&HomeDirectory)
        .DefaultValue("//home");

    opts->AddLongOption("ttl")
        .StoreResult(&Ttl)
        .DefaultValue(TDuration::Zero());

    opts->AddLongOption("interval-shards")
        .StoreResult(&IntervalShards)
        .DefaultValue(1);
}

///////////////////////////////////////////////////////////////////////////////

void TValidatorConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddLongOption("validator-jobs")
        .StoreResult(&NumJobs)
        .DefaultValue(4);

    opts->AddLongOption("validator-job-memory-limit")
        .StoreResult(&MemoryLimit)
        .DefaultValue(2LL << 30);

    opts->AddLongOption("validator-interval-bytes")
        .StoreResult(&IntervalBytes)
        .DefaultValue(64 << 20);

    opts->AddLongOption("validator-poll-delay")
        .StoreResult(&PollDelay)
        .DefaultValue(TDuration::Seconds(5));

    opts->AddLongOption("validator-interval-timeout")
        .StoreResult(&IntervalTimeout)
        .DefaultValue(TDuration::Minutes(1));

    opts->AddLongOption("validator-base-timeout")
        .StoreResult(&BaseTimeout)
        .DefaultValue(TDuration::Hours(1));

    opts->AddLongOption("validator-worker-failure-backoff-delay")
        .StoreResult(&WorkerFailureBackoffDelay)
        .DefaultValue(TDuration::Seconds(30));

    opts->AddLongOption("validator-limit-sort")
        .StoreResult(&SortVerificationLimit)
        .DefaultValue(8LL << 30  /* 8GB */);

}

///////////////////////////////////////////////////////////////////////////////

TConfig::TConfig() {
}

void TConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddLongOption("pool")
        .StoreResult(&Pool)
        .DefaultValue("systest");

    opts->AddLongOption("runner-threads")
        .StoreResult(&RunnerThreads)
        .DefaultValue(8);

    TestConfig.RegisterOptions(opts);
    NetworkConfig.RegisterOptions(opts);
    ValidatorConfig.RegisterOptions(opts);
    HomeConfig.RegisterOptions(opts);
}

void TConfig::Validate()
{
    TestConfig.Validate();
}

}  // namespace NYT::NTest
