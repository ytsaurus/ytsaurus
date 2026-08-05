#include <yt/yt/flow/library/cpp/worker/buffer_state_manager.h>
#include <yt/yt/flow/library/cpp/worker/job_spec.h>

#include <yt/yt/flow/library/cpp/buffers/epoch_cycle_tracker.h>
#include <yt/yt/flow/library/cpp/common/buffer_warmup.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <deque>

namespace NYT::NFlow::NWorker {
namespace {

using namespace NConcurrency;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TMockTimeProvider
    : public TRefCounted
{
public:
    TMockTimeProvider()
        : CurrentTime_(TInstant::Zero().MicroSeconds())
    { }

    TInstant Now() const
    {
        return TInstant::MicroSeconds(CurrentTime_.load(std::memory_order::relaxed));
    }

    void Set(TInstant time)
    {
        CurrentTime_.store(time.MicroSeconds(), std::memory_order::relaxed);
    }

    std::function<TInstant()> GetProvider() const
    {
        return [thisStrong = MakeStrong(this)] {
            return thisStrong->Now();
        };
    }

private:
    std::atomic<i64> CurrentTime_;
};

DEFINE_REFCOUNTED_TYPE(TMockTimeProvider);

////////////////////////////////////////////////////////////////////////////////

class TMockJobDirectory
    : public IJobDirectory
{
public:
    explicit TMockJobDirectory(i64 partitionCount)
        : PartitionCount_(partitionCount)
    { }

    void Reconfigure(const TFlowLayoutPtr& /*flowLayout*/, const TPipelineSpecPtr& /*pipelineSpec*/) override
    { }

    TJobDirectorySnapshotPtr GetSnapshot() const override
    {
        return New<TJobDirectorySnapshot>(
            THashMap<TComputationId, NTableClient::TTableSchemaPtr>{},
            /*converterCache*/ nullptr,
            THashMap<TComputationId, TJobDirectorySnapshot::TComputationRouting>{},
            THashSet<TJobId>{},
            THashSet<TJobId>{},
            THashSet<std::string>{},
            NLogging::TLogger());
    }

    i64 GetPartitionCount(const TComputationId& /*computationId*/) const override
    {
        return PartitionCount_;
    }

    std::optional<TMessageRoute> FindRouteByKey(const TComputationId& /*computationId*/, const TKey& /*key*/) const override
    {
        return std::nullopt;
    }

    DEFINE_SIGNAL_OVERRIDE(TSnapshotPublishedSignature, SnapshotPublished);

private:
    const i64 PartitionCount_;
};

DEFINE_REFCOUNTED_TYPE(TMockJobDirectory);

////////////////////////////////////////////////////////////////////////////////

TDynamicBufferStateManagerSpecPtr CreateDefaultDynamicSpec()
{
    return New<TDynamicBufferStateManagerSpec>();
}

TDynamicBufferStateManagerSpecPtr CreateDynamicSpec(
    i64 inputGuarantee,
    i64 inputLimit,
    i64 outputGuarantee,
    i64 outputLimit)
{
    auto makeOneSide = [&] (i64 guarantee, i64 limit) {
        auto spec = New<TDynamicBufferStateManagerSpec::TOneSideBufferSpec>();
        spec->FairSharePool = NYTree::TSize(limit * 10);
        spec->JobGuarantee = NYTree::TSize(guarantee);
        spec->JobLimit = NYTree::TSize(limit);
        spec->MaxDuration = TDuration::Minutes(1);
        return spec;
    };

    auto spec = New<TDynamicBufferStateManagerSpec>();
    spec->DemandWindow = TDuration::Minutes(1);
    spec->InputBuffer = makeOneSide(inputGuarantee, inputLimit);
    spec->OutputBuffer = makeOneSide(outputGuarantee, outputLimit);
    return spec;
}

TJobSpecPtr CreateJobSpec(
    const TStreamId& inputStreamId,
    const std::vector<TStreamId>& outputStreamIds)
{
    auto computationSpec = New<TComputationSpec>();
    computationSpec->InputStreamIds.insert(inputStreamId);
    for (const auto& outputStreamId : outputStreamIds) {
        computationSpec->OutputStreamIds.insert(outputStreamId);
    }

    auto partition = New<TPartition>();
    partition->ComputationId = TComputationId("computation");

    auto jobSpec = New<TJobSpec>();
    jobSpec->ComputationSpec = computationSpec;
    jobSpec->Partition = partition;
    return jobSpec;
}

TJobSpecPtr CreateJobSpec(const TStreamId& inputStreamId, const TStreamId& outputStreamId)
{
    return CreateJobSpec(inputStreamId, std::vector<TStreamId>{outputStreamId});
}

struct TManagedState
{
    IBufferStateManagerPtr Manager;
    TJobStreamLimitUsageStates States;
};

struct TBufferStateTestParam
{
    bool IsInputBuffer;
    TString Name;
};

class TBufferStateTest
    : public ::testing::TestWithParam<TBufferStateTestParam>
{ };

TManagedState CreateManagedState(
    const TStreamId& inputStreamId,
    const TStreamId& outputStreamId,
    i64 inputGuarantee,
    i64 inputLimit,
    i64 outputGuarantee,
    i64 outputLimit,
    std::function<TInstant()> timeProvider = [] {
        return TInstant::Now();
    })
{
    auto manager = CreateBufferStateManager(
        GetSyncInvoker(),
        New<TMockJobDirectory>(1),
        CreateDynamicSpec(inputGuarantee, inputLimit, outputGuarantee, outputLimit),
        std::move(timeProvider),
        /*workerGroups*/ {},
        /*enablePeriodicManagement*/ false);

    auto jobSpec = CreateJobSpec(inputStreamId, outputStreamId);
    auto states = manager->RegisterJob(TJobId(TGuid::Create()), jobSpec);
    return {.Manager = std::move(manager), .States = std::move(states)};
}

TStreamLimitUsageStatePtr GetSideState(
    const TJobStreamLimitUsageStates& states,
    bool isInputBuffer,
    const TStreamId& streamId)
{
    const auto& side = isInputBuffer ? states.Input : states.Output;
    return side.at(streamId);
}

////////////////////////////////////////////////////////////////////////////////

// Fixture for the direct-drive v2 behaviour tests: mutate Spec_ (v2 on by
// default), call CreateManager(), then drive per-tick with Manage()/FillAndDrain().
class TBufferManagerTest
    : public ::testing::Test
{
protected:
    TIntrusivePtr<TMockTimeProvider> TimeProvider_ = New<TMockTimeProvider>();
    TDynamicBufferStateManagerSpecPtr Spec_ = [] {
        auto spec = CreateDefaultDynamicSpec();
        spec->EnableV2 = true;
        return spec;
    }();
    TInstant Now_ = TInstant::Zero();

    IBufferStateManagerPtr CreateManager(std::vector<TWorkerGroupId> workerGroups = {})
    {
        return CreateBufferStateManager(
            GetSyncInvoker(),
            New<TMockJobDirectory>(1),
            Spec_,
            TimeProvider_->GetProvider(),
            std::move(workerGroups),
            /*enablePeriodicManagement*/ false);
    }

    void Manage(const IBufferStateManagerPtr& manager)
    {
        Now_ += TDuration::Seconds(1);
        TimeProvider_->Set(Now_);
        manager->ManageBuffers();
    }

    // Fill the stream to its current limit, then drain everything.
    static void FillAndDrain(const TStreamLimitUsageStatePtr& state, TStreamUsage& usage)
    {
        usage.PendingInflatedBytes = 1_GB;
        while (state->IsUsageWithinLimits(usage)) {
            usage.CumulativeByteIn += 100'000;
            ++usage.CumulativeCountIn;
            state->Update(usage);
        }
        usage.CumulativeByteOut = usage.CumulativeByteIn;
        usage.CumulativeCountOut = usage.CumulativeCountIn;
        state->Update(usage);
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunSimpleUsedBytesWithThreeMessages(bool isInputBuffer)
{
    auto inputStreamId = TStreamId("input");
    auto outputStreamId = TStreamId("output");
    auto managedState = CreateManagedState(inputStreamId, outputStreamId, 8000, 10000, 8000, 10000);
    const auto& streamId = isInputBuffer ? inputStreamId : outputStreamId;
    auto state = GetSideState(managedState.States, isInputBuffer, streamId);

    TStreamUsage usage;
    auto addAndCheck = [&] (i64 byteSize, i64 expectedUsed) {
        usage.CumulativeByteIn += byteSize;
        ++usage.CumulativeCountIn;
        state->Update(usage);
        auto read = state->Read();
        EXPECT_EQ(read.CumulativeByteIn - read.CumulativeByteOut, expectedUsed);
    };

    addAndCheck(1000, 1000);
    addAndCheck(2000, 3000);
    addAndCheck(3000, 6000);

    usage.CumulativeByteOut = usage.CumulativeByteIn;
    usage.CumulativeCountOut = usage.CumulativeCountIn;
    state->Update(usage);
    EXPECT_EQ(state->Read().CumulativeByteIn - state->Read().CumulativeByteOut, 0);
}

TEST_P(TBufferStateTest, SimpleUsedBytesWithThreeMessages)
{
    RunSimpleUsedBytesWithThreeMessages(GetParam().IsInputBuffer);
}

////////////////////////////////////////////////////////////////////////////////

void RunWarmUpWithTimeSimulation(bool isInputBuffer)
{
    auto inputStreamId = TStreamId("input");
    auto outputStreamId = TStreamId("output");

    auto timeProvider = New<TMockTimeProvider>();
    auto managedState = CreateManagedState(
        inputStreamId,
        outputStreamId,
        /*inputGuarantee*/ 2000,
        /*inputLimit*/ 1'000'000,
        /*outputGuarantee*/ 2000,
        /*outputLimit*/ 1'000'000,
        timeProvider->GetProvider());
    const auto& streamId = isInputBuffer ? inputStreamId : outputStreamId;
    auto state = GetSideState(managedState.States, isInputBuffer, streamId);

    TInstant currentTime = TInstant::Zero();
    TInstant endTime = TInstant::Zero() + TDuration::Minutes(5);
    const i64 messageSize = 200;
    const auto processingLatency = TDuration::Seconds(30);

    std::deque<std::pair<TInstant, i64>> processingMessages;
    std::vector<i64> limitBy30Seconds;
    TStreamUsage usage;
    timeProvider->Set(currentTime);

    while (currentTime < endTime) {
        usage = state->Read();
        if (usage.CumulativeByteIn - usage.CumulativeByteOut + messageSize <= state->GetLimitBytes()) {
            usage.CumulativeByteIn += messageSize;
            ++usage.CumulativeCountIn;
            state->Update(usage);
            processingMessages.push_back({currentTime, messageSize});
            continue;
        }

        currentTime += TDuration::Seconds(1);
        timeProvider->Set(currentTime);

        while (!processingMessages.empty() &&
            processingMessages.front().first + processingLatency <= currentTime)
        {
            usage.CumulativeByteOut += processingMessages.front().second;
            ++usage.CumulativeCountOut;
            state->Update(usage);
            processingMessages.pop_front();
        }

        managedState.Manager->ManageBuffers();

        if (currentTime != TInstant::Zero() && currentTime.Seconds() % 30 == 0) {
            limitBy30Seconds.push_back(state->GetLimitBytes());
        }
    }

    EXPECT_GT(state->GetLimitBytes(), 50000);
    EXPECT_GT(limitBy30Seconds.back(), limitBy30Seconds.front());
}

TEST_P(TBufferStateTest, WarmUpWithTimeSimulation)
{
    RunWarmUpWithTimeSimulation(GetParam().IsInputBuffer);
}

////////////////////////////////////////////////////////////////////////////////
// A/B simulation: v1 (trunk formula) vs v2 (BDP target + gradual issuance).
// Virtual time, 1-second steps; the producer offers a fixed byte rate, the
// consumer is either continuous (fixed processing latency) or epoch-structured
// (drains everything once per epoch cycle, like binary-base updaters).

struct TScenarioOptions
{
    bool EnableV2 = false;
    TDuration Total = TDuration::Minutes(10);
    double OfferedBytesPerSec = 0;
    i64 MessageSize = 10'000;
    // The consumer extracts everything once per cycle (input-buffer drain model:
    // usage is released at batch extraction); a fast consumer is a 1s cycle, a
    // latency-bound one is minutes.
    TDuration EpochCycle = TDuration::Seconds(1);
    // Rate step: at |StepTime| the offered rate becomes |SteppedBytesPerSec|.
    TDuration StepTime = TDuration::Zero();
    double SteppedBytesPerSec = 0;
    // Warm restart: feed the manager persisted warmup statistics before RegisterJob.
    bool SeedWarmup = false;
    // Buffered-time cap (max_duration spec knob); latency-bound pipelines with
    // epochs above the default are expected to raise it.
    TDuration MaxDuration = TDuration::Minutes(1);
};

struct TScenarioResult
{
    // First instant when the trailing-30s accepted throughput reaches 90% of
    // the currently offered rate (measured from the rate step if there is one).
    std::optional<TDuration> TimeToSustained;
    i64 DrainedInLastEpoch = 0;
    i64 FinalLimit = 0;
    i64 LimitAfterFirstTick = 0;
    i64 PeakInflightBytes = 0;
    i64 TotalAccepted = 0;
};

TScenarioResult RunScenario(const TScenarioOptions& options)
{
    auto inputStreamId = TStreamId("input");
    auto outputStreamId = TStreamId("output");
    auto timeProvider = New<TMockTimeProvider>();

    auto spec = CreateDefaultDynamicSpec();
    spec->EnableV2 = options.EnableV2;
    spec->InputBuffer->MaxDuration = options.MaxDuration;
    spec->OutputBuffer->MaxDuration = options.MaxDuration;

    auto manager = CreateBufferStateManager(
        GetSyncInvoker(),
        New<TMockJobDirectory>(1),
        spec,
        timeProvider->GetProvider(),
        /*workerGroups*/ {},
        /*enablePeriodicManagement*/ false);
    TPartitionBufferWarmup warmup;
    if (options.SeedWarmup) {
        // The previous incarnation converged on the offered rate and this epoch cycle.
        warmup.InputSpeeds[inputStreamId] = options.OfferedBytesPerSec * 1.05;
        warmup.EpochCycleSeconds = options.EpochCycle.SecondsFloat();
    }
    auto jobId = TJobId(TGuid::Create());
    auto states = manager->RegisterJob(jobId, CreateJobSpec(inputStreamId, outputStreamId));
    if (options.SeedWarmup) {
        manager->SeedJob(jobId, warmup);
    }
    auto state = states.Input.at(inputStreamId);
    const i64 inflation = state->GetInflationPerMessage();

    TScenarioResult result;
    TStreamUsage usage;
    double backlogBytes = 0;
    std::deque<i64> inflightMessages;
    std::deque<i64> acceptedPerSecond;
    TInstant lastEpochBoundary = TInstant::Zero();

    const TInstant start = TInstant::Zero();
    const TInstant end = start + options.Total;
    const TInstant measureFrom = options.StepTime != TDuration::Zero() ? start + options.StepTime : start;

    for (TInstant now = start; now < end; now += TDuration::Seconds(1)) {
        timeProvider->Set(now);
        const double offered = (options.StepTime != TDuration::Zero() && now >= start + options.StepTime)
            ? options.SteppedBytesPerSec
            : options.OfferedBytesPerSec;
        backlogBytes += offered;

        // Consumer: extract everything once per cycle.
        if (now - lastEpochBoundary >= options.EpochCycle) {
            lastEpochBoundary = now;
            i64 drained = 0;
            while (!inflightMessages.empty()) {
                drained += inflightMessages.front();
                usage.CumulativeByteOut += inflightMessages.front();
                ++usage.CumulativeCountOut;
                inflightMessages.pop_front();
            }
            if (drained > 0) {
                result.DrainedInLastEpoch = drained;
                states.InputEpochCycleTracker->RecordCycle(options.EpochCycle);
            }
        }

        // Transfer: admit messages while usage is within the limit (the input
        // buffer admits one message past the limit, same as the pre-accept check).
        i64 acceptedThisSecond = 0;
        while (backlogBytes >= options.MessageSize) {
            usage.PendingInflatedBytes = static_cast<i64>(backlogBytes) +
                static_cast<i64>(backlogBytes / options.MessageSize) * inflation;
            state->Update(usage);
            if (!state->IsUsageWithinLimits(usage)) {
                break;
            }
            backlogBytes -= options.MessageSize;
            usage.CumulativeByteIn += options.MessageSize;
            ++usage.CumulativeCountIn;
            acceptedThisSecond += options.MessageSize;
            inflightMessages.push_back(options.MessageSize);
        }
        usage.PendingInflatedBytes = static_cast<i64>(backlogBytes) +
            static_cast<i64>(backlogBytes / options.MessageSize) * inflation;
        state->Update(usage);

        result.PeakInflightBytes = std::max(result.PeakInflightBytes, usage.CumulativeByteIn - usage.CumulativeByteOut);
        result.TotalAccepted += acceptedThisSecond;

        acceptedPerSecond.push_back(acceptedThisSecond);
        if (std::ssize(acceptedPerSecond) > 30) {
            acceptedPerSecond.pop_front();
        }
        if (!result.TimeToSustained && now >= measureFrom && offered > 0 && std::ssize(acceptedPerSecond) == 30) {
            i64 trailing = 0;
            for (auto value : acceptedPerSecond) {
                trailing += value;
            }
            if (static_cast<double>(trailing) >= 0.9 * offered * 30) {
                result.TimeToSustained = now - measureFrom;
            }
        }

        manager->ManageBuffers();
        if (now == start) {
            result.LimitAfterFirstTick = state->GetLimitBytes();
        }
    }

    result.FinalLimit = state->GetLimitBytes();
    return result;
}

void PrintComparison(TStringBuf name, const TScenarioResult& v1, const TScenarioResult& v2)
{
    auto formatTime = [] (const std::optional<TDuration>& d) {
        return d ? ToString(*d) : TString("never");
    };
    Cout << "=== " << name << " ===" << Endl;
    Cout << "  time_to_sustained: v1=" << formatTime(v1.TimeToSustained) << " v2=" << formatTime(v2.TimeToSustained) << Endl;
    Cout << "  drained_in_last_epoch: v1=" << v1.DrainedInLastEpoch << " v2=" << v2.DrainedInLastEpoch << Endl;
    Cout << "  final_limit: v1=" << v1.FinalLimit << " v2=" << v2.FinalLimit << Endl;
    Cout << "  peak_inflight: v1=" << v1.PeakInflightBytes << " v2=" << v2.PeakInflightBytes << Endl;
    Cout << "  total_accepted: v1=" << v1.TotalAccepted << " v2=" << v2.TotalAccepted << Endl;
}

TEST(TBufferStrategyComparison, ColdStartFastConsumer)
{
    TScenarioOptions options{
        .Total = TDuration::Minutes(10),
        .OfferedBytesPerSec = 50e6,
        .MessageSize = 100'000,
        .EpochCycle = TDuration::Seconds(2),
    };
    options.EnableV2 = false;
    auto v1 = RunScenario(options);
    options.EnableV2 = true;
    auto v2 = RunScenario(options);
    PrintComparison("ColdStartFastConsumer", v1, v2);

    // The offered rate sizes the cold buffer at once: v2 reaches sustained
    // throughput within a few epochs. (v1's EMA has to grow into it; both accept
    // ~everything here, so the total is not a discriminator.)
    ASSERT_TRUE(v2.TimeToSustained.has_value());
    EXPECT_LE(*v2.TimeToSustained, TDuration::Seconds(30));
}

TEST(TBufferStrategyComparison, LatencyBoundEpochs)
{
    // The epoch is far longer than the v1 demand EMA window (60s), so the v1
    // limit decays between drains and cannot hold a full epoch of input.
    // A latency-bound pipeline raises max_duration to its epoch scale so the
    // buffered-time cap (demand × max_duration) admits a whole epoch of input.
    TScenarioOptions options{
        .Total = TDuration::Minutes(60),
        .OfferedBytesPerSec = 1e6,
        .MessageSize = 100'000,
        .EpochCycle = TDuration::Minutes(5),
        .MaxDuration = TDuration::Minutes(5),
    };
    options.EnableV2 = false;
    auto v1 = RunScenario(options);
    options.EnableV2 = true;
    auto v2 = RunScenario(options);
    PrintComparison("LatencyBoundEpochs", v1, v2);

    // v2 settles on full steady epoch batches; v1 oscillates around the decayed
    // EMA and loses input overall. The demand is measured on the job's own epoch
    // (no wall-clock window), so a cold ramp costs about one epoch — within the
    // 1-2 epoch cold-start goal — and is still faster than v1's oscillation.
    const double idealEpochBatch = options.OfferedBytesPerSec * options.EpochCycle.SecondsFloat();
    EXPECT_GE(v2.DrainedInLastEpoch, static_cast<i64>(0.9 * idealEpochBatch));
    EXPECT_LE(v2.DrainedInLastEpoch, static_cast<i64>(1.15 * idealEpochBatch));
    ASSERT_TRUE(v2.TimeToSustained.has_value());
    ASSERT_TRUE(v1.TimeToSustained.has_value());
    EXPECT_LE(*v2.TimeToSustained, options.EpochCycle);
    EXPECT_LT(*v2.TimeToSustained, *v1.TimeToSustained);
    // Both strategies drain full epochs at steady state here, so totals are close;
    // v2's honest per-epoch ramp costs a couple percent up front (its edge is the
    // faster sustain checked above, not raw total in this v1-survivable scenario).
    EXPECT_GE(v2.TotalAccepted, static_cast<i64>(0.95 * v1.TotalAccepted));
}

TEST(TBufferWarmupBehaviour, SeededRestartLatencyBound)
{
    // Warm restart of a latency-bound partition: with persisted demand and epoch
    // cycle the very first issuance matches the previous steady state, so full
    // throughput resumes within one epoch and without the bootstrap overshoot.
    // A latency-bound pipeline is expected to raise max_duration above its epoch;
    // otherwise the seeded headroom is capped by the default 1 minute.
    TScenarioOptions options{
        .EnableV2 = true,
        .Total = TDuration::Minutes(30),
        .OfferedBytesPerSec = 1e6,
        .MessageSize = 100'000,
        .EpochCycle = TDuration::Minutes(5),
        .MaxDuration = TDuration::Minutes(10),
    };
    auto coldV2 = RunScenario(options);
    options.SeedWarmup = true;
    auto seededV2 = RunScenario(options);
    PrintComparison("SeededRestartLatencyBound (cold-v2 vs seeded-v2)", coldV2, seededV2);

    ASSERT_TRUE(seededV2.TimeToSustained.has_value());
    EXPECT_LE(*seededV2.TimeToSustained, options.EpochCycle);
    EXPECT_LE(*seededV2.TimeToSustained, *coldV2.TimeToSustained);
    EXPECT_GE(seededV2.TotalAccepted, coldV2.TotalAccepted);
    // The very first issued limit already matches the previous steady state
    // instead of the bootstrap floor.
    EXPECT_GE(seededV2.LimitAfterFirstTick, static_cast<i64>(100_MB));
    EXPECT_LE(coldV2.LimitAfterFirstTick, static_cast<i64>(10_MB));
}

TEST(TBufferStrategyComparison, RateStep)
{
    TScenarioOptions options{
        .Total = TDuration::Minutes(15),
        .OfferedBytesPerSec = 100e3,
        .MessageSize = 20'000,
        .EpochCycle = TDuration::Seconds(5),
        .StepTime = TDuration::Minutes(5),
        .SteppedBytesPerSec = 10e6,
    };
    options.EnableV2 = false;
    auto v1 = RunScenario(options);
    options.EnableV2 = true;
    auto v2 = RunScenario(options);
    PrintComparison("RateStep", v1, v2);

    ASSERT_TRUE(v2.TimeToSustained.has_value());
    if (v1.TimeToSustained) {
        EXPECT_LE(*v2.TimeToSustained, *v1.TimeToSustained + TDuration::Seconds(10));
    }
    EXPECT_GE(v2.TotalAccepted, static_cast<i64>(0.99 * v1.TotalAccepted));
    // Same throughput with a materially smaller reserved limit.
    EXPECT_LE(v2.FinalLimit, v1.FinalLimit);
}

TEST(TBufferStrategyComparison, IdleStreamHoldsNoLimit)
{
    TScenarioOptions options{
        .Total = TDuration::Minutes(5),
        .OfferedBytesPerSec = 0,
    };
    options.EnableV2 = false;
    auto v1 = RunScenario(options);
    options.EnableV2 = true;
    auto v2 = RunScenario(options);
    PrintComparison("IdleStreamHoldsNoLimit", v1, v2);

    EXPECT_EQ(v2.FinalLimit, 0);
    EXPECT_GT(v1.FinalLimit, 0);
}

TEST_F(TBufferManagerTest, MixedEpochNeighborsStaySane)
{
    // A 0.5-second-epoch job next to a 180-second-epoch job in one manager:
    // neither estimate may poison the other, the slow job's burst drain must
    // not starve or explode, and the pool invariant must hold throughout.
    // The cap is above the slow job's true epoch (180 s), so its estimate is honest.
    Spec_->InputBuffer->MaxDuration = TDuration::Minutes(6);
    Spec_->InputBuffer->FairSharePool = NYTree::TSize(3_GB);
    Spec_->InputBuffer->JobLimit = NYTree::TSize(500_MB);
    auto manager = CreateManager();

    auto fastStates = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("input"), TStreamId("output")));
    auto slowStates = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("input"), TStreamId("output")));
    auto fast = fastStates.Input.at(TStreamId("input"));
    auto slow = slowStates.Input.at(TStreamId("input"));

    constexpr i64 FastRate = 10_MB; // Per second, drained continuously; epoch 0.5 s.
    constexpr i64 SlowRate = 1_MB;  // Per second, drained in one burst per 180 s.
    constexpr int SlowEpoch = 180;

    TStreamUsage fastUsage;
    TStreamUsage slowUsage;
    i64 slowProduced = 0;
    i64 slowDrained = 0;
    i64 slowLimitBeforeDrain = 0;

    for (int second = 1; second <= 900; ++second) {
        Now_ += TDuration::Seconds(1);
        TimeProvider_->Set(Now_);

        // Fast job: full epoch twice a second, drains everything it admits.
        fastStates.InputEpochCycleTracker->RecordCycle(TDuration::MilliSeconds(500));
        fastUsage.PendingInflatedBytes = 50_MB;
        while (fast->IsUsageWithinLimits(fastUsage) &&
            fastUsage.CumulativeByteIn - fastUsage.CumulativeByteOut < FastRate)
        {
            fastUsage.CumulativeByteIn += 1_MB;
            fast->Update(fastUsage);
        }
        fastUsage.CumulativeByteOut = fastUsage.CumulativeByteIn;
        fast->Update(fastUsage);

        // Slow job: producer trickles at SlowRate into the buffer while the
        // limit admits it; the whole accumulation drains in one burst per epoch.
        slowProduced += SlowRate;
        while (slow->IsUsageWithinLimits(slowUsage) && slowUsage.CumulativeByteIn < slowProduced) {
            slowUsage.CumulativeByteIn += 1_MB;
            slow->Update(slowUsage);
        }
        slowUsage.PendingInflatedBytes = slowProduced - slowUsage.CumulativeByteIn;
        slow->Update(slowUsage);
        if (second % SlowEpoch == SlowEpoch - 1) {
            // The moment the buffer matters: a full accumulation right before the drain.
            slowLimitBeforeDrain = slow->GetLimitBytes();
        }
        if (second % SlowEpoch == 0) {
            slowDrained += slowUsage.CumulativeByteIn - slowUsage.CumulativeByteOut;
            slowUsage.CumulativeByteOut = slowUsage.CumulativeByteIn;
            slow->Update(slowUsage);
            slowStates.InputEpochCycleTracker->RecordCycle(TDuration::Seconds(SlowEpoch));
        }

        manager->ManageBuffers();


        // The post-drain zero reservation must not throttle the next epoch: after a
        // few full cycles the max-filtered demand survives the drain, so the limit
        // is back at the full target right after fresh pending appears.
        if (second > 3 * SlowEpoch && second % SlowEpoch == 1) {
            ASSERT_GE(slow->GetLimitBytes(), static_cast<i64>(180_MB));
        }
    }

    const i64 fastLimit = fast->GetLimitBytes();
    const i64 slowLimit = slowLimitBeforeDrain;
    Cout << "MixedEpochNeighbors: fastLimit=" << fastLimit << " slowLimitBeforeDrain=" << slowLimit
         << " slowDrainedShare=" << static_cast<double>(slowDrained) / slowProduced << Endl;

    // Fast job: needs ~gain*rate*epoch(=manage period floor) = 20 MB; must not be
    // starved by the slow neighbor and must not balloon.
    EXPECT_GE(fastLimit, static_cast<i64>(10_MB));
    EXPECT_LE(fastLimit, static_cast<i64>(200_MB));

    // Slow job: needs ~rate*epoch = 180 MB to feed a whole epoch; with an honest
    // 180-second bucket the demand is the true mean and target = gain * need.
    EXPECT_GE(slowLimit, static_cast<i64>(60_MB));
    EXPECT_LE(slowLimit, static_cast<i64>(500_MB));

    // The slow job's throughput must not collapse: it drains at least ~3/4 of
    // what it produced over five epochs (the first epoch ramps).
    EXPECT_GE(static_cast<double>(slowDrained) / slowProduced, 0.75);
}

TEST_F(TBufferManagerTest, SlowProducerFeedsFastConsumer)
{
    // The reverse order: a 180-second-epoch producer dumps its whole epoch as
    // one burst into a 0.5-second-epoch consumer. Between bursts the consumer's
    // demand window (8 buckets of ~1 s) forgets the burst, so the instant
    // offered-rate credit must open the buffer when the burst arrives, and the
    // limit must shrink back between bursts instead of hoarding the pool.
    Spec_->InputBuffer->MaxDuration = TDuration::Minutes(6);
    Spec_->InputBuffer->FairSharePool = NYTree::TSize(3_GB);
    Spec_->InputBuffer->JobLimit = NYTree::TSize(500_MB);
    auto manager = CreateManager();

    auto states = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("input"), TStreamId("output")));
    auto fast = states.Input.at(TStreamId("input"));

    constexpr int SlowEpoch = 180;
    constexpr i64 Burst = 180_MB;

    TStreamUsage usage;
    i64 pendingBytes = 0;
    int burstArrival = 0;
    std::vector<int> admissionTicks; // Per burst: ticks from arrival to full admission.

    for (int second = 1; second <= 900; ++second) {
        Now_ += TDuration::Seconds(1);
        TimeProvider_->Set(Now_);

        if (second % SlowEpoch == 1) {
            pendingBytes += Burst;
            burstArrival = second;
        }

        // An approximation of the offered rate RecalculateStreamLimits would
        // derive: the pending volume over the burst's age.
        fast->SetOfferedInflatedBytesPerSecond(pendingBytes > 0
                ? static_cast<double>(pendingBytes) / std::max(1, second - burstArrival + 1)
                : 0.0);

        // Admit up to the limit, drain everything admitted (fast consumer).
        while (pendingBytes > 0 && fast->IsUsageWithinLimits(usage)) {
            i64 portion = std::min<i64>(pendingBytes, 1_MB);
            usage.CumulativeByteIn += portion;
            pendingBytes -= portion;
            fast->Update(usage);
        }
        if (pendingBytes == 0 && burstArrival > 0 && admissionTicks.size() < static_cast<size_t>((second - 1) / SlowEpoch + 1)) {
            admissionTicks.push_back(second - burstArrival);
        }
        usage.PendingInflatedBytes = pendingBytes;
        usage.CumulativeByteOut = usage.CumulativeByteIn;
        states.InputEpochCycleTracker->RecordCycle(TDuration::MilliSeconds(500));
        fast->Update(usage);

        manager->ManageBuffers();
    }

    ASSERT_EQ(admissionTicks.size(), 5u);
    Cout << "SlowProducerFeedsFastConsumer: admissionTicks=";
    for (auto t : admissionTicks) {
        Cout << t << " ";
    }
    Cout << "limitBetweenBursts=" << fast->GetLimitBytes() << Endl;

    // Every burst (including the first: the offered-rate credit needs no history)
    // is admitted within a few manage ticks of a 180-second pause.
    for (auto t : admissionTicks) {
        EXPECT_LE(t, 10);
    }
    // Between bursts the reservation is released, not hoarded.
    EXPECT_LE(fast->GetLimitBytes(), static_cast<i64>(2_MB));
}

// The announced-backlog offered rate is a limit-independent demand signal: with
// the flag on, the first tick sizes the buffer for it; with it off (escape hatch
// for over-reporting producers) the cold input stays at the floor instead.
TEST_F(TBufferManagerTest, OfferedRateHonorsTheFlag)
{
    for (bool useOfferedRate : {true, false}) {
        Spec_->V2UseOfferedRate = useOfferedRate;
        auto manager = CreateManager();
        auto state = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("input"), TStreamId("output")))
            .Input.at(TStreamId("input"));

        state->SetOfferedInflatedBytesPerSecond(50e6);
        state->Update(TStreamUsage{.PendingInflatedBytes = 500'000'000});
        Manage(manager);

        if (useOfferedRate) {
            EXPECT_GE(state->GetLimitBytes(), static_cast<i64>(50e6));
        } else {
            EXPECT_LT(state->GetLimitBytes(), static_cast<i64>(50e6));
        }
    }
}

// A source reports its backlog rate in raw bytes+messages; the slot inflates it
// by the per-message cost and fans the whole rate out to every output stream (the
// per-stream split is unknown), sizing each output on the first tick.
TEST_F(TBufferManagerTest, SourceRawRateInflatesAndFansOut)
{
    Spec_->OutputBuffer->JobLimit = NYTree::TSize(2_GB);
    Spec_->OutputBuffer->FairSharePool = NYTree::TSize(4_GB);
    auto manager = CreateManager();
    auto states = manager->RegisterJob(
        TJobId(TGuid::Create()),
        CreateJobSpec(TStreamId("input"), std::vector<TStreamId>{TStreamId("out_a"), TStreamId("out_b")}));

    constexpr double RawBytesPerSecond = 10e6;
    constexpr double MessagesPerSecond = 1e4;
    // Mirrors the computation fan-out: the source rate is split evenly across
    // the output streams so the side total equals the real rate.
    const double streamCount = std::ssize(states.Output);
    for (const auto& [streamId, state] : states.Output) {
        state->SetOfferedRawRate(RawBytesPerSecond / streamCount, MessagesPerSecond / streamCount);
    }

    auto out = states.Output.at(TStreamId("out_a"));
    const double inflatedRate = out->GetOfferedInflatedBytesPerSecond();
    EXPECT_NEAR(
        inflatedRate,
        (RawBytesPerSecond + MessagesPerSecond * out->GetInflationPerMessage()) / streamCount,
        1e-6);
    EXPECT_GT(inflatedRate, RawBytesPerSecond / streamCount);

    Manage(manager);
    for (const auto& [streamId, state] : states.Output) {
        EXPECT_GE(state->GetLimitBytes(), static_cast<i64>(inflatedRate)) << streamId.Underlying();
    }
}

// A job blocked on a stalled downstream produces no usage updates, so the
// read-and-reset peak reads 0 while its bytes stay resident; the limit must not
// collapse below them, or recovery pays a multi-tick re-ramp.
TEST_F(TBufferManagerTest, StalledStreamKeepsItsResidentBytesLimit)
{
    auto manager = CreateManager();
    auto states = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("input"), TStreamId("output")));
    auto output = states.Output.at(TStreamId("output"));

    TStreamUsage usage;
    for (int i = 0; i < 20; ++i) {
        FillAndDrain(output, usage);
        Manage(manager);
    }
    usage.CumulativeByteIn += 50_MB;
    output->Update(usage);
    Manage(manager);
    const i64 resident = usage.CumulativeByteIn - usage.CumulativeByteOut;

    // The stall: no updates at all for many manage ticks.
    for (int i = 0; i < 30; ++i) {
        Manage(manager);
    }
    EXPECT_GE(output->GetLimitBytes(), resident);
}

// The output demand follows the job's current input demand through the measured
// production ratio: an input speedup opens the output budget the same tick, an
// epoch before the output drain would show it.
TEST_F(TBufferManagerTest, RatioFastPathFollowsInputStep)
{
    auto manager = CreateManager();
    auto states = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("input"), TStreamId("output")));
    auto input = states.Input.at(TStreamId("input"));
    auto output = states.Output.at(TStreamId("output"));

    TStreamUsage inputUsage;
    TStreamUsage outputUsage;
    auto tick = [&] (i64 inputBytes, i64 outputBytes) {
        inputUsage.CumulativeByteIn += inputBytes;
        inputUsage.CumulativeByteOut = inputUsage.CumulativeByteIn;
        input->Update(inputUsage);
        outputUsage.CumulativeByteIn += outputBytes;
        outputUsage.CumulativeByteOut = outputUsage.CumulativeByteIn;
        output->Update(outputUsage);
        Manage(manager);
    };
    // Warm a stable production ratio of 0.5 (input 20 MB/s, output 10 MB/s).
    for (int second = 0; second < 60; ++second) {
        tick(20'000'000, 10'000'000);
    }
    const i64 warmOutputLimit = output->GetLimitBytes();

    // Input steps ×10; the output limit follows within a few ticks, before the
    // output drain reflects it.
    for (int second = 0; second < 3; ++second) {
        tick(200'000'000, 10'000'000);
    }
    EXPECT_GE(output->GetLimitBytes(), static_cast<i64>(2.0 * warmOutputLimit));
}

// A steady pipeline with jittery instantaneous signals must persist a STABLE
// warmup: every wobble past the drift gate turns an otherwise-empty epoch
// transaction into a state write, which on a large installation floods the
// states table with row writes and retryable commit conflicts.
TEST_F(TBufferManagerTest, WarmupStaysStableUnderSteadyJitter)
{
    auto manager = CreateManager();
    auto jobId = TJobId(TGuid::Create());
    auto states = manager->RegisterJob(jobId, CreateJobSpec(TStreamId("input"), TStreamId("output")));
    auto input = states.Input.at(TStreamId("input"));

    TStreamUsage usage;
    auto tick = [&] (int second) {
        // Mean 10 MB/s with 3x tick-to-tick jitter; the offered rate flips even harder.
        i64 bytes = (second % 2 == 0) ? 15'000'000 : 5'000'000;
        usage.CumulativeByteIn += bytes;
        usage.CumulativeByteOut = usage.CumulativeByteIn;
        input->Update(usage);
        input->SetOfferedInflatedBytesPerSecond(second % 2 == 0 ? 30e6 : 1e6);
        states.InputEpochCycleTracker->RecordCycle(second % 2 == 0 ? TDuration::Seconds(2) : TDuration::Seconds(1));
        Manage(manager);
    };

    for (int second = 0; second < 300; ++second) {
        tick(second);
    }
    auto first = manager->GetJobWarmup(jobId);
    ASSERT_TRUE(first.InputSpeeds.contains(TStreamId("input")));

    // An odd tick count lands the second snapshot on the opposite jitter phase.
    for (int second = 300; second < 401; ++second) {
        tick(second);
    }
    auto second = manager->GetJobWarmup(jobId);

    EXPECT_FALSE(WarmupDiffers(first, second));
}

// The production slowdown pattern: a bursty giant fills and fully drains within
// each tick (its PEAK is huge while its instantaneous in-flight is ~0) next to a
// tiny stream with pending data. Peaks of different streams are not simultaneous,
// so pool accounting by peaks would charge the drained burst as if it were still
// resident and cap the tiny neighbour to zero.
TEST_F(TBufferManagerTest, DrainedBurstDoesNotStarveTheTinyNeighbour)
{
    Spec_->InputBuffer->FairSharePool = NYTree::TSize(20_GB);
    Spec_->InputBuffer->JobLimit = NYTree::TSize(20_GB);

    auto manager = CreateManager();
    std::vector<TStreamLimitUsageStatePtr> bursts;
    std::vector<TStreamUsage> burstUsages(2);
    for (int i = 0; i < 2; ++i) {
        auto streamId = TStreamId(Format("burst_%v", i));
        bursts.push_back(
            manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(streamId, TStreamId("output"))).Input.at(streamId));
    }
    auto tiny = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("tiny"), TStreamId("output")))
        .Input.at(TStreamId("tiny"));

    TStreamUsage tinyUsage{.PendingInflatedBytes = 1_MB};
    tiny->Update(tinyUsage);
    for (int i = 0; i < 30; ++i) {
        // Each giant admits ~15 GB and drains it all before the manager looks: the
        // peaks sum to 30 GB against the 20 GB pool, the snapshots to 0.
        for (auto b = 0; b < 2; ++b) {
            auto& usage = burstUsages[b];
            usage.PendingInflatedBytes = 100_GB;
            usage.CumulativeByteIn += 15_GB;
            usage.CumulativeCountIn += 1000;
            bursts[b]->Update(usage);
            usage.CumulativeByteOut = usage.CumulativeByteIn;
            usage.CumulativeCountOut = usage.CumulativeCountIn;
            bursts[b]->Update(usage);
        }
        Manage(manager);
    }

    // Liveness: the drained burst holds no pool room, so the tiny stream keeps a
    // live ~floor limit (the proportional trim may shave a fraction) instead of
    // being capped to zero by the giant's peak.
    EXPECT_GE(tiny->GetLimitBytes(), static_cast<i64>(900_KB));
}

// The production profile that stalled a pipeline: hundreds of paper-sized
// overrides whose configured sum dwarfs the pool (a valid config since v1, where
// overrides live outside the fair share). Neither their paper limits nor their
// in-flight touch the pool; every backlogged fair-share stream must keep a live
// (>= floor) limit.
TEST_F(TBufferManagerTest, PaperOverridesDoNotStarveTheFairShare)
{
    constexpr int OverrideCount = 200;
    constexpr i64 Pool = 250_MB;
    Spec_->InputBuffer->FairSharePool = NYTree::TSize(Pool);
    Spec_->InputBuffer->JobLimit = NYTree::TSize(200_MB);
    auto& overrides = Spec_->InputBuffer->JobOverrides[TComputationId("computation")];
    for (int i = 0; i < OverrideCount; ++i) {
        overrides[TStreamId(Format("ovr_%v", i))] = NYTree::TSize(200_MB); // Σ = 40 GB vs the 250 MB pool.
    }

    auto manager = CreateManager();
    std::vector<TStreamLimitUsageStatePtr> overridden;
    for (int i = 0; i < OverrideCount; ++i) {
        auto streamId = TStreamId(Format("ovr_%v", i));
        overridden.push_back(
            manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(streamId, TStreamId("output"))).Input.at(streamId));
    }
    std::vector<TStreamLimitUsageStatePtr> plain;
    for (int i = 0; i < 4; ++i) {
        auto streamId = TStreamId(Format("plain_%v", i));
        plain.push_back(
            manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(streamId, TStreamId("output"))).Input.at(streamId));
    }

    // One override holds real in-flight; the fair-share streams are backlogged.
    overridden[0]->Update(TStreamUsage{.CumulativeByteIn = 10_MB, .CumulativeCountIn = 10});
    for (const auto& state : plain) {
        state->Update(TStreamUsage{.PendingInflatedBytes = 1_GB});
    }
    for (int i = 0; i < 10; ++i) {
        Manage(manager);
    }

    i64 plainTotal = 0;
    for (const auto& state : plain) {
        // Liveness: a backlogged stream must never sit at a zero limit.
        EXPECT_GE(state->GetLimitBytes(), static_cast<i64>(2_MB));
        plainTotal += state->GetLimitBytes();
    }
    EXPECT_LE(plainTotal, Pool);
    EXPECT_EQ(overridden[0]->GetLimitBytes(), static_cast<i64>(200_MB));
    EXPECT_EQ(overridden[OverrideCount - 1]->GetLimitBytes(), static_cast<i64>(200_MB));
}

// A per-stream override is an absolute value that bypasses everything: pool
// pressure, the job limit, the v2 floor, and headroom probing may not move it in
// either direction (v1 semantics — overridden streams are outside fair share).
TEST_F(TBufferManagerTest, OverrideIsExactAndBypassesEverything)
{
    Spec_->InputBuffer->FairSharePool = NYTree::TSize(1_MB); // far below the overrides
    Spec_->InputBuffer->JobLimit = NYTree::TSize(10_MB);
    const i64 smallOverride = 1_MB;  // below the v2 floor and headroom
    const i64 hugeOverride = 100_MB; // above the job limit
    auto& overrides = Spec_->InputBuffer->JobOverrides[TComputationId("computation")];
    overrides[TStreamId("input")] = NYTree::TSize(smallOverride);
    overrides[TStreamId("other")] = NYTree::TSize(hugeOverride);

    auto manager = CreateManager();
    auto computationSpec = New<TComputationSpec>();
    computationSpec->InputStreamIds.insert(TStreamId("input"));
    computationSpec->InputStreamIds.insert(TStreamId("other"));
    auto partition = New<TPartition>();
    partition->ComputationId = TComputationId("computation");
    auto jobSpec = New<TJobSpec>();
    jobSpec->ComputationSpec = computationSpec;
    jobSpec->Partition = partition;
    auto states = manager->RegisterJob(TJobId(TGuid::Create()), jobSpec);
    auto small = states.Input.at(TStreamId("input"));
    auto huge = states.Input.at(TStreamId("other"));

    // Keep the small override fully utilized (would otherwise grow headroom).
    TStreamUsage usage;
    for (int second = 0; second < 20; ++second) {
        FillAndDrain(small, usage);
        Manage(manager);
    }
    EXPECT_EQ(small->GetLimitBytes(), smallOverride);
    EXPECT_EQ(huge->GetLimitBytes(), hugeOverride);
}

// Toggling enable_v2 off and on again must not strand a stale issuance state that
// defeats the publication hysteresis and keeps an oversized limit.
TEST_F(TBufferManagerTest, StrategyToggleDoesNotStrandLimits)
{
    Spec_->InputBuffer->FairSharePool = NYTree::TSize(1_GB);
    Spec_->InputBuffer->JobLimit = NYTree::TSize(500_MB);
    auto manager = CreateManager();
    auto state = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("input"), TStreamId("output")))
        .Input.at(TStreamId("input"));

    TStreamUsage usage;
    auto runSeconds = [&] (int count) {
        for (int second = 0; second < count; ++second) {
            FillAndDrain(state, usage);
            Manage(manager);
        }
    };

    runSeconds(30);
    EXPECT_GT(state->GetLimitBytes(), static_cast<i64>(10_MB));

    auto v1Spec = CloneYsonStruct(Spec_);
    v1Spec->EnableV2 = false;
    manager->Reconfigure(v1Spec);
    runSeconds(5);

    // On again with nothing pending: the fresh issuance must release the
    // reservation, not inherit whatever the v1 path left behind.
    manager->Reconfigure(CloneYsonStruct(Spec_));
    usage.PendingInflatedBytes = 0;
    usage.CumulativeByteOut = usage.CumulativeByteIn;
    state->Update(usage);
    Manage(manager);

    EXPECT_EQ(state->GetLimitBytes(), 0);
}

// The warmup a manager produces must be the sizing v2 itself uses: feeding
// GetJobWarmup's output back through SeedJob on a fresh manager restores the
// steady-state limit on the first tick instead of ramping from the floor.
TEST_F(TBufferManagerTest, WarmupRoundTripSurvivesRestart)
{
    Spec_->InputBuffer->FairSharePool = NYTree::TSize(2_GB);
    Spec_->InputBuffer->JobLimit = NYTree::TSize(500_MB);

    constexpr i64 EpochSeconds = 60;
    constexpr i64 RatePerSecond = 2_MB;

    auto manager = CreateManager();
    auto jobId = TJobId(TGuid::Create());
    auto states = manager->RegisterJob(jobId, CreateJobSpec(TStreamId("input"), TStreamId("output")));
    auto state = states.Input.at(TStreamId("input"));

    // A latency-bound job: producer trickles in, consumer drains the whole buffer
    // once per epoch — the case a decaying EMA understates.
    TStreamUsage usage;
    i64 produced = 0;
    for (int second = 1; second <= 10 * EpochSeconds; ++second) {
        Now_ += TDuration::Seconds(1);
        TimeProvider_->Set(Now_);
        produced += RatePerSecond;
        usage.PendingInflatedBytes = 1_GB;
        while (state->IsUsageWithinLimits(usage) && usage.CumulativeByteIn < produced) {
            usage.CumulativeByteIn += 100'000;
            state->Update(usage);
        }
        if (second % EpochSeconds == 0) {
            usage.CumulativeByteOut = usage.CumulativeByteIn;
            state->Update(usage);
            states.InputEpochCycleTracker->RecordCycle(TDuration::Seconds(EpochSeconds));
        }
        manager->ManageBuffers();
    }

    const i64 steadyLimit = state->GetLimitBytes();
    auto warmup = manager->GetJobWarmup(jobId);
    ASSERT_TRUE(warmup.InputSpeeds.contains(TStreamId("input")));
    EXPECT_NEAR(warmup.EpochCycleSeconds, EpochSeconds, 1e-6);
    // The persisted speed reflects the burst drain rate, not a decayed mean.
    EXPECT_GE(warmup.InputSpeeds.at(TStreamId("input")), 0.5 * RatePerSecond);

    auto restarted = CreateManager();
    auto restartedJobId = TJobId(TGuid::Create());
    auto restartedState = restarted->RegisterJob(restartedJobId, CreateJobSpec(TStreamId("input"), TStreamId("output")))
        .Input.at(TStreamId("input"));
    restarted->SeedJob(restartedJobId, warmup);
    // The seeded speeds are visible to GetJobWarmup at once: a commit landing
    // before the first manage tick must not persist an empty warmup over the
    // converged one.
    EXPECT_TRUE(restarted->GetJobWarmup(restartedJobId).InputSpeeds.contains(TStreamId("input")));

    restartedState->Update(TStreamUsage{.PendingInflatedBytes = 1_GB});
    Manage(restarted);
    EXPECT_GE(restartedState->GetLimitBytes(), steadyLimit / 2);
}

// After a restart the announced backlog is old, so its offered rate is tiny; the
// seeded demand must keep the sizing until the max-rate estimator measures again,
// not collapse through the drain cap.
TEST_F(TBufferManagerTest, SeededLimitSurvivesStaleOfferedRate)
{
    Spec_->InputBuffer->FairSharePool = NYTree::TSize(2_GB);
    Spec_->InputBuffer->JobLimit = NYTree::TSize(500_MB);
    auto manager = CreateManager();
    auto jobId = TJobId(TGuid::Create());
    auto state = manager->RegisterJob(jobId, CreateJobSpec(TStreamId("input"), TStreamId("output")))
        .Input.at(TStreamId("input"));

    TPartitionBufferWarmup warmup;
    warmup.InputSpeeds[TStreamId("input")] = 20e6;
    warmup.EpochCycleSeconds = 5;
    manager->SeedJob(jobId, warmup);

    state->Update(TStreamUsage{.PendingInflatedBytes = 1_GB});
    state->SetOfferedInflatedBytesPerSecond(1000); // an hour-old backlog
    Manage(manager);

    EXPECT_GE(state->GetLimitBytes(), static_cast<i64>(50_MB));
}

////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_SUITE_P(
    ,
    TBufferStateTest,
    ::testing::Values(
        TBufferStateTestParam{.IsInputBuffer = true, .Name = "InputBuffer"},
        TBufferStateTestParam{.IsInputBuffer = false, .Name = "OutputBuffer"}),
    [] (const auto& info) {
        return info.param.Name;
    });

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

// Per side independently, the max among the worker's matched groups wins; a side
// no matched group overrides falls back to the base pool, and a worker outside
// the overridden groups is unaffected.
TEST(TWorkerGroupPoolOverridesTest, PerSideMaxAndFallback)
{
    auto makeSpec = [] {
        auto makeOneSide = [] (i64 pool) {
            auto side = New<TDynamicBufferStateManagerSpec::TOneSideBufferSpec>();
            side->FairSharePool = NYTree::TSize(pool);
            side->JobGuarantee = NYTree::TSize(0);
            side->JobLimit = NYTree::TSize(1_GB);
            side->MaxDuration = TDuration::Minutes(1);
            return side;
        };
        auto spec = New<TDynamicBufferStateManagerSpec>();
        spec->DemandWindow = TDuration::Minutes(1);
        spec->InputBuffer = makeOneSide(10_MB);
        spec->OutputBuffer = makeOneSide(10_MB);
        spec->InputBuffer->WorkerGroupFairSharePoolOverrides[TWorkerGroupId("fat")] = NYTree::TSize(100_MB);
        spec->InputBuffer->WorkerGroupFairSharePoolOverrides[TWorkerGroupId("misc")] = NYTree::TSize(50_MB);
        spec->OutputBuffer->WorkerGroupFairSharePoolOverrides[TWorkerGroupId("misc")] = NYTree::TSize(1_MB);
        return spec;
    };

    auto run = [&] (std::vector<TWorkerGroupId> groups) {
        auto timeProvider = New<TMockTimeProvider>();
        auto manager = CreateBufferStateManager(
            GetSyncInvoker(),
            New<TMockJobDirectory>(1),
            makeSpec(),
            timeProvider->GetProvider(),
            std::move(groups),
            /*enablePeriodicManagement*/ false);
        auto states = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("input"), TStreamId("output")));
        auto input = states.Input.at(TStreamId("input"));
        auto output = states.Output.at(TStreamId("output"));

        TStreamUsage inputUsage;
        TStreamUsage outputUsage;
        TInstant now = TInstant::Zero();
        for (int second = 0; second < 120; ++second) {
            now += TDuration::Seconds(1);
            timeProvider->Set(now);
            inputUsage.CumulativeByteOut += 10'000'000;
            inputUsage.CumulativeCountOut += 10;
            input->Update(inputUsage);
            outputUsage.CumulativeByteOut += 10'000'000;
            outputUsage.CumulativeCountOut += 10;
            output->Update(outputUsage);
            manager->ManageBuffers();
        }
        return std::pair(input->GetLimitBytes(), output->GetLimitBytes());
    };

    auto [fatInput, fatOutput] = run({TWorkerGroupId("fat"), TWorkerGroupId("misc")});
    EXPECT_GT(fatInput, static_cast<i64>(60_MB));
    EXPECT_LE(fatInput, static_cast<i64>(100_MB));
    EXPECT_LE(fatOutput, static_cast<i64>(2_MB));

    auto [plainInput, plainOutput] = run({TWorkerGroupId("other")});
    EXPECT_LE(plainInput, static_cast<i64>(12_MB));
    EXPECT_GT(plainOutput, static_cast<i64>(5_MB));
}

// Blackbox memory-safety attacks on a realistic installation: a 5 GB pool, 3 jobs
// that may each claim the whole pool, and producers rate-limited to one 500 MB
// admission round per tick (50 connections × 10 MB batches). We assert the hard
// Σissued ≤ pool AND that Σused stays within 1.1 × pool: in-flight is not
// evictable, but the used-aware cap re-trims every round, so a stuck stream lets
// its neighbours admit at most one round (0.1 × pool here) on top of it. Messages
// are 10 MB, so the per-message inflation is negligible and Σused ≈ Σ(in - out).
TEST(TBufferMemoryAttack, RealisticInstallationStaysWithin110Percent)
{
    constexpr i64 Message = 10_MB;
    constexpr i64 Pool = 5_GB;
    constexpr i64 JobLimit = 5_GB;     // a single job may claim the whole pool
    constexpr i64 AdmitRound = 500_MB; // 50 connections × 10 MB per tick

    struct TAttackResult
    {
        i64 PeakLimit = 0;
        i64 PeakUsed = 0;
        std::vector<i64> FinalLimits;
    };

    // One manager, |jobCount| single-input jobs; |drive| fills/drains each job's
    // usage per tick, then we record Σlimit and Σused.
    auto runAttack = [&] (int jobCount, int ticks, auto&& drive) {
        auto timeProvider = New<TMockTimeProvider>();
        auto spec = CreateDefaultDynamicSpec();
        spec->EnableV2 = true;
        spec->InputBuffer->FairSharePool = NYTree::TSize(Pool);
        spec->InputBuffer->JobGuarantee = NYTree::TSize(0);
        spec->InputBuffer->JobLimit = NYTree::TSize(JobLimit);
        auto manager = CreateBufferStateManager(
            GetSyncInvoker(),
            New<TMockJobDirectory>(1),
            spec,
            timeProvider->GetProvider(),
            /*workerGroups*/ {},
            /*enablePeriodicManagement*/ false);

        std::vector<TStreamLimitUsageStatePtr> states;
        std::vector<TStreamUsage> usages(jobCount);
        for (int i = 0; i < jobCount; ++i) {
            auto s = manager->RegisterJob(TJobId(TGuid::Create()), CreateJobSpec(TStreamId("input"), TStreamId("output")));
            states.push_back(s.Input.at(TStreamId("input")));
        }

        TAttackResult result;
        TInstant now = TInstant::Zero();
        for (int tick = 0; tick < ticks; ++tick) {
            now += TDuration::Seconds(1);
            timeProvider->Set(now);
            drive(tick, states, usages);
            manager->ManageBuffers();

            i64 totalLimit = 0;
            i64 totalUsed = 0;
            for (int i = 0; i < jobCount; ++i) {
                totalLimit += states[i]->GetLimitBytes();
                totalUsed += usages[i].CumulativeByteIn - usages[i].CumulativeByteOut;
            }
            result.PeakLimit = std::max(result.PeakLimit, totalLimit);
            result.PeakUsed = std::max(result.PeakUsed, totalUsed);
        }
        for (const auto& state : states) {
            result.FinalLimits.push_back(state->GetLimitBytes());
        }
        return result;
    };

    // Admit at most one round (|AdmitRound|) of 10 MB batches this tick, bounded by
    // the current limit; optionally drain afterwards.
    auto fill = [&] (TStreamUsage& usage, const TStreamLimitUsageStatePtr& state, i64 pending, bool drain) {
        usage.PendingInflatedBytes = pending;
        i64 admitted = 0;
        while (state->IsUsageWithinLimits(usage) && admitted < AdmitRound) {
            usage.CumulativeByteIn += Message;
            ++usage.CumulativeCountIn;
            admitted += Message;
            state->Update(usage);
        }
        if (drain) {
            usage.CumulativeByteOut = usage.CumulativeByteIn;
            usage.CumulativeCountOut = usage.CumulativeCountIn;
        }
        state->Update(usage);
    };

    // Attack 1: 3 greedy jobs fill (rate-limited) and hold, racing for the pool.
    auto greedy = runAttack(3, 200, [&] (int, auto& states, auto& usages) {
        for (int i = 0; i < std::ssize(states); ++i) {
            fill(usages[i], states[i], 100_GB, /*drain*/ false);
        }
    });

    // Attack 2: job 0 fills HALF the pool and goes fully quiescent — never drains
    // AND never updates its usage slot again (a wedged job gets no pushes, offers,
    // or extractions), so only the in-flight snapshot can keep its bytes visible
    // to the pool accounting; jobs 1 and 2 then ramp against the stuck memory and
    // must both live within the remaining half.
    auto handover = runAttack(3, 400, [&] (int tick, auto& states, auto& usages) {
        if (tick < 40) {
            if (usages[0].CumulativeByteIn < Pool / 2) {
                fill(usages[0], states[0], 100_GB, /*drain*/ false);
            }
        } else {
            fill(usages[1], states[1], 100_GB, /*drain*/ false);
            fill(usages[2], states[2], 100_GB, /*drain*/ false);
        }
    });

    Cout << "=== TBufferMemoryAttack (5 GB pool) ===" << Endl;
    Cout << "  greedy:   peakLimit=" << greedy.PeakLimit << " peakUsed=" << greedy.PeakUsed << " pool=" << Pool << Endl;
    Cout << "  handover: peakLimit=" << handover.PeakLimit << " peakUsed=" << handover.PeakUsed << " pool=" << Pool << Endl;

    const i64 UsedBound = 11 * Pool / 10;
    EXPECT_LE(greedy.PeakLimit, Pool);
    EXPECT_LE(greedy.PeakUsed, UsedBound);
    EXPECT_LE(handover.PeakLimit, Pool);
    EXPECT_LE(handover.PeakUsed, UsedBound);

    // Liveness: the upper bounds alone can hold with everyone starved to zero
    // (a stalled pipeline satisfies every cap). Racing greedy jobs must all keep
    // live limits, and jobs ramping against stuck memory must make progress.
    for (auto limit : greedy.FinalLimits) {
        EXPECT_GT(limit, 0);
    }
    EXPECT_GT(handover.FinalLimits[1], 0);
    EXPECT_GT(handover.FinalLimits[2], 0);
}

} // namespace
} // namespace NYT::NFlow::NWorker
