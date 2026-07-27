#include <yt/yt/flow/library/cpp/worker/buffer_state_manager.h>
#include <yt/yt/flow/library/cpp/worker/job_spec.h>

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

TDynamicBufferStateManagerSpecPtr CreateDynamicSpec(
    i64 inputGuarantee,
    i64 inputLimit,
    i64 outputGuarantee,
    i64 outputLimit)
{
    auto makeOneSide = [] (i64 guarantee, i64 limit) {
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

TJobSpecPtr CreateJobSpec(const TStreamId& inputStreamId, const TStreamId& outputStreamId)
{
    auto computationSpec = New<TComputationSpec>();
    computationSpec->InputStreamIds.insert(inputStreamId);
    computationSpec->OutputStreamIds.insert(outputStreamId);

    auto partition = New<TPartition>();
    partition->ComputationId = TComputationId("computation");

    auto jobSpec = New<TJobSpec>();
    jobSpec->ComputationSpec = computationSpec;
    jobSpec->Partition = partition;
    return jobSpec;
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
        std::move(timeProvider));

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
            std::move(groups));
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

} // namespace
} // namespace NYT::NFlow::NWorker
