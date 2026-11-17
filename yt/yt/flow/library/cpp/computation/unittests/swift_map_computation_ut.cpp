#include "computation_test_harness.h"

#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NComputationTesting;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;
using namespace testing;

////////////////////////////////////////////////////////////////////////////////

const TStreamId InputStreamId("input");
const TStreamId TimerStreamId("timer");
const TStreamId OutputStreamId("output");
const TMessageId InputMessageId("input-1");
const TKey TimerKey = MakeKey(static_cast<ui64>(7));

//! What the computation under test saw; touched from the serialized invoker only.
struct TPersistenceProbe
{
    //! Whether the input message adds a timer.
    bool AddTimer = true;

    bool InputSent = false;
    int Epochs = 0;
    int Firings = 0;
    //! Committed transactions so far.
    int Commits = 0;
    std::optional<int> CommitsWhenInputProcessed;
    std::optional<int> CommitsWhenInputPersisted;
};

class TTimerRegisteringSwiftMap
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::DoProcessMessage;
    using TSwiftMapComputation::DoProcessTimer;
    using TSwiftMapComputation::TSwiftMapComputation;

    TPersistenceProbe* Probe = nullptr;

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override
    {
        THROW_ERROR_EXCEPTION_IF(++Probe->Epochs > 1000, "Too many epochs");
        if (Probe->CommitsWhenInputPersisted && TimerStore_->GetCount() == 0) {
            THROW_ERROR_EXCEPTION("Test finished");
        }
        TSwiftMapComputation::DoProcess(std::move(input), std::move(output));
    }

    void DoProcessMessage(const TInputMessageConstPtr& /*message*/, IOutputCollectorPtr output) override
    {
        Probe->CommitsWhenInputProcessed = Probe->Commits;
        if (Probe->AddTimer) {
            output->AddTimer(TimerStreamId, TSystemTimestamp(1));
        }
    }

    void DoProcessTimer(const TInputTimerConstPtr& /*timer*/, IOutputCollectorPtr /*output*/) override
    {
        ++Probe->Firings;
    }
};

YT_FLOW_DEFINE_COMPUTATION(TTimerRegisteringSwiftMap);

//! Feeds a single input message and records when it is marked persisted.
class TSwiftMapHarness
{
public:
    const TComputationSpecPtr Spec = New<TComputationSpec>();
    TPersistenceProbe Probe;

    TSwiftMapHarness()
    {
        Spec->ComputationClassName = TypeName<TTimerRegisteringSwiftMap>();
        Spec->GroupBySchema = GetTestKeySchema();
        Spec->InputStreamIds.insert(InputStreamId);
        Spec->OutputStreamIds.insert(OutputStreamId);
        auto timerSpec = New<TTimerSpec>();
        timerSpec->TimeType = ETimeType::CurrentTime;
        timerSpec->DeduplicateEqualTimestamps = false;
        Spec->TimerStreams[TimerStreamId] = timerSpec;
        Spec->StreamsDependency[TimerStreamId] = {InputStreamId};
        // An output of a Swift computation must not depend on its timers.
        Spec->StreamsDependency[OutputStreamId] = {InputStreamId};
        Spec->Parameters = ConvertTo<IMapNodePtr>(TYsonStringBuf("{}"));
    }

    //! Runs the computation until it throws.
    TError Run()
    {
        return RunComputation(
            {
                .Spec = Spec,
                .OutputSchema = New<TTableSchema>(std::vector{TColumnSchema("value", EValueType::Int64)}),
                .GetNextBatch = [this] {
                    std::vector<TInputMessageConstPtr> messages;
                    if (!std::exchange(Probe.InputSent, true)) {
                        messages.push_back(MakeTestInputMessage(InputStreamId, InputMessageId, TimerKey));
                    }
                    return messages;
                },
                .OnCommit = [this] {
                    ++Probe.Commits;
                },
                .OnMarkPersisted = [this] (std::span<const TMessageId> messageIds) {
                    for (const auto& messageId : messageIds) {
                        if (messageId == InputMessageId && !Probe.CommitsWhenInputPersisted) {
                            Probe.CommitsWhenInputPersisted = Probe.Commits;
                        }
                    }
                },
            },
            [this] (TComputationContextPtr context, TDynamicComputationContextPtr dynamicContext) {
                auto computation = New<TTimerRegisteringSwiftMap>(std::move(context), std::move(dynamicContext));
                computation->Probe = &Probe;
                return computation;
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TSwiftMapComputationTest, InputIsPersistedAfterTheCommitThatRegistersItsTimer)
{
    TSwiftMapHarness harness;
    EXPECT_THAT(ToString(harness.Run()), HasSubstr("Test finished"));

    const auto& probe = harness.Probe;
    // An upstream that took the input as persisted before the commit would lose the timer on a restart.
    ASSERT_TRUE(probe.CommitsWhenInputProcessed);
    ASSERT_TRUE(probe.CommitsWhenInputPersisted);
    EXPECT_GT(*probe.CommitsWhenInputPersisted, *probe.CommitsWhenInputProcessed);
    EXPECT_EQ(probe.Firings, 1);
}

TEST(TSwiftMapComputationTest, InputWithoutTimersIsPersistedInItsEpoch)
{
    TSwiftMapHarness harness;
    harness.Probe.AddTimer = false;
    EXPECT_THAT(ToString(harness.Run()), HasSubstr("Test finished"));

    const auto& probe = harness.Probe;
    ASSERT_TRUE(probe.CommitsWhenInputProcessed);
    ASSERT_TRUE(probe.CommitsWhenInputPersisted);
    EXPECT_EQ(*probe.CommitsWhenInputPersisted, *probe.CommitsWhenInputProcessed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
