#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <util/string/cast.h>

namespace NExample {

using namespace NYT;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

// Both reducers do byte-for-byte the same work, so the only difference between the variants
// is computation vs process-function dispatch — the delta in worker CPU per message is the
// process-function overhead.
std::atomic<ui64> DummyAccumulator = 0;

////////////////////////////////////////////////////////////////////////////////

// Baseline: a hand-written swift-map computation (same as pure_swift_high_throughput).
class TReducer
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::TSwiftMapComputation;

    void DoProcessMessage(
        const TMessage& message,
        IOutputCollectorPtr /*output*/) override
    {
        DummyAccumulator += FromString<ui64>(GetColumnValue<std::string>(message, "key"));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReducer);

////////////////////////////////////////////////////////////////////////////////

// The equivalent process function: identical per-message body, hosted by
// TProcessFunctionSwiftMapComputation (the swift-map adapter). IProcessFunction's ProcessMessage
// is the apples-to-apples analog of the computation's per-message DoProcessMessage.
class TReducerFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        DummyAccumulator += FromString<ui64>(GetColumnValue<std::string>(message, "key"));
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TReducerFunction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExample

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
