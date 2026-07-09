#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NExample {

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NFlow;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

//! Reads raw queue rows and emits them on the "data" stream.
class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(
            MakeUnversionedInt64Value(GetColumnValue<i64>(message, "value")),
            "value");
        auto out = builder.Finish();
        out.MessageId = message.MessageId;
        output->AddMessage(std::move(out));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

//! Passthrough that spends one throttler quota unit per message before emitting.
//! Exercises the per-job distributed throttler client end-to-end.
class TThrottledPassthrough
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        WaitFor(GetThrottler(TThrottlerId("api"))->Throttle(1))
            .ThrowOnError();

        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(
            MakeUnversionedInt64Value(GetColumnValue<i64>(message, "value")),
            "value");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TThrottledPassthrough);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExample

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
