#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto builder = MakeOutputMessageBuilder("data");
        for (i64 i = 0; i < GetColumnValue<i64>(message, "repeat"); ++i) {
            builder.Payload().Set(GetColumnValue<TStringBuf>(message, "data"), "data");
            output->AddMessage(builder.Finish());
        }
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
