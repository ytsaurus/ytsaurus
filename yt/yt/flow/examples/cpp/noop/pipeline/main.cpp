#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

// Reads messages from a source and does nothing with them.
class TNoopComputation
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/) override
    { }
};

YT_FLOW_DEFINE_COMPUTATION(TNoopComputation);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return TSimpleRunnerProgram().Run(argc, argv);
}
