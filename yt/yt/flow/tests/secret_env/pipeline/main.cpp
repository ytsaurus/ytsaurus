#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/core/misc/error.h>

#include <util/system/env.h>

using namespace NYT;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

// Reads messages from a random source and asserts that the secret declared via vanilla.secret_env
// is visible inside the job as a plain env var. If the secret did not arrive, the job throws and
// the pipeline never completes — so completion proves secret_env forwarding works end-to-end.
class TSecretChecker
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/) override
    {
        auto secret = GetEnv("YT_MY_SECRET");
        THROW_ERROR_EXCEPTION_UNLESS(secret == "5",
            "YT_MY_SECRET is not visible in the vanilla job (got %Qv)",
            secret);
    }
};

YT_FLOW_DEFINE_COMPUTATION(TSecretChecker);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return TSimpleRunnerProgram().Run(argc, argv);
}
