#include <yt/cpp/roren/bigrt/parse_graph.h>

#include <yt/cpp/roren/bigrt/bigrt.h>
#include <yt/cpp/roren/bigrt/concurrency_transforms.h>
#include <yt/cpp/roren/bigrt/proto/config.pb.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>
#include <bigrt/lib/processing/state_manager/generic/manager.h>
#include <bigrt/lib/processing/state_manager/generic/factory.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NRoren;
using namespace NRoren::NPrivate;

TEST(ParseGraph, Simple)
{
    auto executor = ::MakeIntrusive<TProgram::TExecutorStub>();
    auto pipeline = MakePipeline(executor);
    pipeline
        | ReadMessageBatch()
        | ParDo([] (const NBigRT::TMessageBatch& batch) {
            return batch.Messages.size();
        }, {.Name = "first"})
        | StartConcurrencyBlock(TConcurrentBlockConfig{})
        | ParDo([] (const size_t& x) {
            return x + 2;
        }, {.Name = "second"})
        | ParDo([] (const size_t&) {
            // Do nothing
        }, {.Name = "third"});

    auto result = ParseBigRtPipeline(pipeline);

    const TString expected =
        "TSerializedExecutionBlock\n"
        "  ParDo: TParDoTree{first}\n"
        "  Outputs:\n"
        "    TConcurrentExecutionBlock\n"
        "      ParDo: TParDoTree{second, third}\n";
    EXPECT_EQ(result.ExecutionBlock->GetDebugDescription(), expected);
}

class TMyStatefulParDo
    : public IStatefulDoFn<TKV<TString, int>, int, std::vector<int>>
{
public:
    void Do(const TInputRow&, TOutput<TOutputRow>&, TState&) override
    {
        Y_FAIL("not implemented");
    }
};

TEST(ParseGraph, WithStateful)
{
    auto executor = ::MakeIntrusive<TProgram::TExecutorStub>();
    auto pipeline = MakePipeline(executor);
    auto pState = MakeBigRtPState<TString, std::vector<int>>(pipeline, nullptr);

    pipeline
        | ReadMessageBatch()
        | ParDo([] (const NBigRT::TMessageBatch& batch) -> TKV<TString, int> {
            return {"foo", batch.Messages.size()};
        }, {.Name = "first-stateless"})
        | StatefulParDo(pState, ::MakeIntrusive<TMyStatefulParDo>(), {.Name = "second-stateful"})
        | ParDo([] (const int&) {
            // Do nothing
        }, {.Name = "third-stateless"});

    auto result = ParseBigRtPipeline(pipeline);

    const TString expected =
        "TSerializedExecutionBlock\n"
        "  ParDo: TParDoTree{first-stateless, second-stateful (wrapped), third-stateless}\n";

    EXPECT_EQ(result.ExecutionBlock->GetDebugDescription(), expected);
}
