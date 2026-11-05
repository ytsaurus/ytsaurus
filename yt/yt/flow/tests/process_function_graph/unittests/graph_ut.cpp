#include <yt/yt/flow/tests/process_function_graph/functions/functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/process_function_graph_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow::NProcessFunctionGraphTest {
namespace {

using namespace NTesting;

////////////////////////////////////////////////////////////////////////////////

TEST(TProcessFunctionGraphExampleTest, CountsBranchesAndMergesThem)
{
    // [BEGIN process_function_graph_test_harness]
    TTestStateEnvironment environment;
    TProcessFunctionGraphTestHarness graph(environment, R"({
        input_stream_ids = [left; right];
        output_stream_ids = [result];
        process_function_graph = {
            nodes = {
                left = {
                    processing_function = "NYT::NFlow::NProcessFunctionGraphTest::TStateAndTimerFunction";
                    processing_function_parameters = {increment = 2;};
                    input_stream_ids = [left];
                    output_stream_ids = [left_counts];
                    timer_streams = {timer = {time_type = current_time;};};
                };
                right = {
                    processing_function = "NYT::NFlow::NProcessFunctionGraphTest::TStateAndTimerFunction";
                    processing_function_parameters = {increment = 3;};
                    input_stream_ids = [right];
                    output_stream_ids = [right_counts];
                    timer_streams = {timer = {time_type = current_time;};};
                };
                merge = {
                    processing_function = "NYT::NFlow::NProcessFunctionGraphTest::TMergeFunction";
                    input_stream_ids = [left_counts; right_counts];
                    output_stream_ids = [result];
                };
            };
        };
    })");
    graph
        .RegisterStream<TKeyMessage>("left")
        .RegisterStream<TKeyMessage>("right")
        .RegisterStream<TStageMessage>("left_counts")
        .RegisterStream<TStageMessage>("right_counts")
        .RegisterStream<TFinalMessage>("result");

    auto key = MakeKey<ui64>(7);
    auto fillKey = [] (TMessageBuilder& builder) {
        builder.Payload().Set<ui64>(7, "key");
    };
    graph.RunEpoch({graph.MakeMessage("left", key, fillKey)});
    ASSERT_EQ(graph.GetMessages().size(), 1u);
    EXPECT_EQ(graph.ReadNodeState<i64>("left", "count", key), 2);

    // Timers scheduled in an epoch fire in the next one.
    graph.RunEpoch({}, graph.MakeFiringTimers());
    EXPECT_EQ(graph.GetMessages().size(), 1u);
    // [END process_function_graph_test_harness]
    EXPECT_EQ(graph.ReadNodeState<i64>("merge", "count", key), 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NProcessFunctionGraphTest
