#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace testing;

////////////////////////////////////////////////////////////////////////////////

//! Returns a pipeline whose transform has the timer stream "fetch_timer" with |timerSpec| and |extraTimerStreams|;
//! |computationOptions| are extra options of the transform.
TPipelineSpecPtr MakePipelineSpec(TStringBuf timerSpec, TStringBuf computationOptions = "", TStringBuf extraTimerStreams = "")
{
    auto yson = Format(R"##(
        {
            computations = {
                reader = {
                    computation_class_name = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
                    source_streams = {
                        random = { source_class_name = "NYT::NFlow::TRandomSource"; };
                    };
                    output_stream_ids = [ events ];
                    streams_dependency = { events = [ random ]; };
                };
                fetcher = {
                    computation_class_name = "NYT::NFlow::TPassthroughComputation";
                    group_by_schema = [];
                    input_stream_ids = [ events ];
                    timer_streams = { fetch_timer = %v; %v };
                    streams_dependency = { fetch_timer = [ events ]; };
                    %v
                };
            };
            streams = {
                events = { schema = []; };
            };
        }
    )##",
        timerSpec,
        extraTimerStreams,
        computationOptions);
    return ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(yson));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSwiftTimerSpecValidationTest, RejectsDeduplicateEqualTimestamps)
{
    auto spec = MakePipelineSpec("{ swift = %true; deduplicate_equal_timestamps = %true; }");
    EXPECT_THROW_THAT(
        ValidatePipelineSpec(spec),
        testing::AllOf(HasSubstr("Swift timer stream \"fetch_timer\""), HasSubstr("deduplicate_equal_timestamps")));
}

TEST(TSwiftTimerSpecValidationTest, RejectsPagedTimerStore)
{
    auto spec = MakePipelineSpec(
        "{ swift = %true; deduplicate_equal_timestamps = %false; }",
        "timer_store_backend = paged;");
    EXPECT_THROW_THAT(
        ValidatePipelineSpec(spec),
        testing::AllOf(HasSubstr("Swift timer stream \"fetch_timer\""), HasSubstr("paged")));
}

TEST(TSwiftTimerSpecValidationTest, RejectsTimerStreamInGroupOfSwiftStream)
{
    // Both streams wait for the event watermark of "events".
    auto spec = MakePipelineSpec(
        "{ swift = %true; deduplicate_equal_timestamps = %false; }",
        /*computationOptions*/ "",
        "plain_timer = {};");
    EXPECT_THROW_THAT(
        ValidatePipelineSpec(spec),
        testing::AllOf(HasSubstr("Swift timer stream \"fetch_timer\""), HasSubstr("plain_timer")));
}

TEST(TSwiftTimerSpecValidationTest, RejectsCurrentTimeStreamAlongWithCurrentTimeSwiftStream)
{
    auto spec = MakePipelineSpec(
        "{ swift = %true; deduplicate_equal_timestamps = %false; time_type = current_time; }",
        /*computationOptions*/ "",
        "plain_timer = { time_type = current_time; };");
    EXPECT_THROW_THAT(
        ValidatePipelineSpec(spec),
        testing::AllOf(HasSubstr("Swift timer stream \"fetch_timer\""), HasSubstr("plain_timer")));
}

TEST(TSwiftTimerSpecValidationTest, AcceptsTimerStreamOutOfGroupOfSwiftStream)
{
    auto spec = MakePipelineSpec(
        "{ swift = %true; deduplicate_equal_timestamps = %false; time_type = current_time; }",
        /*computationOptions*/ "",
        "plain_timer = {};");
    EXPECT_NO_THROW(ValidatePipelineSpec(spec));
}

TEST(TSwiftTimerSpecValidationTest, AcceptsTransformWithSwiftTimerStream)
{
    auto spec = MakePipelineSpec("{ swift = %true; deduplicate_equal_timestamps = %false; }");
    EXPECT_TRUE(GetOrCrash(GetOrCrash(spec->Computations, "fetcher")->TimerStreams, TStreamId("fetch_timer"))->Swift);
    EXPECT_NO_THROW(ValidatePipelineSpec(spec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
