#include <yt/yt/flow/library/cpp/process_function/host/swift_stable_batch_source_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const std::optional<std::string> ExpressionA = "event_id % 2 = 0";
const std::optional<std::string> ExpressionB = "event_id % 3 = 0";

TEST(TSkipIfMemoryTest, FirstSightGroupsRememberExpression)
{
    auto memory = New<TSkipIfMemory>();
    EXPECT_FALSE(SelectSkipIfExpression(memory.Get(), TMessageId("a"), TMessageId("b"), std::nullopt).Changed);
    EXPECT_FALSE(SelectSkipIfExpression(memory.Get(), TMessageId("c"), TMessageId("c"), std::nullopt).Changed);

    auto selection = SelectSkipIfExpression(memory.Get(), TMessageId("d"), TMessageId("e"), ExpressionA);
    EXPECT_TRUE(selection.Changed);
    EXPECT_EQ(selection.Expression, ExpressionA);

    // Equal expressions of consecutive groups share one entry.
    ASSERT_EQ(memory->size(), 2);
    EXPECT_EQ(memory->front(), std::pair(TMessageId("c"), std::optional<std::string>()));
    EXPECT_EQ(memory->back(), std::pair(TMessageId("e"), ExpressionA));
}

TEST(TSkipIfMemoryTest, ReplayedGroupGetsExpressionOfFirstRead)
{
    auto memory = New<TSkipIfMemory>();
    SelectSkipIfExpression(memory.Get(), TMessageId("a"), TMessageId("b"), std::nullopt);
    SelectSkipIfExpression(memory.Get(), TMessageId("c"), TMessageId("d"), ExpressionA);

    EXPECT_EQ(SelectSkipIfExpression(memory.Get(), TMessageId("a"), TMessageId("b"), ExpressionB).Expression, std::nullopt);
    // A trim may cut the beginning of a group.
    auto selection = SelectSkipIfExpression(memory.Get(), TMessageId("d"), TMessageId("d"), ExpressionB);
    EXPECT_EQ(selection.Expression, ExpressionA);
    EXPECT_FALSE(selection.Changed);
    EXPECT_EQ(memory->size(), 2);
}

TEST(TSkipIfMemoryTest, PersistedGroupsAreForgotten)
{
    auto memory = New<TSkipIfMemory>();
    SelectSkipIfExpression(memory.Get(), TMessageId("a"), TMessageId("b"), std::nullopt);
    SelectSkipIfExpression(memory.Get(), TMessageId("c"), TMessageId("d"), ExpressionA);

    memory->AdvanceExclusive(TMessageId("c"));
    EXPECT_EQ(SelectSkipIfExpression(memory.Get(), TMessageId("c"), TMessageId("d"), ExpressionB).Expression, ExpressionA);

    memory->AdvanceExclusive(TMessageId("e"));
    EXPECT_TRUE(memory->empty());
    auto selection = SelectSkipIfExpression(memory.Get(), TMessageId("e"), TMessageId("f"), ExpressionB);
    EXPECT_EQ(selection.Expression, ExpressionB);
    EXPECT_FALSE(selection.Changed);
}

////////////////////////////////////////////////////////////////////////////////

TComputationSpecPtr MakeSpec(TStringBuf sourceClassName, TStringBuf watermarkStrategy = "{}")
{
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonString(Format(R"({
        computation_class_name = "NYT::NFlow::TSwiftStableBatchSourceComputation";
        output_stream_ids = [out];
        source_streams = {
            input = {
                source_class_name = %Qv;
            };
        };
        watermark_strategy = %v;
    })",
        sourceClassName,
        watermarkStrategy)));
    return spec;
}

TEST(TSwiftStableBatchSourceValidationTest, AcceptsReplayStableSource)
{
    EXPECT_NO_THROW(ValidateSwiftStableBatchSourceSpecSpecifics(*MakeSpec("NYT::NFlow::TQueueSource")));
}

TEST(TSwiftStableBatchSourceValidationTest, RejectsSourceWithoutReplayStability)
{
    ASSERT_THAT(TRegistry::Get()->GetSourceTypeNames(), testing::Contains("NYT::NFlow::TRandomSource"));
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateSwiftStableBatchSourceSpecSpecifics(*MakeSpec("NYT::NFlow::TRandomSource")),
        "replay-stable");
}

TEST(TSwiftStableBatchSourceValidationTest, RejectsReadDelays)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateSwiftStableBatchSourceSpecSpecifics(*MakeSpec(
            "NYT::NFlow::TQueueSource",
            "{watermark_alignment = {group_name = g; read_delays = {input = 10000}}}")),
        "read_delays");
}

TEST(TSwiftStableBatchSourceValidationTest, IsRegistered)
{
    EXPECT_THAT(
        TRegistry::Get()->GetComputationTypeNames(),
        testing::Contains("NYT::NFlow::TSwiftStableBatchSourceComputation"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
