#include <yt/yt/flow/library/cpp/computation/message_filter.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr CreateFilterSchema()
{
    return New<TTableSchema>(std::vector{
        TColumnSchema("skip", EValueType::Boolean),
        TColumnSchema("other", EValueType::Boolean),
    });
}

TMessage CreateFilterMessage(const TTableSchemaPtr& schema, std::optional<bool> skip)
{
    TMessageBuilder builder(TStreamId("stream"), schema);
    builder.SetMessageId(TMessageId("message"));
    builder.SetSystemTimestamp(TSystemTimestamp(1));
    builder.SetEventTimestamp(TSystemTimestamp(1));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1));
    if (skip.has_value()) {
        builder.Payload().Set<bool>(*skip, "skip");
    } else {
        builder.Payload().SetValue(MakeUnversionedNullValue(), "skip");
    }
    builder.Payload().Set<bool>(!skip.value_or(false), "other");
    return builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFlowQueryMessageFilterTest, ReconfiguresSupportedPredicates)
{
    auto schema = CreateFilterSchema();
    auto skipped = CreateFilterMessage(schema, /*skip*/ true);
    auto kept = CreateFilterMessage(schema, /*skip*/ false);
    auto filter = CreateMessageFilter();
    EXPECT_FALSE(filter->IsEnabled());
    EXPECT_FALSE(filter->ShouldSkip(skipped));

    filter->Reconfigure("true");
    EXPECT_TRUE(filter->IsEnabled());
    EXPECT_TRUE(filter->ShouldSkip(kept));

    filter->Reconfigure("false");
    EXPECT_FALSE(filter->ShouldSkip(skipped));

    filter->Reconfigure("[skip]");
    EXPECT_TRUE(filter->ShouldSkip(skipped));
    EXPECT_FALSE(filter->ShouldSkip(kept));

    filter->Reconfigure(std::nullopt);
    EXPECT_FALSE(filter->IsEnabled());
    EXPECT_FALSE(filter->ShouldSkip(skipped));
}

TEST(TFlowQueryMessageFilterTest, ResolvesReferencesForEachSchema)
{
    auto schema = CreateFilterSchema();
    auto reorderedSchema = New<TTableSchema>(std::vector{
        TColumnSchema("other", EValueType::Boolean),
        TColumnSchema("skip", EValueType::Boolean),
    });
    auto filter = CreateMessageFilter("[skip]");

    EXPECT_TRUE(filter->ShouldSkip(CreateFilterMessage(schema, /*skip*/ true)));
    EXPECT_FALSE(filter->ShouldSkip(CreateFilterMessage(reorderedSchema, /*skip*/ false)));
    EXPECT_FALSE(filter->ShouldSkip(CreateFilterMessage(schema, /*skip*/ false)));
    EXPECT_TRUE(filter->ShouldSkip(CreateFilterMessage(reorderedSchema, /*skip*/ true)));
}

TEST(TFlowQueryMessageFilterTest, RejectsNullAndContinuesWithTheSameSchema)
{
    auto schema = CreateFilterSchema();
    auto filter = CreateMessageFilter("[skip]");

    EXPECT_THROW_WITH_SUBSTRING(
        filter->ShouldSkip(CreateFilterMessage(schema, /*skip*/ std::nullopt)),
        "must evaluate to a boolean value");
    EXPECT_FALSE(filter->ShouldSkip(CreateFilterMessage(schema, /*skip*/ false)));
    EXPECT_TRUE(filter->ShouldSkip(CreateFilterMessage(schema, /*skip*/ true)));
}

TEST(TFlowQueryMessageFilterTest, PartitionsMessagesAndAccountsForSkippedBytes)
{
    auto schema = CreateFilterSchema();
    auto keptFirst = New<TInputMessage>(CreateFilterMessage(schema, /*skip*/ false), TKey());
    auto skippedFirst = New<TInputMessage>(CreateFilterMessage(schema, /*skip*/ true), TKey());
    auto keptLast = New<TInputMessage>(CreateFilterMessage(schema, /*skip*/ false), TKey());
    auto skippedLast = New<TInputMessage>(CreateFilterMessage(schema, /*skip*/ true), TKey());
    auto filter = CreateMessageFilter("[skip]");

    auto result = filter->Partition({keptFirst, skippedFirst, keptLast, skippedLast});

    EXPECT_THAT(result.Kept, ::testing::ElementsAre(keptFirst, keptLast));
    EXPECT_THAT(result.Skipped, ::testing::ElementsAre(skippedFirst, skippedLast));
    ASSERT_EQ(result.SkippedStatistics.size(), 1u);
    auto statisticsIt = result.SkippedStatistics.find(skippedFirst->StreamId);
    ASSERT_NE(statisticsIt, result.SkippedStatistics.end());
    EXPECT_EQ(statisticsIt->second.Count, 2);
    EXPECT_EQ(statisticsIt->second.ByteSize, skippedFirst->ByteSize + skippedLast->ByteSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
