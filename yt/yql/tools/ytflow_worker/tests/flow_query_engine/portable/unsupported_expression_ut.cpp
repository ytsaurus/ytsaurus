#include <yt/yt/flow/library/cpp/computation/message_filter.h>

#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// Support for CreateMessageFilter in YQL flows may be dropped entirely (YQLOVERYT-505).
TEST(TPortableFlowQueryTest, RejectsUnsupportedFilterLazilyAndRecovers)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::Int64),
    });
    auto anotherSchema = New<TTableSchema>(std::vector{
        TColumnSchema("other", EValueType::Boolean),
        TColumnSchema("value", EValueType::Int64),
    });
    auto filter = CreateMessageFilter("is_null(value)");
    ASSERT_TRUE(filter->IsEnabled());
    EXPECT_TRUE(filter->Partition(/*messages*/ {}).Kept.empty());

    std::vector<TMessage> messages;
    for (const auto& payloadSchema : {schema, anotherSchema}) {
        TMessageBuilder builder(TStreamId("stream"), payloadSchema);
        builder.SetMessageId(TMessageId("message"));
        builder.Payload().Set<i64>(7, "value");
        messages.push_back(builder.Finish());

        EXPECT_THROW_WITH_SUBSTRING(
            filter->ShouldSkip(messages.back()),
            "Unsupported portable \"function\" \"is_null\"");
        EXPECT_THROW_WITH_SUBSTRING(
            filter->ShouldSkip(messages.back()),
            "Unsupported portable \"function\" \"is_null\"");
    }

    filter->Reconfigure("true");
    for (const auto& message : messages) {
        EXPECT_TRUE(filter->ShouldSkip(message));
    }
}

TEST(TPortableFlowQueryTest, RejectsUnsupportedColumnThroughFlowCacheAndRecovers)
{
    auto cache = CreateFastColumnEvaluatorCache();
    auto unsupportedSchema = New<TTableSchema>(std::vector{
        TColumnSchema("hash", EValueType::Uint64, ESortOrder::Ascending)
            .SetExpression("farm_hash(is_null(value))"),
        TColumnSchema("value", EValueType::Uint64),
    });

    EXPECT_THROW_WITH_SUBSTRING(
        cache->Find(unsupportedSchema),
        "Unsupported portable \"function\" \"is_null\"");
    EXPECT_THROW_WITH_SUBSTRING(
        cache->Find(unsupportedSchema),
        "Unsupported portable \"function\" \"is_null\"");

    auto supportedSchema = New<TTableSchema>(std::vector{
        TColumnSchema("tablet", EValueType::Uint64, ESortOrder::Ascending)
            .SetExpression("value % 7u"),
        TColumnSchema("value", EValueType::Uint64),
    });
    auto evaluator = cache->Find(supportedSchema);
    auto rowBuffer = New<TRowBuffer>();
    auto row = rowBuffer->AllocateUnversioned(/*valueCount*/ 2);
    row[0] = MakeUnversionedNullValue(/*id*/ 0);
    row[1] = MakeUnversionedUint64Value(17, /*id*/ 1);

    evaluator->EvaluateKeys(row, rowBuffer, /*preserveColumnsIds*/ false);
    EXPECT_EQ(row[0], MakeUnversionedUint64Value(3, /*id*/ 0));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
