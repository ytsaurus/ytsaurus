#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TFlowQueryGroupByTest
    : public ::testing::Test
{
protected:
    const IJobDirectoryPtr Directory_ = CreateJobDirectory(
        CreatePayloadConverterCache(CreateFastColumnEvaluatorCache()),
        NLogging::TLogger());

    TJobDirectorySnapshotPtr Configure(std::vector<TColumnSchema> keyColumns, const std::string& expression)
    {
        keyColumns.insert(
            keyColumns.begin(),
            TColumnSchema("hash", EValueType::Uint64)
                .SetRequired(true)
                .SetExpression(expression));
        for (auto& column : keyColumns) {
            column.SetSortOrder(ESortOrder::Ascending);
        }

        auto computation = New<TComputationSpec>();
        computation->GroupBySchema = New<TTableSchema>(std::move(keyColumns));
        auto pipeline = New<TPipelineSpec>();
        pipeline->Computations["grouped"] = std::move(computation);
        Directory_->Reconfigure(New<TFlowLayout>(), pipeline);
        return Directory_->GetSnapshot();
    }
};

TEST_F(TFlowQueryGroupByTest, HistoricalHashExpressions)
{
    auto sourceSchema = New<TTableSchema>(std::vector{
        TColumnSchema("key", EValueType::String),
    });
    TMessageBuilder builder("input", sourceSchema);
    builder.Payload().SetValue(MakeUnversionedStringValue("abc"));
    auto message = builder.Finish();

    const std::vector<std::pair<std::string, ui64>> cases{
        {"farm_hash(key)", 0x7b15151e28709746ULL},
        {"farm_hash([key])", 0x7b15151e28709746ULL},
        {"farm_hash(\"key\")", 0x407e25082fdedf2cULL},
    };
    for (const auto& [expression, expectedHash] : cases) {
        SCOPED_TRACE(expression);
        auto snapshot = Configure(sourceSchema->Columns(), expression);
        auto key = snapshot->ComputeMessageKey("grouped", message);
        ASSERT_TRUE(key);
        EXPECT_EQ(MakeKey(expectedHash, "abc"), *key);
    }
}

TEST_F(TFlowQueryGroupByTest, CompositeKeyPreservesNullAndReordersColumns)
{
    auto sourceSchema = New<TTableSchema>(std::vector{
        TColumnSchema("number", EValueType::Uint64),
        TColumnSchema("unused", EValueType::Boolean),
        TColumnSchema("key", EValueType::String),
    });
    auto snapshot = Configure(
        {
            TColumnSchema("key", EValueType::String),
            TColumnSchema("number", EValueType::Uint64),
        },
        "farm_hash([key], [number])");

    TMessageBuilder builder("input", sourceSchema);
    builder.Payload().SetValue(MakeUnversionedUint64Value(7));
    builder.Payload().SetValue(MakeUnversionedBooleanValue(true, /*id*/ 1));
    builder.Payload().SetValue(MakeUnversionedStringValue("alpha", /*id*/ 2));
    auto key = snapshot->ComputeMessageKey("grouped", builder.Finish());
    ASSERT_TRUE(key);
    EXPECT_EQ(MakeKey(ui64{0x269c2b99970a5b3dULL}, "alpha", ui64{7}), *key);

    builder.Payload().SetValue(MakeUnversionedUint64Value(7));
    builder.Payload().SetValue(MakeUnversionedBooleanValue(false, /*id*/ 1));
    builder.Payload().SetValue(MakeUnversionedNullValue(/*id*/ 2));
    auto nullKey = snapshot->ComputeMessageKey("grouped", builder.Finish());
    ASSERT_TRUE(nullKey);
    EXPECT_EQ(MakeKey(ui64{0xf03df45b1975b535ULL}, std::nullopt, ui64{7}), *nullKey);
}

TEST_F(TFlowQueryGroupByTest, HashesInputMessageIdPayloadColumn)
{
    auto sourceSchema = New<TTableSchema>(std::vector{
        TColumnSchema("$input_message_id", EValueType::String),
    });
    TMessageBuilder builder("input", sourceSchema);
    builder.SetMessageId(TMessageId("different-message-id"));
    builder.Payload().SetValue(MakeUnversionedStringValue("msg-17"));
    auto snapshot = Configure(sourceSchema->Columns(), "farm_hash([$input_message_id])");

    auto key = snapshot->ComputeMessageKey("grouped", builder.Finish());
    ASSERT_TRUE(key);
    EXPECT_EQ(MakeKey(ui64{0x463ef428a4ff1c4dULL}, "msg-17"), *key);
}

TEST_F(TFlowQueryGroupByTest, ReconfigurePreservesComputedKeysOfOldSnapshot)
{
    auto sourceSchema = New<TTableSchema>(std::vector{
        TColumnSchema("key", EValueType::String),
    });
    TMessageBuilder builder("input", sourceSchema);
    builder.Payload().SetValue(MakeUnversionedStringValue("abc"));
    auto message = builder.Finish();
    auto oldSnapshot = Configure(sourceSchema->Columns(), "farm_hash(key)");
    auto oldKey = oldSnapshot->ComputeMessageKey("grouped", message);
    ASSERT_TRUE(oldKey);
    EXPECT_EQ(MakeKey(ui64{0x7b15151e28709746ULL}, "abc"), *oldKey);

    auto newSnapshot = Configure(sourceSchema->Columns(), "farm_hash(\"key\")");
    auto newKey = newSnapshot->ComputeMessageKey("grouped", message);
    ASSERT_TRUE(newKey);
    EXPECT_EQ(MakeKey(ui64{0x407e25082fdedf2cULL}, "abc"), *newKey);
    EXPECT_EQ(oldKey, oldSnapshot->ComputeMessageKey("grouped", message));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
