#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/random/source.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/source_context.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TRandomRecord
{
    std::string Key;
    std::string Data;

    bool operator==(const TRandomRecord& other) const = default;
};

class TRandomSourceTest
    : public ::testing::Test
{
protected:
    static constexpr i64 RecordCount = 200;

    const TActionQueuePtr ActionQueue = New<TActionQueue>();

    void TearDown() override
    {
        ActionQueue->Shutdown();
    }

    // Reads the first |RecordCount| records of partition |partitionIndex| from a fresh source instance.
    std::vector<TRandomRecord> ReadRecords(
        int partitionIndex,
        i64 maxRowsPerBatch,
        std::string pipelinePath = "//home/flow/pipeline")
    {
        auto read = BIND([=, this] {
            auto source = MakeSource(partitionIndex, pipelinePath);
            auto stateManager = New<TStateManagerMock>();
            source->Init(stateManager->CreateContext()->WithPrefix("source"));

            auto batcherSettings = New<TMessageBatcherSettings>();
            batcherSettings->MaxRowsPerBatch = TSize(maxRowsPerBatch);

            std::vector<TRandomRecord> records;
            while (std::ssize(records) < RecordCount) {
                auto batches = WaitFor(source->GetNextBatch(batcherSettings)).ValueOrThrow();
                for (const auto& batch : batches) {
                    for (const auto& message : batch.Messages) {
                        records.push_back({
                            .Key = GetColumnValue<std::string>(message, "key"),
                            .Data = GetColumnValue<std::string>(message, "data"),
                        });
                    }
                }
            }
            source->Terminate();
            records.resize(RecordCount);
            return records;
        });
        return WaitFor(read.AsyncVia(ActionQueue->GetInvoker()).Run()).ValueOrThrow();
    }

private:
    TRandomSourcePtr MakeSource(int partitionIndex, const std::string& pipelinePath)
    {
        auto context = CreateTestSourceContext(ActionQueue->GetInvoker());
        context->PipelinePath = NYPath::TRichYPath(NYPath::TYPath(pipelinePath));
        context->TimeProvider = New<TFakeTimeProvider>();
        context->SourceKey = MakeKey(partitionIndex);
        context->SourceSpec = ConvertTo<TSourceSpecPtr>(NYson::TYsonString(TStringBuf(R"""({
            "stream_id" = "random";
            "parameters" = {};
        })""")));
        context->SourceSpec->SourceClassName = TypeName<TRandomSource>();

        auto dynamicContext = New<TDynamicSourceContext>();
        dynamicContext->DynamicSourceSpec = New<TDynamicSourceSpec>();
        dynamicContext->DynamicSourceSpec->Parameters = ConvertTo<IMapNodePtr>(NYson::TYsonString(TStringBuf(R"""({
            "partition_count" = 2;
            "message_size_mean" = 64;
            "message_count_mean" = 20;
            "message_key_range" = 100;
        })""")));
        dynamicContext->DynamicPartitionSpec = GetEphemeralNodeFactory()->CreateMap();

        return New<TRandomSource>(std::move(context), std::move(dynamicContext));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TRandomSourceTest, SameOffsetYieldsSameRecord)
{
    auto expected = ReadRecords(/*partitionIndex*/ 0, /*maxRowsPerBatch*/ 1000);
    for (i64 maxRowsPerBatch : {1, 7, 1000}) {
        EXPECT_EQ(ReadRecords(/*partitionIndex*/ 0, maxRowsPerBatch), expected)
            << "MaxRowsPerBatch: " << maxRowsPerBatch;
    }
}

TEST_F(TRandomSourceTest, PartitionsYieldDifferentRecords)
{
    EXPECT_NE(
        ReadRecords(/*partitionIndex*/ 0, /*maxRowsPerBatch*/ 1000),
        ReadRecords(/*partitionIndex*/ 1, /*maxRowsPerBatch*/ 1000));
}

TEST_F(TRandomSourceTest, PipelinesYieldDifferentRecords)
{
    EXPECT_NE(
        ReadRecords(/*partitionIndex*/ 0, /*maxRowsPerBatch*/ 1000, /*pipelinePath*/ "//home/flow/first"),
        ReadRecords(/*partitionIndex*/ 0, /*maxRowsPerBatch*/ 1000, /*pipelinePath*/ "//home/flow/second"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
