#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/failed_messages.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>

namespace NYT::NFlow::NTables {
namespace {

using namespace NApi;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace testing;

class TFailedMessagesTest
    : public Test
{
protected:
    const NProfiling::TSolomonRegistryPtr Registry_ = New<NProfiling::TSolomonRegistry>();
    TFailedMessagesPtr Table_;
    TStreamSpecsPtr StreamSpecs_;
    const TPartitionId PartitionId_ = TPartitionId(TGuid::Create());
    const TSystemTimestamp BatchTimestamp_ = TSystemTimestamp(123);

    void SetUp() override
    {
        Registry_->SetWindowSize(12);
        auto context = New<TContext>();
        context->PipelinePath = "//pipeline";
        context->Profiler = NProfiling::TProfiler(Registry_, "/failed_messages_test");
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>("FailedMessagesTest", context->Logger, context->Profiler);
        auto originalSpec = New<TStreamSpec>();
        originalSpec->Schema = New<TTableSchema>(std::vector{TColumnSchema("value", EValueType::String)});
        auto currentSpec = New<TStreamSpec>();
        currentSpec->Schema = New<TTableSchema>(std::vector{
            TColumnSchema("value", EValueType::String),
            TColumnSchema("tag", EValueType::String),
        });
        StreamSpecs_ = New<TStreamSpecs>(THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>{
            {TStreamId("source"), {{TStreamSpecId(1), originalSpec}, {TStreamSpecId(2), currentSpec}}},
        });
        Table_ = New<TFailedMessages>(context, StreamSpecs_, "capture-worker:9012", TFlowCoreTarget("capture-core-target"));
    }

    i64 CollectCapturedRows()
    {
        Registry_->ProcessRegistrations();
        Registry_->Collect();
        const auto dump = Registry_->DumpSensors();
        for (const auto& cube : dump.cubes()) {
            if (cube.name() != "yt/failed_messages_test/captured_rows") {
                continue;
            }
            i64 value = 0;
            for (const auto& projection : cube.projections()) {
                if (projection.has_counter()) {
                    value = std::max(value, projection.counter());
                }
            }
            return value;
        }
        ADD_FAILURE() << "Missing captured_rows counter";
        return -1;
    }

    TFailedMessage MakeFailure()
    {
        auto schema = StreamSpecs_->GetSchema(TStreamSpecId(1));
        TMessageBuilder builder(TStreamId("source"), schema);
        builder.SetMessageId(TMessageId("original-id"));
        builder.SetSystemTimestamp(TSystemTimestamp(11));
        builder.SetAlignmentTimestamp(TSystemTimestamp(11));
        builder.SetEventTimestamp(TSystemTimestamp(7));
        builder.Payload().Set(std::string("original-payload"), 0);
        return {
            New<TInputMessage>(builder.Finish(), MakeKey("queue-identity", ui64(7))),
            TError("Invalid input"),
        };
    }
};

TEST_W(TFailedMessagesTest, PreservesOriginalStreamSpecIdPayloadSourceKeyAndFirstCapture)
{
    EXPECT_EQ(StreamSpecs_->GetLastSpecId(TStreamId("source")), TStreamSpecId(2));
    auto failure = MakeFailure();
    auto duplicate = failure;
    duplicate.Reason = TError("Second capture must not replace the first");
    auto transaction = CreateRetryableTransaction();
    Table_->Write(transaction, TComputationId("computation"), PartitionId_, BatchTimestamp_, {failure, duplicate});

    auto attempt = New<StrictMock<TMockTransaction>>();
    attempt->StartTimestamp = NTransactionClient::TTimestamp(100);
    EXPECT_CALL(*attempt, LookupRows("//pipeline/failed_messages", _, _, _))
        .WillOnce([] (const auto&, const auto&, const auto& keys, const auto& options) {
            EXPECT_EQ(keys.Size(), 1u);
            EXPECT_TRUE(options.KeepMissingRows);
            EXPECT_EQ(options.Timestamp, NTransactionClient::TTimestamp(100));
            return MakeFuture(TUnversionedLookupRowsResult{
                .Rowset = CreateRowset(New<TTableSchema>(), MakeSharedRange(std::vector<TUnversionedRow>{TUnversionedRow{}})),
            });
        });
    EXPECT_CALL(*attempt, ModifyRows("//pipeline/failed_messages", _, _, _))
        .WillOnce([&] (const auto&, const auto& names, const auto& modifications, const auto&) {
            ASSERT_EQ(modifications.Size(), 1u);
            const auto row = std::get<NRowModifications::TWriteRow>(modifications[0]).Row;
            auto cell = [&] (const char* name) {
                const auto id = names->GetIdOrThrow(name);
                for (const auto& value : row) {
                    if (value.Id == id) {
                        return value;
                    }
                }
                YT_ABORT();
            };
            EXPECT_EQ(FromUnversionedValue<std::string>(cell("message_id")), "original-id");
            EXPECT_EQ(FromUnversionedValue<ui64>(cell("event_timestamp")), 7u);
            EXPECT_EQ(FromUnversionedValue<ui64>(cell("failed_at")), 123'000u);
            EXPECT_EQ(FromUnversionedValue<ui64>(cell("replay_attempts")), 0u);
            EXPECT_EQ(FromUnversionedValue<std::string>(cell("payload")), ToProto<TProtobufString>(failure.Message->Payload));
            EXPECT_EQ(cell("key").AsStringBuf(), ConvertToYsonString(failure.Message->Key.Underlying()).AsStringBuf());
            EXPECT_EQ(FromUnversionedValue<i64>(cell("stream_spec_id")), 1);
            auto diagnostic = ConvertToNode(NYson::TYsonString(cell("diagnostic").AsStringBuf()))->AsMap();
            EXPECT_EQ(diagnostic->GetChildValueOrThrow<ui64>("system_timestamp"), 11u);
            EXPECT_EQ(diagnostic->GetChildValueOrThrow<std::string>("worker_id"), "capture-worker:9012");
            EXPECT_EQ(diagnostic->GetChildValueOrThrow<std::string>("flow_core_target"), "capture-core-target");
            EXPECT_EQ(diagnostic->GetChildValueOrThrow<TError>("error").GetMessage(), "Invalid input");
        });
    EXPECT_EQ(CollectCapturedRows(), 1);
    transaction->DoAttempt(attempt);
    EXPECT_EQ(CollectCapturedRows(), 0);
}

TEST_W(TFailedMessagesTest, RereadsEveryAttemptAndNeverOverwritesAnExistingFailure)
{
    auto transaction = CreateRetryableTransaction();
    Table_->Write(transaction, TComputationId("computation"), PartitionId_, BatchTimestamp_, {MakeFailure()});
    auto attempt = New<StrictMock<TMockTransaction>>();
    attempt->StartTimestamp = NTransactionClient::TTimestamp(100);
    bool exists = false;
    auto rowBuffer = New<TRowBuffer>();
    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue("computation", 0));
    const TUnversionedRow presentRow = rowBuffer->CaptureRow(builder.GetRow());
    EXPECT_CALL(*attempt, LookupRows("//pipeline/failed_messages", _, _, _))
        .Times(3)
        .WillRepeatedly([&] (const auto&, const auto&, const auto&, const auto&) {
            return MakeFuture(TUnversionedLookupRowsResult{
                .Rowset = CreateRowset(New<TTableSchema>(), MakeSharedRange(std::vector<TUnversionedRow>{exists ? presentRow : TUnversionedRow{}}, rowBuffer)),
            });
        });
    EXPECT_CALL(*attempt, ModifyRows("//pipeline/failed_messages", _, _, _)).Times(1);
    transaction->DoAttempt(attempt);
    exists = true;
    transaction->DoAttempt(attempt);

    auto repeatedFailure = MakeFailure();
    repeatedFailure.Reason = TError("Repeated failure");
    auto repeatedCapture = CreateRetryableTransaction();
    Table_->Write(repeatedCapture, TComputationId("computation"), PartitionId_, TSystemTimestamp(124), {repeatedFailure});
    repeatedCapture->DoAttempt(attempt);

    EXPECT_EQ(CollectCapturedRows(), 2);
}

TEST_W(TFailedMessagesTest, EmptyWriteDoesNotIncrementCaptureCount)
{
    auto transaction = CreateRetryableTransaction();
    Table_->Write(transaction, TComputationId("computation"), PartitionId_, BatchTimestamp_, {});

    EXPECT_TRUE(transaction->IsEmpty());
    EXPECT_EQ(CollectCapturedRows(), 0);
}

TEST_W(TFailedMessagesTest, FailedLookupDoesNotRepeatCaptureCount)
{
    auto transaction = CreateRetryableTransaction();
    Table_->Write(transaction, TComputationId("computation"), PartitionId_, BatchTimestamp_, {MakeFailure()});
    auto attempt = New<StrictMock<TMockTransaction>>();
    attempt->StartTimestamp = NTransactionClient::TTimestamp(100);
    EXPECT_CALL(*attempt, LookupRows("//pipeline/failed_messages", _, _, _))
        .WillOnce(Return(MakeFuture<TUnversionedLookupRowsResult>(TError("Lookup failed"))))
        .WillOnce(Return(MakeFuture(TUnversionedLookupRowsResult{
            .Rowset = CreateRowset(New<TTableSchema>(), MakeSharedRange(std::vector<TUnversionedRow>{TUnversionedRow{}})),
        })));
    EXPECT_THROW(transaction->DoAttempt(attempt), TErrorException);
    EXPECT_EQ(CollectCapturedRows(), 1);

    EXPECT_CALL(*attempt, ModifyRows("//pipeline/failed_messages", _, _, _)).Times(1);
    transaction->DoAttempt(attempt);
    EXPECT_EQ(CollectCapturedRows(), 0);
}

} // namespace
} // namespace NYT::NFlow::NTables
