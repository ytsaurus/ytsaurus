#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_controller.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_writer.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/suspendable_action_queue.h>

namespace NYT {
namespace NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;

using testing::HasSubstr;
using testing::UnorderedElementsAreArray;

////////////////////////////////////////////////////////////////////////////////

std::string MakeRandomString(size_t stringSize)
{
    std::string randomString;
    randomString.reserve(stringSize);
    for (size_t i = 0; i < stringSize; i++) {
        randomString += ('a' + rand() % 25);
    }
    return randomString;
}

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionTest
    : public TApiTestBase
{
protected:
    NNative::IClientPtr NativeClient_;
    NNative::IConnectionPtr NativeConnection_;
    TNameTablePtr ChunkNameTable_;
    ISuspendableActionQueuePtr ActionQueue_;
    TUnversionedOwningRowBuilder RowBuilder_;
    ITransactionPtr Transaction_;
    TDistributedChunkSessionControllerConfigPtr ControllerConfig_;

    void SetUp() override
    {
        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
        NativeConnection_ = NativeClient_->GetNativeConnection();
        ChunkNameTable_ = New<TNameTable>();
        ActionQueue_ = CreateSuspendableActionQueue("TestSession");
        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();
        ControllerConfig_ = New<TDistributedChunkSessionControllerConfig>();
        ControllerConfig_->Account = "intermediate";
        ControllerConfig_->WriteSessionPingPeriod = TDuration::MilliSeconds(100);
    }

    static std::vector<TUnversionedOwningRow> ReadAllChunkRows(TChunkId chunkId)
    {
        auto dataSourceDirectory = New<TDataSourceDirectory>();
        dataSourceDirectory->DataSources().emplace_back(TDataSource(
            EDataSourceType::UnversionedTable,
            /*path*/ "",
            New<TTableSchema>(),
            /*virtualKeyPrefixLength*/ 0,
            /*columns*/ std::nullopt,
            /*omittedInaccessibleColumns*/ {},
            /*timestamp*/ NullTimestamp,
            /*retentionTimestamp*/ NullTimestamp,
            /*columnRenameDescriptors*/ {}));

        NChunkClient::NProto::TChunkSpec chunkSpec;
        chunkSpec.set_row_count_override(0);
        chunkSpec.set_data_weight_override(0);
        ToProto(chunkSpec.mutable_chunk_id(), chunkId);

        std::vector<TDataSliceDescriptor> dataSlices;
        dataSlices.emplace_back(std::move(chunkSpec));

        auto tableReaderOptions = New<NTableClient::TTableReaderOptions>();
        auto tableReaderConfig = New<TTableReaderConfig>();

        auto reader = CreateSchemalessSequentialMultiReader(
            New<TTableReaderConfig>(),
            New<NTableClient::TTableReaderOptions>(),
            NChunkClient::TChunkReaderHost::FromClient(DynamicPointerCast<NNative::IClient>(Client_)),
            dataSourceDirectory,
            dataSlices,
            /*hintKeyPrefixes*/ std::nullopt,
            New<TNameTable>(),
            TClientChunkReadOptions(),
            TReaderInterruptionOptions::InterruptibleWithEmptyKey());

        std::vector<TUnversionedOwningRow> rows;
        while (auto batch = reader->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }
            for (const auto& row : batch->MaterializeRows()) {
                rows.emplace_back(row);
            }
        }

        return rows;
    }

    int GetColumnId(std::string_view columnName)
    {
        return ChunkNameTable_->GetIdOrRegisterName(columnName);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedChunkSessionTest, SingleWriter)
{
    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        ChunkNameTable_,
        ActionQueue_->GetInvoker());

    auto coordinatorNode = WaitFor(controller->StartSession())
        .ValueOrThrow();

    // Ensure that controller sends pings to coordinator.
    Sleep(TDuration::Seconds(2));

    std::vector<TUnversionedOwningRow> rows;

    RowBuilder_.AddValue(MakeUnversionedUint64Value(/*value*/ 1, GetColumnId("a")));
    RowBuilder_.AddValue(MakeUnversionedInt64Value(/*value*/ 2, GetColumnId("b")));
    RowBuilder_.AddValue(MakeUnversionedStringValue(/*value*/ "First row string", GetColumnId("c")));
    rows.push_back(RowBuilder_.FinishRow());

    RowBuilder_.AddValue(MakeUnversionedUint64Value(/*value*/ 3, GetColumnId("a")));
    RowBuilder_.AddValue(MakeUnversionedInt64Value(/*value*/ 4, GetColumnId("b")));
    RowBuilder_.AddValue(MakeUnversionedStringValue(/*value*/ "Second row string", GetColumnId("c")));
    rows.push_back(RowBuilder_.FinishRow());

    auto writer = CreateDistributedChunkWriter(
        coordinatorNode,
        controller->GetSessionId(),
        NativeConnection_,
        New<TDistributedChunkWriterConfig>(),
        ActionQueue_->GetInvoker());

    WaitFor(writer->Write(MakeSharedRange(std::vector<TUnversionedRow>(rows.begin(), rows.end()))))
        .ThrowOnError();

    WaitFor(controller->Close())
        .ThrowOnError();

    EXPECT_EQ(ReadAllChunkRows(controller->GetSessionId().ChunkId), rows);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedChunkSessionTest, MultipleWriters)
{
    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        ChunkNameTable_,
        ActionQueue_->GetInvoker());

    auto coordinatorNode = WaitFor(controller->StartSession())
        .ValueOrThrow();

    std::vector<TUnversionedOwningRow> rows;
    std::vector<IDistributedChunkWriterPtr> writers;

    for (int writerIndex = 0; writerIndex < 4; ++writerIndex) {
        writers.push_back(CreateDistributedChunkWriter(
            coordinatorNode,
            controller->GetSessionId(),
            NativeConnection_,
            New<TDistributedChunkWriterConfig>(),
            ActionQueue_->GetInvoker()));
    }

    for (int index = 0; index < 5; ++index) {
        std::vector<TFuture<void>> writeFutures;

        for (int writerIndex = 0; writerIndex < std::ssize(writers); ++writerIndex) {
            RowBuilder_.AddValue(MakeUnversionedUint64Value(index * std::size(writers) + writerIndex, GetColumnId("intColumn")));
            RowBuilder_.AddValue(MakeUnversionedBooleanValue(false, GetColumnId("boolColumn")));
            RowBuilder_.AddValue(MakeUnversionedStringValue(MakeRandomString(10), GetColumnId("stringColumn")));
            rows.push_back(RowBuilder_.FinishRow());

            RowBuilder_.AddValue(MakeUnversionedUint64Value(index * std::size(writers) + writerIndex, GetColumnId("intColumn")));
            RowBuilder_.AddValue(MakeUnversionedBooleanValue(true, GetColumnId("boolColumn")));
            RowBuilder_.AddValue(MakeUnversionedStringValue(MakeRandomString(10), GetColumnId("stringColumn")));
            rows.push_back(RowBuilder_.FinishRow());

            auto range = MakeSharedRange(std::vector<TUnversionedRow>{rows[std::ssize(rows) - 2], rows[std::ssize(rows) - 1]});

            writeFutures.push_back(writers[writerIndex]->Write(std::move(range)));
        }

        WaitFor(AllSucceeded(std::move(writeFutures)))
            .ThrowOnError();
    }

    WaitFor(controller->Close())
        .ThrowOnError();

    auto resultRows = ReadAllChunkRows(controller->GetSessionId().ChunkId);

    EXPECT_THAT(
        resultRows,
        UnorderedElementsAreArray(rows));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedChunkSessionTest, CoordinatorLeaseExpired)
{
    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        ChunkNameTable_,
        ActionQueue_->GetInvoker());

    auto coordinatorNode = WaitFor(controller->StartSession())
        .ValueOrThrow();

    WaitFor(ActionQueue_->Suspend(/*immediately*/ true))
        .ThrowOnError();

    Sleep(TDuration::Seconds(2));

    ActionQueue_->Resume();

    RowBuilder_.AddValue(MakeUnversionedUint64Value(/*value*/ 1, GetColumnId("a")));
    auto row = RowBuilder_.FinishRow();

    auto writer = CreateDistributedChunkWriter(
        coordinatorNode,
        controller->GetSessionId(),
        NativeConnection_,
        New<TDistributedChunkWriterConfig>(),
        ActionQueue_->GetInvoker());

    EXPECT_THROW_THAT(
        WaitFor(writer->Write(MakeSharedRange(std::vector<TUnversionedRow>{row})))
            .ThrowOnError(),
        HasSubstr("invalid or expired"));

    // Close should succeed, because data node session lease has not expired yet.
    WaitFor(controller->Close())
        .ThrowOnError();

    EXPECT_EQ(std::ssize(ReadAllChunkRows(controller->GetSessionId().ChunkId)), 0);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedChunkSessionTest, CoordinatorLeaseExpiredWithDataOnNodes)
{
    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        ChunkNameTable_,
        ActionQueue_->GetInvoker());

    auto coordinatorNode = WaitFor(controller->StartSession())
        .ValueOrThrow();

    auto writer = CreateDistributedChunkWriter(
        coordinatorNode,
        controller->GetSessionId(),
        NativeConnection_,
        New<TDistributedChunkWriterConfig>(),
        ActionQueue_->GetInvoker());

    RowBuilder_.AddValue(MakeUnversionedUint64Value(/*value*/ 1, GetColumnId("a")));
    RowBuilder_.AddValue(MakeUnversionedInt64Value(/*value*/ 2, GetColumnId("b")));
    RowBuilder_.AddValue(MakeUnversionedStringValue(/*value*/ "First row string", GetColumnId("c")));
    auto row = RowBuilder_.FinishRow();

    WaitFor(writer->Write(MakeSharedRange(std::vector<TUnversionedRow>{row})))
        .ThrowOnError();

    WaitFor(ActionQueue_->Suspend(/*immediately*/ true))
        .ThrowOnError();

    Sleep(TDuration::Seconds(2));

    ActionQueue_->Resume();

    RowBuilder_.AddValue(MakeUnversionedUint64Value(/*value*/ 2, GetColumnId("a")));

    EXPECT_THROW_THAT(
        WaitFor(writer->Write(MakeSharedRange(std::vector<TUnversionedRow>{RowBuilder_.FinishRow()})))
            .ThrowOnError(),
        HasSubstr("invalid or expired"));

    // Close should succeed, because data node session lease has not expired yet.
    WaitFor(controller->Close())
        .ThrowOnError();

    auto resultRows = ReadAllChunkRows(controller->GetSessionId().ChunkId);
    EXPECT_EQ(resultRows, std::vector{row});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT
