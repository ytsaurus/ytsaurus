#include <yt/yt/library/formats/arrow_writer.h>
#include <yt/yt/library/formats/format.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/async_stream_helpers.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/test_framework/framework.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/buffer.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/api.h>

#include <array>

namespace NYT::NFormats {
namespace {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

using TColumn = IUnversionedColumnarRowBatch::TColumn;
using TValueBuffer = IUnversionedColumnarRowBatch::TValueBuffer;

class TTestColumnarBatch
    : public IUnversionedColumnarRowBatch
{
public:
    explicit TTestColumnarBatch(std::vector<const TColumn*> columns)
        : Columns_(std::move(columns))
    { }

    int GetRowCount() const override
    {
        return Columns_.front()->ValueCount;
    }

    TSharedRange<TUnversionedRow> MaterializeRows() override
    {
        YT_ABORT();
    }

    TRange<const TColumn*> MaterializeColumns() override
    {
        return Columns_;
    }

    TRange<TDictionaryId> GetRetiringDictionaryIds() const override
    {
        return {};
    }

private:
    const std::vector<const TColumn*> Columns_;
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr MakeWriter(
    const TTableSchemaPtr& schema,
    TBlobOutput* output,
    TArrowWriterStatisticsPtr statistics)
{
    auto nameTable = TNameTable::FromSchema(*schema);
    auto control = New<TControlAttributesConfig>();
    control->EnableEndOfStream = true;
    return CreateWriterForArrow(
        /*config*/ New<TArrowFormatConfig>(),
        nameTable,
        /*schemas*/ {schema},
        /*columns*/ {},
        /*output*/ CreateAsyncAdapter(output),
        /*enableContextSaving*/ false,
        /*controlAttributesConfig*/ control,
        /*keyColumnCount*/ 0,
        std::move(statistics));
}

void WriteRows(const ISchemalessFormatWriterPtr& writer)
{
    for (int rowIndex = 0; rowIndex < 2; ++rowIndex) {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedUint64Value(rowIndex + 10, 0));
        builder.AddValue(MakeUnversionedUint64Value(rowIndex + 20, 1));
        auto row = builder.FinishRow();
        if (!writer->Write({row.Get()})) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }
    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TArrowWriterStatisticsTest, RowBatches)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        {"first", EValueType::Uint64},
        {"second", EValueType::Uint64},
    });
    auto statistics = New<TArrowWriterStatistics>();
    TBlobOutput output;
    auto writer = MakeWriter(schema, &output, statistics);
    WriteRows(writer);
    EXPECT_EQ(statistics->RecordBatchCount, 2);
    EXPECT_EQ(statistics->ColumnBatchCounts[EArrowEncoding::Dense], 4);
    EXPECT_EQ(statistics->ColumnBatchCounts[EArrowEncoding::Dictionary], 0);
}

TEST(TArrowWriterStatisticsTest, ReusedDictionaryCountsEveryColumnBatch)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        {"dense", EValueType::Uint64},
        {"dictionary", EValueType::Uint64},
    });
    auto type = schema->Columns()[0].LogicalType();
    std::array<ui64, 4> denseValues{100, 200, 300, 400};
    TColumn dense;
    dense.Id = 0;
    dense.Type = type;
    dense.StartIndex = 0;
    dense.ValueCount = 4;
    dense.Values = TValueBuffer{
        .BitWidth = 64,
        .Data = TRef(denseValues.data(), sizeof(denseValues)),
    };
    std::array<ui64, 2> dictionaryValues{10, 20};
    auto leaf = dense;
    leaf.Id = -1;
    leaf.ValueCount = 2;
    leaf.Values = TValueBuffer{
        .BitWidth = 64,
        .Data = TRef(dictionaryValues.data(), sizeof(dictionaryValues)),
    };
    std::array<ui32, 4> indexes{1, 2, 1, 0};
    auto dictionary = dense;
    dictionary.Id = 1;
    dictionary.Values = TValueBuffer{
        .BitWidth = 32,
        .Data = TRef(indexes.data(), sizeof(indexes)),
    };
    dictionary.Dictionary = IUnversionedColumnarRowBatch::TDictionaryEncoding{
        .DictionaryId = IUnversionedColumnarRowBatch::GenerateDictionaryId(),
        .ZeroMeansNull = true,
        .ValueColumn = &leaf,
    };
    auto batch = New<TTestColumnarBatch>(std::vector<const TColumn*>{&dense, &dictionary});
    auto statistics = New<TArrowWriterStatistics>();
    TBlobOutput output;
    auto writer = MakeWriter(schema, &output, statistics);
    for (int batchIndex = 0; batchIndex < 2; ++batchIndex) {
        if (!writer->WriteBatch(batch)) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }
    WaitFor(writer->Close())
        .ThrowOnError();
    EXPECT_EQ(statistics->RecordBatchCount, 2);
    EXPECT_EQ(statistics->ColumnBatchCounts[EArrowEncoding::Dense], 2);
    EXPECT_EQ(statistics->ColumnBatchCounts[EArrowEncoding::Dictionary], 2);
    auto bytes = output.Flush();
    auto source = std::make_shared<arrow20::io::BufferReader>(
        arrow20::Buffer::FromString(std::string(bytes.Begin(), bytes.Size())));
    int dictionaryMessageCount = 0;
    while (auto message = arrow20::ipc::ReadMessage(source.get()).ValueOrDie()) {
        dictionaryMessageCount += message->type() == arrow20::ipc::MessageType::DICTIONARY_BATCH;
    }
    EXPECT_EQ(dictionaryMessageCount, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
