#include "dataset.h"

#include "private.h"
#include "schema_ordering_writer.h"
#include "writer.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/memory_reader.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/columnar_chunk_meta.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/library/formats/yson_parser.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <util/folder/path.h>

#include <util/stream/file.h>

#include <array>
#include <bit>
#include <map>
#include <random>
#include <utility>

namespace NYT::NRowIOBenchmark {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NFormats;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DatasetSeed = 42;
constexpr int StringLength = 1024;

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr CreateRandomSchema(int columnCount)
{
    YT_VERIFY(columnCount > 0 && columnCount % 2 == 0);
    std::vector<TColumnSchema> columns;
    for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
        columns.emplace_back(
            Format("column_%v", columnIndex),
            columnIndex < columnCount / 2 ? EValueType::Int64 : EValueType::String);
    }
    return New<TTableSchema>(std::move(columns));
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow GenerateRow(std::mt19937_64* random, int columnCount)
{
    TUnversionedOwningRowBuilder builder;
    for (int columnIndex = 0; columnIndex < columnCount / 2; ++columnIndex) {
        builder.AddValue(MakeUnversionedInt64Value(
            std::bit_cast<i64>((*random)()),
            columnIndex));
    }
    for (int columnIndex = columnCount / 2; columnIndex < columnCount; ++columnIndex) {
        std::string value(StringLength, '\0');
        for (auto& character : value) {
            character = static_cast<char>('a' + (*random)() % 26);
        }
        builder.AddValue(MakeUnversionedStringValue(value, columnIndex));
    }
    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

TMemoryWriterPtr CreateDataset(const TTableSchemaPtr& schema, int rowCount)
{
    auto sink = New<TMemoryWriter>();
    auto chunkWriter = CreateWriter(schema, sink, EOptimizeFor::Scan);
    auto writer = CreateSchemaOrderingWriter(chunkWriter);
    std::mt19937_64 random(DatasetSeed);
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        auto row = GenerateRow(&random, schema->GetColumnCount());
        if (!writer->Write({row.Get()})) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }
    WaitFor(chunkWriter->Close())
        .ThrowOnError();
    return sink;
}

////////////////////////////////////////////////////////////////////////////////

void ReadRows(const IUnversionedWriterPtr& writer, const TFsPath& path)
{
    TWritingValueConsumer consumer(writer);
    TTableConsumer tableConsumer({}, &consumer);
    TTableOutput output(CreateParserForYson(&tableConsumer, EYsonType::ListFragment));
    TFileInput input((path / "rows.yson").GetPath());
    std::array<char, 64_KB> buffer;
    while (auto size = input.Read(buffer.data(), buffer.size())) {
        output.Write(buffer.data(), size);
    }
    output.Finish();
    WaitFor(consumer.Flush())
        .ThrowOnError();
}

TDataset LoadDataset(const TFsPath& path)
{
    TFileInput schemaFile((path / "schema.yson").GetPath());
    auto schemaYson = schemaFile.ReadAll();
    auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(schemaYson)));
    if (!schema->IsStrict() || schema->IsEmpty()) {
        THROW_ERROR_EXCEPTION("Local dataset must have a nonempty strict schema");
    }
    auto sink = New<TMemoryWriter>();
    auto writer = CreateWriter(schema, sink, EOptimizeFor::Scan);
    ReadRows(CreateSchemaOrderingWriter(writer), path);
    auto rowCount = writer->GetDataStatistics().row_count();
    if (rowCount == 0) {
        THROW_ERROR_EXCEPTION("Local dataset must contain at least one row");
    }
    WaitFor(writer->Close())
        .ThrowOnError();
    return {
        .Schema = std::move(schema),
        .Source = std::move(sink),
        .RowCount = rowCount,
        .LogicalBytes = writer->GetDataWeight() - rowCount,
    };
}

////////////////////////////////////////////////////////////////////////////////

class TRowCursor
{
public:
    explicit TRowCursor(ISchemalessChunkReaderPtr reader)
        : Reader_(std::move(reader))
    { }

    TUnversionedRow Next()
    {
        while (RowIndex_ == std::ssize(Rows_)) {
            Batch_ = Reader_->Read();
            if (!Batch_) {
                return {};
            }
            if (Batch_->IsEmpty()) {
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }
            Rows_ = Batch_->MaterializeRows();
            RowIndex_ = 0;
        }
        return Rows_[RowIndex_++];
    }

private:
    const ISchemalessChunkReaderPtr Reader_;

    IUnversionedRowBatchPtr Batch_;
    TSharedRange<TUnversionedRow> Rows_;
    i64 RowIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

bool AreValuesEqual(const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    if (lhs.Type == rhs.Type && IsAnyOrComposite(lhs.Type)) {
        if (lhs.AsStringBuf() == rhs.AsStringBuf()) {
            return true;
        }
        return AreNodesEqual(
            ConvertToNode(TYsonStringBuf(lhs.AsStringBuf())),
            ConvertToNode(TYsonStringBuf(rhs.AsStringBuf())),
            {.DoubleTypePrecision = 0.0});
    }
    return CompareRowValues(lhs, rhs) == 0;
}

////////////////////////////////////////////////////////////////////////////////

void ValidateRow(
    TUnversionedRow actual,
    TUnversionedRow expected,
    const TNameTablePtr& nameTable,
    const TTableSchemaPtr& schema,
    i64 rowIndex)
{
    YT_VERIFY(static_cast<int>(actual.GetCount()) == schema->GetColumnCount());
    YT_VERIFY(static_cast<int>(expected.GetCount()) == schema->GetColumnCount());
    std::vector<bool> seen(schema->GetColumnCount());
    for (const auto& value : actual) {
        auto name = nameTable->GetName(value.Id);
        int columnIndex = schema->GetColumnIndexOrThrow(name);
        YT_VERIFY(!seen[columnIndex]);
        seen[columnIndex] = true;
        YT_VERIFY(expected[columnIndex].Id == columnIndex);
        if (!AreValuesEqual(value, expected[columnIndex])) {
            THROW_ERROR_EXCEPTION("Output value differs from input")
                .With("row_index", rowIndex)
                .With("column_name", name)
                .With("actual_value", ToString(value))
                .With("expected_value", ToString(expected[columnIndex]));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateReader(
    const TTableSchemaPtr& schema,
    const TMemoryWriterPtr& source)
{
    auto state = New<TChunkState>(TChunkState{
        .BlockCache = GetNullBlockCache(),
        .TableSchema = schema,
    });
    return CreateSchemalessRangeChunkReader(
        CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>()),
        state,
        New<TColumnarChunkMeta>(*source->GetChunkMeta()),
        TChunkReaderConfig::GetDefault(),
        TChunkReaderOptions::GetDefault(),
        CreateMemoryReader(source->GetChunkMeta(), source->GetBlocks()),
        TNameTable::FromSchema(*schema),
        /*chunkReadOptions*/ {},
        /*sortColumns*/ {},
        /*omittedInaccessibleColumns*/ {},
        TColumnFilter(),
        TReadRange(),
        /*partitionTags*/ {},
        /*memoryManagerHolder*/ nullptr,
        /*virtualRowIndex*/ std::nullopt,
        /*interruptDescriptorKeyLength*/ 0,
        GetCurrentInvoker());
}

////////////////////////////////////////////////////////////////////////////////

void ValidateOutput(
    const TTableSchemaPtr& schema,
    const TMemoryWriterPtr& expected,
    const TMemoryWriterPtr& actual,
    i64 expectedRowCount)
{
    for (const auto& block : actual->GetBlocks()) {
        block.CheckChecksum()
            .ThrowOnError();
    }
    auto reader = CreateReader(schema, actual);
    TRowCursor actualRows(reader);
    TRowCursor expectedRows(CreateReader(schema, expected));
    i64 rowCount = 0;
    while (auto row = actualRows.Next()) {
        YT_VERIFY(rowCount < expectedRowCount);
        auto expectedRow = expectedRows.Next();
        YT_VERIFY(expectedRow);
        ValidateRow(row, expectedRow, reader->GetNameTable(), schema, rowCount);
        ++rowCount;
    }
    YT_VERIFY(rowCount == expectedRowCount);
    YT_VERIFY(!expectedRows.Next());
}

////////////////////////////////////////////////////////////////////////////////

i64 GetEncodedSize(const TMemoryWriterPtr& sink)
{
    i64 size = 0;
    for (const auto& block : sink->GetBlocks()) {
        size += block.Size();
    }
    return size;
}

////////////////////////////////////////////////////////////////////////////////

TDataset GenerateRandomDataset(int columnCount, int rowCount)
{
    auto schema = CreateRandomSchema(columnCount);
    auto source = CreateDataset(schema, rowCount);
    i64 logicalBytes = rowCount * (columnCount / 2 * (sizeof(i64) + StringLength));
    return {
        .Schema = std::move(schema),
        .Source = std::move(source),
        .RowCount = rowCount,
        .LogicalBytes = logicalBytes,
    };
}

////////////////////////////////////////////////////////////////////////////////

TDataset ReadLocalDataset(const std::string& path)
{
    static std::map<std::string, TDataset> Cache;
    auto& dataset = Cache[path];
    if (!dataset.Source) {
        dataset = LoadDataset(TFsPath(path));
    }
    return dataset;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
