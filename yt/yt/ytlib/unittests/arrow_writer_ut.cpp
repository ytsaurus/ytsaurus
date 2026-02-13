#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

#include <yt/yt/library/formats/arrow_metadata_constants.h>
#include <yt/yt/library/formats/arrow_writer.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/memory_reader.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/core/concurrency/async_stream_helpers.h>

#include <util/stream/null.h>
#include <util/string/hex.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/cast.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/io/memory.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow_next/cpp/src/generated/Message.fbs.h>

#include <limits>
#include <stdlib.h>

namespace NYT::NTableClient {
namespace {

using namespace NChunkClient;
using namespace NFormats;
using namespace NNamedValue;
using namespace NTableClient;
using namespace NTzTypes;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowBatchPtr MakeColumnarRowBatch(
    TRange<NTableClient::TUnversionedRow> rows,
    TTableSchemaPtr Schema_)
{
    auto memoryWriter = New<TMemoryWriter>();

    auto config = New<TChunkWriterConfig>();
    config->Postprocess();
    config->BlockSize = 256;
    config->Postprocess();

    auto options = New<TChunkWriterOptions>();
    options->OptimizeFor = EOptimizeFor::Scan;
    options->Postprocess();

    auto chunkWriter = CreateSchemalessChunkWriter(
        config,
        options,
        Schema_,
        /*nameTable*/ nullptr,
        memoryWriter,
        /*writeBlocksOptions*/ {},
        /*dataSink*/ std::nullopt);

    TUnversionedRowsBuilder builder;

    Y_UNUSED(chunkWriter->Write(rows));
    chunkWriter->Close().Get().IsOK();

    auto MemoryReader_ = CreateMemoryReader(
        memoryWriter->GetChunkMeta(),
        memoryWriter->GetBlocks());

    NChunkClient::NProto::TChunkSpec ChunkSpec_;
    ToProto(ChunkSpec_.mutable_chunk_id(), NullChunkId);
    ChunkSpec_.set_table_row_index(42);

    auto ChunkMeta_ = New<TColumnarChunkMeta>(*memoryWriter->GetChunkMeta());

    auto ChunkState_ = New<TChunkState>(TChunkState{
            .BlockCache = GetNullBlockCache(),
            .ChunkSpec = ChunkSpec_,
            .TableSchema = Schema_,
    });

    auto schemalessRangeChunkReader = CreateSchemalessRangeChunkReader(
        CreateColumnEvaluatorCache(New<NQueryClient::TColumnEvaluatorCacheConfig>()),
        ChunkState_,
        ChunkMeta_,
        TChunkReaderConfig::GetDefault(),
        TChunkReaderOptions::GetDefault(),
        MemoryReader_,
        TNameTable::FromSchema(*Schema_),
        /* chunkReadOptions */ {},
        /* sortColumns */ {},
        /* omittedInaccessibleColumns */ {},
        TColumnFilter(),
        TReadRange());

    TRowBatchReadOptions opt{
        .MaxRowsPerRead = std::ssize(rows) + 10,
        .Columnar = true};
    auto batch = ReadRowBatch(schemalessRangeChunkReader, opt);
    return batch;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateArrowWriter(TNameTablePtr nameTable,
    IOutputStream* outputStream,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    TArrowFormatConfigPtr arrowConfig = New<TArrowFormatConfig>())
{
    TControlAttributesConfigPtr controlAttributes = New<TControlAttributesConfig>();
    controlAttributes->EnableTableIndex = false;
    controlAttributes->EnableRowIndex = false;
    controlAttributes->EnableRangeIndex = false;
    controlAttributes->EnableTabletIndex = false;
    return CreateWriterForArrow(
        std::move(arrowConfig),
        nameTable,
        schemas,
        /*columns*/ {},
        NConcurrency::CreateAsyncAdapter(static_cast<IOutputStream*>(outputStream)),
        false,
        controlAttributes,
        0);
}

ISchemalessFormatWriterPtr CreateArrowWriteWithSystemColumns(TNameTablePtr nameTable,
    IOutputStream* outputStream,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    auto controlAttributes = NYT::New<TControlAttributesConfig>();
    controlAttributes->EnableTableIndex = true;
    controlAttributes->EnableRowIndex = true;
    controlAttributes->EnableRangeIndex = true;
    controlAttributes->EnableTabletIndex = true;
    return CreateWriterForArrow(
        New<TArrowFormatConfig>(),
        nameTable,
        schemas,
        /*columns*/ {},
        NConcurrency::CreateAsyncAdapter(static_cast<IOutputStream*>(outputStream)),
        false,
        controlAttributes,
        0);
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow20::RecordBatch> MakeBatch(const TStringStream& outputStream)
{
    auto buffer = std::make_shared<arrow20::Buffer>(reinterpret_cast<const uint8_t*>(outputStream.Data()), outputStream.Size());
    arrow20::io::BufferReader bufferReader(buffer);

    std::shared_ptr<arrow20::ipc::RecordBatchStreamReader> batchReader = (arrow20::ipc::RecordBatchStreamReader::Open(&bufferReader)).ValueOrDie();

    auto batch = batchReader->Next().ValueOrDie();
    return batch;
}

std::vector<std::shared_ptr<arrow20::RecordBatch>> MakeAllBatch(const TStringStream& outputStream, int batchCount)
{
    auto buffer = std::make_shared<arrow20::Buffer>(reinterpret_cast<const uint8_t*>(outputStream.Data()), outputStream.Size());
    arrow20::io::BufferReader bufferReader(buffer);

    std::shared_ptr<arrow20::ipc::RecordBatchStreamReader> batchReader = (arrow20::ipc::RecordBatchStreamReader::Open(&bufferReader)).ValueOrDie();

    std::vector<std::shared_ptr<arrow20::RecordBatch>> batches;
    for (int i = 0; i < batchCount; i++) {
        auto batch = batchReader->Next().ValueOrDie();
        if (batch == nullptr) {
            batchReader = (arrow20::ipc::RecordBatchStreamReader::Open(&bufferReader)).ValueOrDie();
            batchCount++;
        } else {
            batches.push_back(batch);
        }
    }
    return batches;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<int64_t> ReadInteger64Array(const std::shared_ptr<arrow20::Array>& array)
{
    auto int64Array = std::dynamic_pointer_cast<arrow20::Int64Array>(array);
    YT_VERIFY(int64Array);
    return  {int64Array->raw_values(), int64Array->raw_values() + array->length()};
}

std::vector<uint64_t> ReadUInteger64Array(const std::shared_ptr<arrow20::Array>& array)
{
    auto uint64Array = std::dynamic_pointer_cast<arrow20::UInt64Array>(array);
    YT_VERIFY(uint64Array);
    return  {uint64Array->raw_values(), uint64Array->raw_values() + array->length()};
}

std::vector<uint32_t> ReadInteger32Array(const std::shared_ptr<arrow20::Array>& array)
{
    auto int32Array = std::dynamic_pointer_cast<arrow20::Int32Array>(array);
    YT_VERIFY(int32Array);
    return  {int32Array->raw_values(), int32Array->raw_values() + array->length()};
}

std::vector<uint32_t> ReadUInteger32Array(const std::shared_ptr<arrow20::Array>& array)
{
    auto uint32Array = std::dynamic_pointer_cast<arrow20::UInt32Array>(array);
    YT_VERIFY(uint32Array);
    return  {uint32Array->raw_values(), uint32Array->raw_values() + array->length()};
}

std::vector<uint16_t> ReadUInteger16Array(const std::shared_ptr<arrow20::Array>& array)
{
    auto uint16Array = std::dynamic_pointer_cast<arrow20::UInt16Array>(array);
    YT_VERIFY(uint16Array);
    return  {uint16Array->raw_values(), uint16Array->raw_values() + array->length()};
}

std::vector<int64_t> ReadIntegerDateArray(const std::shared_ptr<arrow20::Array>& array)
{
    auto int32Array = std::dynamic_pointer_cast<arrow20::Date32Array>(array);
    YT_VERIFY(int32Array);
    return  {int32Array->raw_values(), int32Array->raw_values() + array->length()};
}

std::vector<int64_t> ReadIntegerDate64Array(const std::shared_ptr<arrow20::Array>& array)
{
    auto timestampArray = std::dynamic_pointer_cast<arrow20::TimestampArray>(array);
    YT_VERIFY(timestampArray);
    return  {timestampArray->raw_values(), timestampArray->raw_values() + array->length()};
}

std::vector<int64_t> ReadTimestampArray(const std::shared_ptr<arrow20::Array>& array)
{
    auto int64Array = std::dynamic_pointer_cast<arrow20::TimestampArray>(array);
    YT_VERIFY(int64Array);
    return  {int64Array->raw_values(), int64Array->raw_values() + int64Array->length()};
}

std::vector<std::string> ReadStringArray(const std::shared_ptr<arrow20::Array>& array)
{
    auto arraySize = array->length();
    auto binArray = std::dynamic_pointer_cast<arrow20::BinaryArray>(array);
    YT_VERIFY(binArray);
    std::vector<std::string> stringArray;
    for (int i = 0; i < arraySize; i++) {
        stringArray.push_back(binArray->GetString(i));
    }
    return stringArray;
}

std::vector<bool> ReadBooleanArray(const std::shared_ptr<arrow20::Array>& array)
{
    auto arraySize = array->length();
    auto booleanArray = std::dynamic_pointer_cast<arrow20::BooleanArray>(array);
    YT_VERIFY(booleanArray);
    std::vector<bool> result;
    for (int i = 0; i < arraySize; i++) {
        result.push_back(booleanArray->Value(i));
    }
    return result;
}

std::vector<double> ReadDoubleArray(const std::shared_ptr<arrow20::Array>& array)
{
    auto doubleArray = std::dynamic_pointer_cast<arrow20::DoubleArray>(array);
    YT_VERIFY(doubleArray);
    return  {doubleArray->raw_values(), doubleArray->raw_values() + array->length()};
}

std::vector<float> ReadFloatArray(const std::shared_ptr<arrow20::Array>& array)
{
    auto floatArray = std::dynamic_pointer_cast<arrow20::FloatArray>(array);
    YT_VERIFY(floatArray);
    return  {floatArray->raw_values(), floatArray->raw_values() + array->length()};
}

std::vector<std::string> ReadStringArrayFromDict(const std::shared_ptr<arrow20::Array>& array)
{
    auto dictAr = std::dynamic_pointer_cast<arrow20::DictionaryArray>(array);
    YT_VERIFY(dictAr);
    auto indices = ReadUInteger32Array(dictAr->indices());

    // Get values array.
    auto values = ReadStringArray(dictAr->dictionary());

    std::vector<std::string> result;
    for (int i = 0; i < std::ssize(indices); i++) {
        if (array->IsNull(i)) {
            result.push_back("");
        } else {
            auto index = indices[i];
            auto value = values[index];
            result.push_back(value);
        }
    }
    return result;
}

std::vector<std::string> ReadAnyStringArray(const std::shared_ptr<arrow20::Array>& array)
{
    if (std::dynamic_pointer_cast<arrow20::BinaryArray>(array)) {
        return ReadStringArray(array);
    } else if (std::dynamic_pointer_cast<arrow20::DictionaryArray>(array)) {
        return ReadStringArrayFromDict(array);
    }
    YT_ABORT();
}

bool IsDictColumn(const std::shared_ptr<arrow20::Array>& array)
{
    return std::dynamic_pointer_cast<arrow20::DictionaryArray>(array) != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

using TColumnInteger = std::vector<int64_t>;
using TColumnInteger32 = std::vector<int32_t>;
using TColumnString = std::vector<std::string>;
using TColumnNullableString = std::vector<std::optional<std::string>>;
using TColumnNullableInteger = std::vector<std::optional<int64_t>>;
using TColumnBool = std::vector<bool>;
using TColumnDouble = std::vector<double>;
using TColumnFloat = std::vector<float>;

using TColumnStringWithNulls = std::vector<std::optional<std::string>>;
using TColumnBoolWithNulls = std::vector<std::optional<bool>>;
using TColumnDoubleWithNulls = std::vector<std::optional<double>>;

template <typename T, bool Nullable>
using TColumn = std::vector<std::conditional_t<Nullable, std::optional<T>, T>>;

struct TOwnerRows
{
    std::vector<TUnversionedRow> Rows;
    std::vector<TUnversionedOwningRowBuilder> Builders;
    TNameTablePtr NameTable;
    std::vector<TUnversionedOwningRow> OwningRows;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TCppType, EValueType YTType, bool Nullable>
TOwnerRows MakeUnversionedNumericRows(
    const std::vector<TColumn<TCppType, Nullable>>& column,
    const std::vector<std::string>& columnNames)
{
    YT_VERIFY(column.size() > 0);

    auto nameTable = New<TNameTable>();

    std::vector<TUnversionedOwningRowBuilder> rowsBuilders(column[0].size());

    for (int colIdx = 0; colIdx < std::ssize(column); colIdx++) {
        auto columnId = nameTable->RegisterName(columnNames[colIdx]);

        for (int rowIndex = 0; rowIndex < std::ssize(column[colIdx]); rowIndex++) {
            TCppType value;
            if constexpr (Nullable) {
                if (!column[colIdx][rowIndex]) {
                    rowsBuilders[rowIndex].AddValue(MakeUnversionedNullValue(columnId));
                    continue;
                }
                value = *column[colIdx][rowIndex];
            } else {
                value = column[colIdx][rowIndex];
            }

            TUnversionedValue unversionedValue;
            // Switch would also work here, but this way we execute exactly one statement.
            if constexpr (YTType == EValueType::Int64) {
                unversionedValue = MakeUnversionedInt64Value(value, columnId);
            } else if constexpr (YTType == EValueType::Uint64) {
                unversionedValue = MakeUnversionedUint64Value(value, columnId);
            } else if constexpr (YTType == EValueType::Double) {
                unversionedValue = MakeUnversionedDoubleValue(value, columnId);
            } else if constexpr (YTType == EValueType::Boolean) {
                unversionedValue = MakeUnversionedBooleanValue(value, columnId);
            } else {
                static_assert(false, "YT type is not numeric");
            }
            rowsBuilders[rowIndex].AddValue(unversionedValue);
        }
    }

    std::vector<TUnversionedRow> rows;
    std::vector<TUnversionedOwningRow> owningRows;
    for (int rowIndex = 0; rowIndex < std::ssize(rowsBuilders); rowIndex++) {
        owningRows.push_back(rowsBuilders[rowIndex].FinishRow());
        rows.push_back(owningRows.back().Get());
    }
    return {std::move(rows), std::move(rowsBuilders), std::move(nameTable), std::move(owningRows)};
}

template <typename TCppType, EValueType YTType, bool Nullable>
TOwnerRows MakeUnversionedStringLikeRows(
    const std::vector<TColumn<TCppType, Nullable>>& column,
    const std::vector<std::string>& columnNames)
{
    YT_VERIFY(column.size() > 0);

    std::vector<std::string> buffer;

    auto nameTable = New<TNameTable>();

    std::vector<TUnversionedOwningRowBuilder> rowsBuilders(column[0].size());

    for (int colIdx = 0; colIdx < std::ssize(column); colIdx++) {
        auto columnId = nameTable->RegisterName(columnNames[colIdx]);

        for (int rowIndex = 0; rowIndex < std::ssize(column[colIdx]); rowIndex++) {
            if constexpr (Nullable) {
                if (!column[colIdx][rowIndex]) {
                    rowsBuilders[rowIndex].AddValue(MakeUnversionedNullValue(columnId));
                    continue;
                }
                buffer.emplace_back(*column[colIdx][rowIndex]);
            } else {
                buffer.emplace_back(column[colIdx][rowIndex]);
            }

            rowsBuilders[rowIndex].AddValue(MakeUnversionedStringLikeValue(YTType, buffer.back(), columnId));
        }
    }

    std::vector<TUnversionedRow> rows;
    std::vector<TUnversionedOwningRow> owningRows;
    for (int rowIndex = 0; rowIndex < std::ssize(rowsBuilders); rowIndex++) {
        owningRows.push_back(rowsBuilders[rowIndex].FinishRow());
        rows.push_back(owningRows.back().Get());
    }
    return {std::move(rows), std::move(rowsBuilders), std::move(nameTable), std::move(owningRows)};
}

TOwnerRows MakeUnversionedIntegerRows(
    const std::vector<TColumnInteger>& column,
    const std::vector<std::string>& columnNames,
    bool isSigned = true)
{
    if (isSigned) {
        return MakeUnversionedNumericRows<int64_t, EValueType::Int64, false>(column, columnNames);
    } else {
        return MakeUnversionedNumericRows<int64_t, EValueType::Uint64, false>(column, columnNames);
    }
}

TOwnerRows MakeUnversionedNullableIntegerRows(
    const std::vector<TColumnNullableInteger>& column,
    const std::vector<std::string>& columnNames,
    bool isSigned = true)
{
    if (isSigned) {
        return MakeUnversionedNumericRows<int64_t, EValueType::Int64, true>(column, columnNames);
    } else {
        return MakeUnversionedNumericRows<int64_t, EValueType::Uint64, true>(column, columnNames);
    }
}

TOwnerRows MakeUnversionedFloatRows(
    const std::vector<TColumnFloat>& column,
    const std::vector<std::string>& columnNames)
{
    return MakeUnversionedNumericRows<float, EValueType::Double, false>(column, columnNames);
}

TOwnerRows MakeUnversionedStringRows(
    const std::vector<TColumnString>& column,
    const std::vector<std::string>& columnNames)
{
    return MakeUnversionedStringLikeRows<std::string, EValueType::String, false>(column, columnNames);
}

TOwnerRows MakeUnversionedNullableStringRows(
    const std::vector<TColumnNullableString>& column,
    const std::vector<std::string>& columnNames)
{
    return MakeUnversionedStringLikeRows<std::string, EValueType::String, true>(column, columnNames);
}

////////////////////////////////////////////////////////////////////////////////

std::string BinaryYsonFromTextYson(const std::string& ysonString) {
    TStringStream binaryYsonString;

    TYsonWriter ysonWriter(&binaryYsonString, EYsonFormat::Binary);
    ParseYsonStringBuffer(ysonString, EYsonType::Node, &ysonWriter);

    return binaryYsonString.Str();
}

template <bool Nullable>
TOwnerRows MakeUnversionedAnyRowsFromYsonImpl(
    const std::vector<TColumn<std::string, Nullable>>& columns,
    const std::vector<std::string>& columnNames)
{
    int columnCount = columns.size();
    int rowCount = columns[0].size();

    std::vector<TColumn<std::string, Nullable>> binaryColumns;
    binaryColumns.assign(columnCount, TColumn<std::string, Nullable>(rowCount));

    for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            if constexpr (Nullable) {
                if (columns[columnIndex][rowIndex]) {
                    binaryColumns[columnIndex][rowIndex] = BinaryYsonFromTextYson(*columns[columnIndex][rowIndex]);
                }
            } else {
                binaryColumns[columnIndex][rowIndex] = BinaryYsonFromTextYson(columns[columnIndex][rowIndex]);
            }
        }
    }

    return MakeUnversionedStringLikeRows<std::string, EValueType::Any, Nullable>(binaryColumns, columnNames);
}

TOwnerRows MakeUnversionedAnyRowsFromYson(
    const std::vector<std::vector<std::string>>& columns,
    const std::vector<std::string>& columnNames)
{
    return MakeUnversionedAnyRowsFromYsonImpl<false>(columns, columnNames);
}

TOwnerRows MakeUnversionedNullableAnyRowsFromYson(
    const std::vector<std::vector<std::optional<std::string>>>& columns,
    const std::vector<std::string>& columnNames)
{
    return MakeUnversionedAnyRowsFromYsonImpl<true>(columns, columnNames);
}

////////////////////////////////////////////////////////////////////////////////

std::string MakeRandomString(int stringSize)
{
    std::string randomString;
    randomString.reserve(stringSize);
    for (int i = 0; i < stringSize; i++) {
        randomString += ('a' + rand() % 30);
    }
    return randomString;
}

////////////////////////////////////////////////////////////////////////////////

void CheckColumnNames(
    std::shared_ptr<arrow20::RecordBatch> batch,
    const std::vector<std::string>& columnNames)
{
    EXPECT_EQ(batch->num_columns(), std::ssize(columnNames));
    for (int i = 0; i < std::ssize(columnNames); i++) {
        EXPECT_EQ(batch->column_name(i), columnNames[i]);
    }
}

bool CheckMaxConst(const char* ptr)
{
    ui32 constMax = 0xFFFFFFFF;
    return *(reinterpret_cast<const uint32_t*>(ptr)) == constMax;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TArrowWriterTest, SimpleInteger)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"integer"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], EValueType::Int64),
    }));

    TStringStream outputStream;

    TColumnInteger column = {42, 179179};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);
    EXPECT_EQ(ReadInteger64Array(batch->column(0)), column);
}

TEST(TArrowWriterTest, Json)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"json"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::Json),
    }));

    TStringStream outputStream;

    TColumnString column = {"42", "true"};

    auto rows = MakeUnversionedStringRows({column}, columnNames);
    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);
    EXPECT_EQ(ReadAnyStringArray(batch->column(0)), column);
}

TEST(TArrowWriterTest, YT_20699_WrongAlign)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"date"};

    // In such a scheme, the metadata will have a size not a multiple of 2^8
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::Date),
    }));

    TStringStream outputStream;
    i64 ma = std::numeric_limits<int>::max();

    TColumnInteger column = {18367, ma};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames, false);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    std::string data(outputStream.Data(), outputStream.Size());
    auto ptr = outputStream.Data();
    auto restSize =  outputStream.Size();
    while (restSize > 0) {
        EXPECT_TRUE(restSize >= 4);
        EXPECT_TRUE(CheckMaxConst(ptr));
        ptr += 4;
        uint32_t fbSize = *(reinterpret_cast<const uint32_t*>(ptr));
        ptr += 4;
        auto message = org::apache::arrow20::flatbuf::GetMessage(ptr);
        ptr += fbSize;
        ptr += message->bodyLength();
        restSize -= (8 + fbSize + message->bodyLength());
    }
}

TEST(TArrowWriterTest, SimpleDate)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"date"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::Date),
    }));

    TStringStream outputStream;
    i64 ma = std::numeric_limits<int>::max();

    TColumnInteger column = {18367, ma};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames, false);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);
    EXPECT_EQ(ReadIntegerDateArray(batch->column(0)), column);
}

TEST(TArrowWriterTest, OptionalDate)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"date"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::Date),
    }));

    TStringStream outputStream;
    i64 ma = std::numeric_limits<int>::max();

    TColumnNullableInteger column = {18367, std::nullopt, ma, std::nullopt};

    auto rows = MakeUnversionedNullableIntegerRows({column}, columnNames, false);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);
    auto arrowColumn = batch->column(0);
    EXPECT_EQ(arrowColumn->null_count(), 2);
    auto columnData = ReadIntegerDateArray(batch->column(0));
    EXPECT_EQ(columnData[0], 18367);
    EXPECT_EQ(columnData[1], 0);
}

TEST(TArrowWriterTest, OptionalRleDate)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"date"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::Date),
    }));

    TStringStream outputStream;
    i64 ma = std::numeric_limits<int>::max();

    TColumnNullableInteger column = {20, 20, 20, 30, 30, 30, std::nullopt, std::nullopt};
    for (int i = 0; i < 100; i++) {
        column.push_back(ma);
    }

    auto rows = MakeUnversionedNullableIntegerRows({column}, columnNames, false);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    auto columnarBatch = MakeColumnarRowBatch(rows.Rows, tableSchemas[0]);
    EXPECT_TRUE(writer->WriteBatch(columnarBatch));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);
    auto arrowColumn = batch->column(0);
    EXPECT_EQ(arrowColumn->null_count(), 2);
}

TEST(TArrowWriterTest, SimpleDatatime)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"datatime"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::Datetime),
    }));

    TStringStream outputStream;

    TColumnInteger column = {1586966302, 5};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames, false);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);

    EXPECT_EQ(ReadIntegerDate64Array(batch->column(0)), column);
}

TEST(TArrowWriterTest, SimpleTimestamp)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"timestamp"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::Timestamp),
    }));

    TStringStream outputStream;

    TColumnInteger column = {1586966302504185, 5000};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames, false);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);

    EXPECT_EQ(ReadTimestampArray(batch->column(0)), column);
}

TEST(TArrowWriterTest, SimpleInterval)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"Interval"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::Interval),
    }));

    TStringStream outputStream;

    TColumnInteger column = {1586966302504185, 5000};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames, false);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);

    EXPECT_EQ(ReadInteger64Array(batch->column(0)), column);
}

TEST(TArrowWriterTest, SimpleFloat)
{
    EXPECT_TRUE(true);
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"float"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::Float),
    }));

    TStringStream outputStream;

    TColumnFloat column = {1.2, 3.14};

    auto rows = MakeUnversionedFloatRows({column}, columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);
    EXPECT_EQ(ReadFloatArray(batch->column(0)), column);
}

TEST(TArrowWriterTest, ColumnarBatch)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"integer"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], EValueType::Int64),
    }));

    TStringStream outputStream;

    TColumnInteger column = {42, 179179};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    auto columnarBatch = MakeColumnarRowBatch(rows.Rows, tableSchemas[0]);
    EXPECT_TRUE(writer->WriteBatch(columnarBatch));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);
    EXPECT_EQ(ReadInteger64Array(batch->column(0)), column);
}

TEST(TArrowWriterTest, RowBatch)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"integer"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], EValueType::Int64),
    }));

    TStringStream outputStream;

    TColumnInteger column = {42, 179179};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    auto rowBatch = CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows.Rows)));

    EXPECT_TRUE(writer->WriteBatch(rowBatch));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);
    EXPECT_EQ(ReadInteger64Array(batch->column(0)), column);
}

TEST(TArrowWriterTest, Null)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"integer", "null"};
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], EValueType::Int64),
        TColumnSchema(columnNames[1], EValueType::Null),
    }));

    TStringStream outputStream;
    auto nameTable = New<TNameTable>();
    auto columnId = nameTable->RegisterName(columnNames[0]);
    auto nullColumnId = nameTable->RegisterName(columnNames[1]);

    TUnversionedRowBuilder row1, row2;
    row1.AddValue(MakeUnversionedNullValue(columnId));
    row1.AddValue(MakeUnversionedNullValue(nullColumnId));

    row2.AddValue(MakeUnversionedInt64Value(3, columnId));
    row2.AddValue(MakeUnversionedNullValue(nullColumnId));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow()};
    auto writer = CreateArrowWriter(nameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);
    EXPECT_EQ(ReadInteger64Array(batch->column(0))[1], 3);
}

TEST(TArrowWriterTest, SimpleMultiTypes)
{
    // First table
    std::vector<std::string> columnNames1 = {"string"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames1[0], EValueType::String),
    }));

    // Second table
    std::vector<std::string> columnNames2 = {"int"};
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames2[0], EValueType::Int64),
    }));

    // Third table
    std::vector<std::string> columnNames3 = {"int2", "string2"};
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames3[0], EValueType::Int64),
        TColumnSchema(columnNames3[1], EValueType::String),
    }));

    TStringStream outputStream;

    // Register names
    auto nameTable = New<TNameTable>();
    auto stringId1 = nameTable->RegisterName(columnNames1[0]);
    auto intId1 = nameTable->RegisterName(columnNames2[0]);
    auto intId2 = nameTable->RegisterName(columnNames3[0]);
    auto stringId2 = nameTable->RegisterName(columnNames3[1]);
    auto tableId = nameTable->RegisterName("$table_index");

    auto writer = CreateArrowWriteWithSystemColumns(nameTable, &outputStream, tableSchemas);

    std::vector<TUnversionedOwningRow> owningRows;
    std::vector<TUnversionedRow> rows;

    int firstBatchSize = 0;

    // First batch

    std::vector<std::string> stringColumn1 = {MakeRandomString(7), MakeRandomString(3), MakeRandomString(10)};
    firstBatchSize += std::ssize(stringColumn1);
    for (int rowIndex = 0; rowIndex < std::ssize(stringColumn1); ++rowIndex) {
        TUnversionedOwningRowBuilder rowsBuilders;
        rowsBuilders.AddValue(MakeUnversionedStringValue(stringColumn1[rowIndex], stringId1));
        rowsBuilders.AddValue(MakeUnversionedInt64Value(0, tableId));
        owningRows.push_back(rowsBuilders.FinishRow());
        rows.push_back(owningRows.back().Get());
    }

    // Second batch

    std::vector<int64_t> intColumn1 = {1, 2, 3, 4, 5};
    firstBatchSize += std::ssize(intColumn1);
    for (int rowIndex = 0; rowIndex < std::ssize(intColumn1); ++rowIndex) {
        TUnversionedOwningRowBuilder rowsBuilders;
        rowsBuilders.AddValue(MakeUnversionedInt64Value(intColumn1[rowIndex], intId1));
        rowsBuilders.AddValue(MakeUnversionedInt64Value(1, tableId));
        owningRows.push_back(rowsBuilders.FinishRow());
        rows.push_back(owningRows.back().Get());
    }

    // Third batch

    std::vector<int64_t> intColumn2= {1, 2, 3};
    std::vector<std::string> stringColumn2 = {MakeRandomString(4), MakeRandomString(12), MakeRandomString(2)};
    firstBatchSize += std::ssize(stringColumn2);
    for (int rowIndex = 0; rowIndex < std::ssize(stringColumn2); ++rowIndex) {
        TUnversionedOwningRowBuilder rowsBuilders;
        rowsBuilders.AddValue(MakeUnversionedStringValue(stringColumn2[rowIndex], stringId2));
        rowsBuilders.AddValue(MakeUnversionedInt64Value(intColumn2[rowIndex], intId2));
        rowsBuilders.AddValue(MakeUnversionedInt64Value(2, tableId));
        owningRows.push_back(rowsBuilders.FinishRow());
        rows.push_back(owningRows.back().Get());
    }

    // Fourth batch

    std::vector<std::string> stringColumn3 = {MakeRandomString(5), MakeRandomString(6)};
    for (int rowIndex = 0; rowIndex < std::ssize(stringColumn3); ++rowIndex) {
        TUnversionedOwningRowBuilder rowsBuilders;
        rowsBuilders.AddValue(MakeUnversionedStringValue(stringColumn3[rowIndex], stringId1));
        rowsBuilders.AddValue(MakeUnversionedInt64Value(0, tableId));
        owningRows.push_back(rowsBuilders.FinishRow());
        rows.push_back(owningRows.back().Get());
    }

    // Fifth batch

    std::vector<int64_t> intColumn3 = {42, 128};
    for (int rowIndex = 0; rowIndex < std::ssize(intColumn3); ++rowIndex) {
        TUnversionedOwningRowBuilder rowsBuilders;
        rowsBuilders.AddValue(MakeUnversionedInt64Value(intColumn3[rowIndex], intId1));
        rowsBuilders.AddValue(MakeUnversionedInt64Value(1, tableId));
        owningRows.push_back(rowsBuilders.FinishRow());
        rows.push_back(owningRows.back().Get());
    }

    auto rangeRows = TRange(rows);

    EXPECT_TRUE(writer->Write(rangeRows.Slice(0, firstBatchSize)));
    EXPECT_TRUE(writer->Write(rangeRows.Slice(firstBatchSize, rangeRows.Size())));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batches = MakeAllBatch(outputStream, 5);

    // Check first batch

    EXPECT_EQ(ReadStringArray(batches[0]->column(1)), stringColumn1);

    // Check second batch

    EXPECT_EQ(ReadInteger64Array(batches[1]->column(1)), intColumn1);

    // Check third batch

    EXPECT_EQ(ReadInteger64Array(batches[2]->column(1)), intColumn2);
    EXPECT_EQ(ReadStringArray(batches[2]->column(2)), stringColumn2);

    // Check fourth batch

    EXPECT_EQ(ReadStringArray(batches[3]->column(1)), stringColumn3);

    // Check fifth batch

    EXPECT_EQ(ReadInteger64Array(batches[4]->column(1)), intColumn3);

}

TEST(TArrowWriterTest, SimpleString)
{
    std::vector<std::string> columnNames = {"string"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], EValueType::String),
    }));

    TStringStream outputStream;

    TColumnString column = {"cat", "mouse"};

    auto rows = MakeUnversionedStringRows({column}, columnNames);
    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);
    EXPECT_EQ(ReadAnyStringArray(batch->column(0)), column);
}

TEST(TArrowWriterTest, TzTypeIndex)
{
    std::vector<std::string> columnNames = {"tzDate", "tzDatetime", "tzTimestamp", "tzDate32", "tzDatetime64", "tzTimestamp64"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::TzDate),
        TColumnSchema(columnNames[1], ESimpleLogicalValueType::TzDatetime),
        TColumnSchema(columnNames[2], ESimpleLogicalValueType::TzTimestamp),
        TColumnSchema(columnNames[3], ESimpleLogicalValueType::TzDate32),
        TColumnSchema(columnNames[4], ESimpleLogicalValueType::TzDatetime64),
        TColumnSchema(columnNames[5], ESimpleLogicalValueType::TzTimestamp64),
    }));

    TStringStream outputStream;

    auto dateValue = MakeTzString<ui16>(42, "Europe/Moscow");
    auto datetimeValue = MakeTzString<ui32>(42, "Europe/Moscow");
    auto timestampValue = MakeTzString<ui64>(42, "Europe/Moscow");
    auto date32Value = MakeTzString<i32>(42, "Europe/Moscow");
    auto datetime64Value = MakeTzString<i64>(42, "Europe/Moscow");
    auto timestamp64Value = MakeTzString<i64>(42, "Europe/Moscow");

    auto rows = MakeUnversionedStringRows({
        {dateValue},
        {datetimeValue},
        {timestampValue},
        {date32Value},
        {datetime64Value},
        {timestamp64Value}},
        columnNames);

    TArrowFormatConfigPtr arrowConfig = New<TArrowFormatConfig>();
    arrowConfig->EnableTzIndex = true;
    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas, arrowConfig);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(0));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::UInt16Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::UInt16Array>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), 42);
        EXPECT_EQ(tzNameArray->Value(0), 1);
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(1));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::UInt32Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::UInt16Array>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), static_cast<ui32>(42));
        EXPECT_EQ(tzNameArray->Value(0), 1);
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(2));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::UInt64Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::UInt16Array>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), static_cast<ui64>(42));
        EXPECT_EQ(tzNameArray->Value(0), 1);
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(3));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::Int32Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::UInt16Array>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), static_cast<i32>(42));
        EXPECT_EQ(tzNameArray->Value(0), 1);
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(4));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::Int64Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::UInt16Array>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), 42);
        EXPECT_EQ(tzNameArray->Value(0), 1);
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(5));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::Int64Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::UInt16Array>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), 42);
        EXPECT_EQ(tzNameArray->Value(0), 1);
    }

}

TEST(TArrowWriterTest, TzTypeName)
{
    std::vector<std::string> columnNames = {"tzDate", "tzDatetime", "tzTimestamp", "tzDate32", "tzDatetime64", "tzTimestamp64"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::TzDate),
        TColumnSchema(columnNames[1], ESimpleLogicalValueType::TzDatetime),
        TColumnSchema(columnNames[2], ESimpleLogicalValueType::TzTimestamp),
        TColumnSchema(columnNames[3], ESimpleLogicalValueType::TzDate32),
        TColumnSchema(columnNames[4], ESimpleLogicalValueType::TzDatetime64),
        TColumnSchema(columnNames[5], ESimpleLogicalValueType::TzTimestamp64),
    }));

    TStringStream outputStream;

    auto dateValue = MakeTzString<ui16>(42, "Europe/Moscow");
    auto datetimeValue = MakeTzString<ui32>(42, "Europe/Moscow");
    auto timestampValue = MakeTzString<ui64>(42, "Europe/Moscow");
    auto date32Value = MakeTzString<i32>(42, "Europe/Moscow");
    auto datetime64Value = MakeTzString<i64>(42, "Europe/Moscow");
    auto timestamp64Value = MakeTzString<i64>(42, "Europe/Moscow");

    auto rows = MakeUnversionedStringRows({
        {dateValue},
        {datetimeValue},
        {timestampValue},
        {date32Value},
        {datetime64Value},
        {timestamp64Value}},
        columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(0));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::UInt16Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::BinaryArray>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), 42);
        EXPECT_EQ(tzNameArray->Value(0), "Europe/Moscow");
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(1));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::UInt32Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::BinaryArray>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), static_cast<ui32>(42));
        EXPECT_EQ(tzNameArray->Value(0), "Europe/Moscow");
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(2));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::UInt64Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::BinaryArray>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), static_cast<ui64>(42));
        EXPECT_EQ(tzNameArray->Value(0), "Europe/Moscow");
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(3));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::Int32Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::BinaryArray>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), static_cast<i32>(42));
        EXPECT_EQ(tzNameArray->Value(0), "Europe/Moscow");
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(4));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::Int64Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::BinaryArray>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), 42);
        EXPECT_EQ(tzNameArray->Value(0), "Europe/Moscow");
    }

    {
        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(5));
        auto timestampArray = std::dynamic_pointer_cast<arrow20::Int64Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::BinaryArray>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), 42);
        EXPECT_EQ(tzNameArray->Value(0), "Europe/Moscow");
    }

}

TEST(TArrowWriterTest, TzRle)
{
    std::vector<std::string> columnNames = {"tzDate"};
    std::vector<TTableSchemaPtr> tableSchemas;

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::TzDate),
    }));

    TStringStream outputStream;

    auto dateValue = MakeTzString<ui16>(42, "Europe/Moscow");
    std::vector<std::string> tzValues;
    for (int i = 0; i < 100; ++i) {
        tzValues.push_back(dateValue);
    }
    auto rows = MakeUnversionedStringRows({tzValues}, columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    {
        EXPECT_TRUE(IsDictColumn(batch->column(0)));
        auto dictArray = std::dynamic_pointer_cast<arrow20::DictionaryArray>(batch->column(0));

        auto structArrowArray = std::dynamic_pointer_cast<arrow20::StructArray>(dictArray->dictionary());
        auto timestampArray = std::dynamic_pointer_cast<arrow20::UInt16Array>(structArrowArray->field(0));
        auto tzNameArray = std::dynamic_pointer_cast<arrow20::BinaryArray>(structArrowArray->field(1));
        EXPECT_EQ(timestampArray->Value(0), 42);
        EXPECT_EQ(tzNameArray->Value(0), "Europe/Moscow");
    }

}

TEST(TArrowWriterTest, NullTz)
{
    std::vector<std::string> columnNames = {"tzDate"};
    std::vector<TTableSchemaPtr> tableSchemas;

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], ESimpleLogicalValueType::TzDate),
    }));

    TStringStream outputStream;

    std::vector<std::optional<std::string>> tzValues;
    for (int i = 0; i < 10; ++i) {
        tzValues.push_back(std::nullopt);
    }

    auto rows = MakeUnversionedNullableStringRows({tzValues}, columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

}

TEST(TArrowWriterTest, DictionaryString)
{
    std::vector<std::string> columnNames = {"string"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], EValueType::String),
    }));
    TStringStream outputStream;

    std::string longString, longString2;
    for (int i = 0; i < 20; i++) {
        longString += 'a';
        longString2 += 'b';
    }

    auto rows = MakeUnversionedStringRows({{longString, longString2, longString, longString2}}, columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);
    EXPECT_EQ(ReadAnyStringArray(batch->column(0))[0], longString);
    EXPECT_TRUE(IsDictColumn(batch->column(0)));
}

TEST(TArrowWriterTest, EnumString)
{
    const int batchCount = 10;
    const int rowCount = 10;

    std::vector<std::string> columnNames = {"string", "string2", "string3"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], EValueType::String),
        TColumnSchema(columnNames[1], EValueType::String),
        TColumnSchema(columnNames[2], EValueType::String),
    }));

    TStringStream outputStream;

    std::string LONG_STRING = "abcdefghijklmnopqrst";
    std::string A_STRING = "aaaaaaaaaaaaaaaaaaaaaaa";
    std::string CAT = "cat";
    std::string FOO = "foofoofoofoofoofoo";
    std::string BAR = "barbarbarbarbarbarbar";

    std::vector<std::string> enumStrings = {LONG_STRING, A_STRING, CAT, FOO, BAR};
    auto nameTable = New<TNameTable>();
    nameTable->RegisterName(columnNames[0]);
    nameTable->RegisterName(columnNames[1]);
    nameTable->RegisterName(columnNames[2]);

    auto writer = CreateArrowWriter(nameTable, &outputStream, tableSchemas);
    std::vector<std::vector<std::optional<std::string>>> stringColumns;
    std::vector<std::vector<std::optional<std::string>>> constColumns;
    std::vector<std::vector<std::optional<std::string>>> optColumns;

    for (int i = 0; i < batchCount; i++) {
        std::vector<std::optional<std::string>> column;
        std::vector<std::optional<std::string>> constColumn;
        std::vector<std::optional<std::string>> optColumn;

        for (int j = 0; j < rowCount; j++) {
            column.push_back(enumStrings[rand() % enumStrings.size()]);
            constColumn.push_back(enumStrings[0]);
            if (rand() % 2 == 0) {
                optColumn.push_back(enumStrings[rand() % enumStrings.size()]);
            } else {
                optColumn.push_back(std::nullopt);
            }
        }
        auto rows = MakeUnversionedNullableStringRows({column, constColumn, optColumn}, columnNames);
        EXPECT_TRUE(writer->Write(rows.Rows));
        stringColumns.push_back(column);
        constColumns.push_back(constColumn);
        optColumns.push_back(optColumn);
    }

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batches = MakeAllBatch(outputStream, batchCount);

    int batchIndex = 0;
    for (const auto& batch : batches) {
        CheckColumnNames(batch, columnNames);

        auto column = ReadAnyStringArray(batch->column(0));
        auto constColumn = ReadAnyStringArray(batch->column(1));
        auto optColumn = ReadAnyStringArray(batch->column(2));

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            if (optColumns[batchIndex][rowIndex] == std::nullopt) {
                EXPECT_TRUE(batch->column(2)->IsNull(rowIndex));
            } else {
                EXPECT_EQ(optColumn[rowIndex], *optColumns[batchIndex][rowIndex]);
            }
            EXPECT_EQ(column[rowIndex], *stringColumns[batchIndex][rowIndex]);
            EXPECT_EQ(constColumn[rowIndex], *constColumns[batchIndex][rowIndex]);
        }

        batchIndex++;
    }

}

TEST(TArrowWriterTest, DictionaryAndDirectStrings)
{
    std::vector<std::string> columnNames = {"string"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], EValueType::String),
    }));

    TStringStream outputStream;

    std::string longString, longString2;
    for (int i = 0; i < 20; i++) {
        longString += 'a';
        longString2 += 'b';
    }
    TColumnString firstColumn = {longString, longString2, longString, longString2};
    TColumnString secondColumn = {"cat", "dog", "mouse", "table"};

    auto dictRows = MakeUnversionedStringRows({firstColumn}, columnNames);
    auto directRows = MakeUnversionedStringRows({secondColumn}, columnNames);

    auto writer = CreateArrowWriter(dictRows.NameTable, &outputStream, tableSchemas);

    // Write first batch, that will be decode as dictionary.
    EXPECT_TRUE(writer->Write(dictRows.Rows));

    // Write second batch, that will be decode as direct.
    EXPECT_TRUE(writer->Write(directRows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();


    auto batches = MakeAllBatch(outputStream, 2);

    CheckColumnNames(batches[0], columnNames);
    CheckColumnNames(batches[1], columnNames);

    EXPECT_EQ(ReadAnyStringArray(batches[0]->column(0)), firstColumn);
    EXPECT_EQ(ReadAnyStringArray(batches[1]->column(0)), secondColumn);
}

TEST(TArrowWriterTest, SeveralIntegerColumnsOneBatch)
{
    // Constans.
    const int columnCount = 100;
    const int rowCount = 100;

    std::vector<TTableSchemaPtr> tableSchemas;
    TStringStream outputStream;

    std::vector<std::string> columnNames;
    std::vector<TColumnInteger> columnsElements(columnCount);

    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        // Create column name.
        std::string columnName = "integer" + std::to_string(columnIndex);
        columnNames.push_back(columnName);

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            columnsElements[columnIndex].push_back(rand());
        }
    }

    std::vector<TColumnSchema> schemas_;
    for (int columnIdx = 0; columnIdx < columnCount; columnIdx++) {
        schemas_.push_back(TColumnSchema(columnNames[columnIdx], EValueType::Int64));
    }
    tableSchemas.push_back(New<TTableSchema>(schemas_));

    auto rows = MakeUnversionedIntegerRows(columnsElements, columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        EXPECT_EQ(ReadInteger64Array(batch->column(columnIndex)), columnsElements[columnIndex]);
    }
}

TEST(TArrowWriterTest, SeveralStringColumnsOneBatch)
{
    const int columnCount = 10;
    const int rowCount = 10;
    const int stringSize = 10;

    std::vector<TTableSchemaPtr> tableSchemas;

    TStringStream outputStream;

    std::vector<std::string> columnNames;
    std::vector<TColumnString> columnsElements(columnCount);

    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {

        std::string columnName = "string" + std::to_string(columnIndex);
        columnNames.push_back(columnName);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            columnsElements[columnIndex].push_back(MakeRandomString(stringSize));
        }
    }

    std::vector<TColumnSchema> schemas_;
    for (int columnIdx = 0; columnIdx < columnCount; columnIdx++) {
        schemas_.push_back(TColumnSchema(columnNames[columnIdx], EValueType::String));
    }
    tableSchemas.push_back(New<TTableSchema>(schemas_));

    auto rows = MakeUnversionedStringRows(columnsElements, columnNames);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        EXPECT_EQ(ReadAnyStringArray(batch->column(columnIndex)), columnsElements[columnIndex]);
    }
}

TEST(TArrowWriterTest, SeveralMultiTypesColumnsOneBatch)
{
    // Constants.
    const int rowCount = 10;
    const int stringSize = 10;

    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema("bool", EValueType::Boolean),
        TColumnSchema("double", EValueType::Double),
        TColumnSchema("any", EValueType::Any)
    }));

    TStringStream outputStream;

    auto nameTable = New<TNameTable>();
    std::vector<TUnversionedOwningRowBuilder> rowsBuilders(rowCount);

    std::vector<std::string> columnNames;

    std::vector<bool> boolColumn;
    std::vector<double> doubleColumn;
    std::vector<std::string> anyColumn;
    std::vector<TUnversionedRow> rows;

    // Fill bool column.
    std::string columnName = "bool";
    auto boolId = nameTable->RegisterName(columnName);
    columnNames.push_back(columnName);
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        boolColumn.push_back((rand() % 2) == 0);

        rowsBuilders[rowIndex].AddValue(MakeUnversionedBooleanValue(boolColumn[rowIndex], boolId));
    }

    // Fill double column.
    columnName = "double";
    auto columnId = nameTable->RegisterName(columnName);
    columnNames.push_back(columnName);
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        doubleColumn.push_back((double)(rand() % 100) / 10.0);
        rowsBuilders[rowIndex].AddValue(MakeUnversionedDoubleValue(doubleColumn[rowIndex], columnId));
    }

    // Fill any column.
    columnName = "any";
    auto anyId = nameTable->RegisterName(columnName);
    columnNames.push_back(columnName);
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        std::string randomString = MakeRandomString(stringSize);

        anyColumn.push_back(randomString);

        rowsBuilders[rowIndex].AddValue(MakeUnversionedAnyValue(randomString, anyId));
    }

    std::vector<TUnversionedOwningRow> owningRows;
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        owningRows.push_back(rowsBuilders[rowIndex].FinishRow());
        rows.push_back(owningRows.back().Get());
    }

    auto writer = CreateArrowWriter(nameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows));

    writer->Close()
        .Get()
        .ThrowOnError();


    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    EXPECT_EQ(ReadBooleanArray(batch->column(0)), boolColumn);
    EXPECT_EQ(ReadDoubleArray(batch->column(1)), doubleColumn);
    EXPECT_EQ(ReadAnyStringArray(batch->column(2)), anyColumn);
}

TEST(TArrowWriterTest, SeveralIntegerSeveralBatches)
{
    // Constants.
    const int columnCount = 10;
    const int rowCount = 10;
    const int batchCount = 10;

    std::vector<std::string> columnNames;
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<TColumnSchema> schemas_;

    for (int columnIdx = 0; columnIdx < columnCount; columnIdx++) {
        std::string columnName = "integer" + std::to_string(columnIdx);
        columnNames.push_back(columnName);
        schemas_.push_back(TColumnSchema(columnNames[columnIdx], EValueType::Int64));
    }
    tableSchemas.push_back(New<TTableSchema>(schemas_));

    TStringStream outputStream;
    std::vector<std::vector<TColumnInteger>> columnsElements(batchCount, std::vector<TColumnInteger>(columnCount));

    auto nameTable = New<TNameTable>();
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        std::string columnName = "integer" + std::to_string(columnIndex);
        nameTable->RegisterName(columnName);
    }
    auto writer = CreateArrowWriter(nameTable, &outputStream, tableSchemas);


    for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                columnsElements[batchIndex][columnIndex].push_back(rand());
            }
        }

        auto rows = MakeUnversionedIntegerRows(columnsElements[batchIndex], columnNames);
        EXPECT_TRUE(writer->Write(rows.Rows));
    }

    writer->Close()
        .Get()
        .ThrowOnError();


    auto batches = MakeAllBatch(outputStream, batchCount);

    int batchIndex = 0;
    for (const auto& batch : batches) {
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            CheckColumnNames(batch, columnNames);
            EXPECT_EQ(ReadInteger64Array(batch->column(columnIndex)), columnsElements[batchIndex][columnIndex]);
        }
        batchIndex++;
    }
}

TEST(TArrowWriterTest, SeveralMultiTypesSeveralBatches)
{
    // Сonstants.
    const int rowCount = 10;
    const int batchCount = 10;
    const int stringSize = 10;

    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema("bool", EValueType::Boolean),
        TColumnSchema("double", EValueType::Double),
        TColumnSchema("any", EValueType::Any)
    }));

    TStringStream outputStream;

    auto nameTable = New<TNameTable>();

    std::vector<std::string> columnNames = {"bool", "double", "any"};
    auto boolId = nameTable->RegisterName(columnNames[0]);
    auto doubleId = nameTable->RegisterName(columnNames[1]);
    auto anyId = nameTable->RegisterName(columnNames[2]);

    std::vector<TColumnBoolWithNulls> boolColumns(batchCount);
    std::vector<TColumnDoubleWithNulls> doubleColumns(batchCount);
    std::vector<TColumnStringWithNulls> anyColumns(batchCount);

    auto writer = CreateArrowWriter(nameTable, &outputStream, tableSchemas);

    std::vector<TUnversionedOwningRow> owningRows;

    for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
        std::vector<TUnversionedOwningRowBuilder> rowsBuilders(rowCount);
        std::vector<TUnversionedRow> rows;

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            if (rand() % 2 == 0) {
                boolColumns[batchIndex].push_back(std::nullopt);
                doubleColumns[batchIndex].push_back(std::nullopt);
                anyColumns[batchIndex].push_back(std::nullopt);
                rowsBuilders[rowIndex].AddValue(MakeUnversionedNullValue(boolId));
                rowsBuilders[rowIndex].AddValue(MakeUnversionedNullValue(doubleId));
                rowsBuilders[rowIndex].AddValue(MakeUnversionedNullValue(anyId));
            } else {
                boolColumns[batchIndex].push_back((rand() % 2) == 0);
                rowsBuilders[rowIndex].AddValue(MakeUnversionedBooleanValue(*boolColumns[batchIndex][rowIndex], boolId));

                doubleColumns[batchIndex].push_back((double)(rand() % 100) / 10.0);
                rowsBuilders[rowIndex].AddValue(MakeUnversionedDoubleValue(*doubleColumns[batchIndex][rowIndex], doubleId));

                std::string randomString = MakeRandomString(stringSize);
                anyColumns[batchIndex].push_back(randomString);
                rowsBuilders[rowIndex].AddValue(MakeUnversionedAnyValue(randomString, anyId));
            }
            owningRows.push_back(rowsBuilders[rowIndex].FinishRow());
            rows.push_back(owningRows.back().Get());
        }

        EXPECT_TRUE(writer->Write(rows));
    }

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batches = MakeAllBatch(outputStream, batchCount);
    int batchIndex = 0;
    for (const auto& batch : batches) {
        CheckColumnNames(batch, columnNames);

        auto boolArray = ReadBooleanArray(batch->column(0));
        auto doubleArray = ReadDoubleArray(batch->column(1));
        auto anyArray = ReadAnyStringArray(batch->column(2));

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            if (boolColumns[batchIndex][rowIndex] == std::nullopt) {
                EXPECT_TRUE(batch->column(0)->IsNull(rowIndex));
                EXPECT_TRUE(batch->column(1)->IsNull(rowIndex));
                EXPECT_TRUE(batch->column(2)->IsNull(rowIndex));
            } else {
                EXPECT_EQ(boolArray[rowIndex], *boolColumns[batchIndex][rowIndex]);
                EXPECT_EQ(doubleArray[rowIndex], *doubleColumns[batchIndex][rowIndex]);
                EXPECT_EQ(anyArray[rowIndex], *anyColumns[batchIndex][rowIndex]);
            }
        }

        batchIndex++;
    }
}

TEST(TArrowWriterTest, AnyMetadata)
{
    std::string columnName = "any";
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {columnName};
    std::vector<std::string> anyColumn;

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], EValueType::Any),
    }));

    TStringStream outputStream;

    auto nameTable = New<TNameTable>();
    auto anyId = nameTable->RegisterName(columnName);
    size_t rowCount = 2;
    size_t stringSize = 2;
    std::vector<TUnversionedOwningRowBuilder> rowsBuilders(rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        std::string randomString = MakeRandomString(stringSize);

        anyColumn.push_back(randomString);

        rowsBuilders[rowIndex].AddValue(MakeUnversionedAnyValue(randomString, anyId));
    }

    std::vector<TUnversionedOwningRow> owningRows;
    std::vector<TUnversionedRow> rows;
    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        owningRows.push_back(rowsBuilders[rowIndex].FinishRow());
        rows.push_back(owningRows.back().Get());
    }

    auto writer = CreateArrowWriter(nameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);

    auto columnMetadata = batch->schema()->field(0)->metadata();
    EXPECT_TRUE(columnMetadata);
    auto value = *(columnMetadata->Get("YtType"));
    EXPECT_EQ(value, "yson");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TArrowWriterComplexTest, BasicStruct)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"struct"};

    auto structType = StructLogicalType({
        TStructField{"a", "a", SimpleLogicalType(ESimpleLogicalValueType::String)},
        TStructField{"b", "b", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
    }, /*removedFieldStableNames*/ {});

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], structType),
    }));

    TStringStream outputStream;

    std::vector<std::string> ysonStrings = {
        "[foo;123;]",
        "[bar;456;]",
    };

    auto rows = MakeUnversionedAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto structArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(0));

    EXPECT_EQ(
        ReadStringArray(structArray->GetFieldByName("a")),
        std::vector<std::string>({"foo", "bar"}));
    EXPECT_EQ(
        ReadInteger64Array(structArray->GetFieldByName("b")),
        std::vector<i64>({123, 456}));
}

TEST(TArrowWriterComplexTest, BasicList)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"list"};

    auto listType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64));

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], listType),
    }));

    TStringStream outputStream;

    std::vector<std::string> ysonStrings = {
        "[1;2;3;]",
        "[5;8;]",
    };

    auto rows = MakeUnversionedAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto listArray = std::dynamic_pointer_cast<arrow20::ListArray>(batch->column(0));

    EXPECT_EQ(
        ReadInteger32Array(listArray->offsets()),
        std::vector<ui32>({0, 3, 5}));
    EXPECT_EQ(
        ReadInteger64Array(listArray->Flatten().ValueOrDie()),
        std::vector<i64>({1, 2, 3, 5, 8}));
}

TEST(TArrowWriterComplexTest, BasicDict)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"dict"};

    auto dictType = DictLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        SimpleLogicalType(ESimpleLogicalValueType::String));

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], dictType),
    }));

    TStringStream outputStream;

    std::vector<std::string> ysonStrings = {
        "[[12;\"foo\";];[34;\"bar\";];]",
        "[[56;\"\"];]",
    };

    auto rows = MakeUnversionedAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto mapArray = std::dynamic_pointer_cast<arrow20::MapArray>(batch->column(0));

    EXPECT_EQ(
        ReadInteger32Array(mapArray->offsets()),
        std::vector<ui32>({0, 2, 3}));
    EXPECT_EQ(
        ReadInteger64Array(mapArray->keys()),
        std::vector<i64>({12, 34, 56}));
    EXPECT_EQ(
        ReadStringArray(mapArray->items()),
        std::vector<std::string>({"foo", "bar", ""}));
}

TEST(TArrowWriterComplexTest, OptionalStruct)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"optional"};

    auto listType = OptionalLogicalType(StructLogicalType({
        TStructField{"integer", "integer", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
    }, /*removedFieldStableNames*/ {}));

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], listType),
    }));

    std::vector<std::optional<std::string>> ysonStrings = {
        "[12;]",
        std::nullopt,
        "[34;]",
    };

    auto rows = MakeUnversionedNullableAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    TStringStream outputStream;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto structArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(0));

    EXPECT_EQ(structArray->null_bitmap_data()[0], 0b101);
    auto integers = ReadInteger64Array(structArray->GetFieldByName("integer"));
    EXPECT_EQ(integers[0], 12);
    EXPECT_EQ(integers[2], 34);
}

TEST(TArrowWriterComplexTest, StructOptional)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"struct"};

    auto listType = StructLogicalType({
        TStructField{
            .Name = "integer",
            .StableName = "integer",
            .Type = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        },
    }, /*removedFieldStableNames*/ {});

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], listType),
    }));

    std::vector<std::string> ysonStrings = {
        "[12;]",
        "[#;]",
        "[34;]",
    };

    auto rows = MakeUnversionedAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    TStringStream outputStream;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto structArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(0));

    auto integerArray = structArray->GetFieldByName("integer");
    EXPECT_EQ(integerArray->null_bitmap_data()[0], 0b101);
    auto integers = ReadInteger64Array(integerArray);
    EXPECT_EQ(integers[0], 12);
    EXPECT_EQ(integers[2], 34);
}

TEST(TArrowWriterComplexTest, DictionaryStruct)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"struct"};

    auto structType = OptionalLogicalType(StructLogicalType({
        TStructField{
            .Name = "a",
            .StableName = "a",
            .Type = SimpleLogicalType(ESimpleLogicalValueType::String),
        },
        TStructField{
            .Name = "b",
            .StableName = "b",
            .Type = SimpleLogicalType(ESimpleLogicalValueType::Int64),
        },
    }, /*removedFieldStableNames*/ {}));

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], structType),
    }));

    TStringStream outputStream;

    const int copiesCount = 10;

    std::vector<std::optional<std::string>> ysonStrings;
    std::vector<std::optional<std::string>> strings;
    std::vector<std::optional<int64_t>> integers;
    for (int i = 0; i < copiesCount; ++i) {
        ysonStrings.push_back("[foo;123;]");
        strings.push_back("foo");
        integers.push_back(123);
        ysonStrings.push_back("[bar;456;]");
        strings.push_back("bar");
        integers.push_back(456);
        ysonStrings.push_back(std::nullopt);
        strings.push_back(std::nullopt);
        integers.push_back(std::nullopt);
    }

    auto rows = MakeUnversionedNullableAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto structArray = std::dynamic_pointer_cast<arrow20::DictionaryArray>(batch->column(0));

    auto indices = ReadUInteger32Array(structArray->indices());
    auto dictionary = std::dynamic_pointer_cast<arrow20::StructArray>(structArray->dictionary());
    auto stringValues = ReadStringArray(dictionary->GetFieldByName("a"));
    auto integerValues = ReadInteger64Array(dictionary->GetFieldByName("b"));

    for (int i = 0; i < std::ssize(ysonStrings); ++i) {
        if (ysonStrings[i]) {
            EXPECT_EQ(*strings[i], stringValues[indices[i]]);
            EXPECT_EQ(*integers[i], integerValues[indices[i]]);
        } else {
            EXPECT_TRUE(structArray->IsNull(i));
        }
    }
}

TEST(TArrowWriterComplexTest, OptionalOptional)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"optional"};

    auto listType = OptionalLogicalType(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)));

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], listType),
    }));

    std::vector<std::optional<std::string>> ysonStrings = {
        std::nullopt,
        "[#;]",
        "[-42;]",
    };

    auto rows = MakeUnversionedNullableAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    TStringStream outputStream;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto columnMetadata = batch->schema()->field(0)->metadata();
    EXPECT_TRUE(columnMetadata);
    auto value = *(columnMetadata->Get(YTTypeMetadataKey));
    EXPECT_EQ(value, YTTypeMetadataValueNestedOptional);

    auto outerOptionalArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(0));

    EXPECT_EQ(outerOptionalArray->null_bitmap_data()[0], 0b110);

    auto innerOptionalArray = std::dynamic_pointer_cast<arrow20::Int64Array>(outerOptionalArray->fields()[0]);

    EXPECT_TRUE(innerOptionalArray->IsNull(1));
    EXPECT_FALSE(innerOptionalArray->IsNull(2));

    auto integerValues = ReadInteger64Array(innerOptionalArray);

    EXPECT_EQ(integerValues[2], -42);
}

TEST(TArrowWriterTest, EmptyStruct)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"struct"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], StructLogicalType({}, /*removedFieldStableNames*/ {})),
    }));

    TStringStream outputStream;

    std::vector<std::string> ysonStrings(3, "[]");

    auto rows = MakeUnversionedAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto columnMetadata = batch->schema()->field(0)->metadata();
    EXPECT_TRUE(columnMetadata);
    auto value = *(columnMetadata->Get(YTTypeMetadataKey));
    EXPECT_EQ(value, YTTypeMetadataValueEmptyStruct);
}

TEST(TArrowWriterComplexTest, OptionalEmptyStruct)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"optional"};

    auto optionalType = OptionalLogicalType(StructLogicalType({}, /*removedFieldStableNames*/ {}));

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], optionalType),
    }));

    std::vector<std::optional<std::string>> ysonStrings = {
        "[]",
        std::nullopt,
        std::nullopt,
        "[]",
        std::nullopt,
    };

    auto rows = MakeUnversionedNullableAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    TStringStream outputStream;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto structArray = batch->column(0);

    for (int rowIndex = 0; rowIndex < std::ssize(ysonStrings); ++rowIndex) {
        EXPECT_EQ(ysonStrings[rowIndex].has_value(), structArray->IsValid(rowIndex));
    }

    auto columnMetadata = batch->schema()->field(0)->metadata();
    EXPECT_TRUE(columnMetadata);
    auto value = *(columnMetadata->Get(YTTypeMetadataKey));
    EXPECT_EQ(value, YTTypeMetadataValueEmptyStruct);
}

TEST(TArrowWriterComplexTest, NullTypes)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"null", "null_struct"};

    auto optionalType = OptionalLogicalType(StructLogicalType({}, /*removedFieldStableNames*/ {}));

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], NullLogicalType()),
        TColumnSchema(
            columnNames[1],
            OptionalLogicalType(StructLogicalType(
                {{"null", "null", NullLogicalType()}},
                /*removedFieldStableNames*/ {}))),
    }));

    std::vector<std::optional<std::string>> nulls = {
        std::nullopt,
        std::nullopt,
    };
    std::vector<std::optional<std::string>> nullStructs = {
        "[#;]",
        std::nullopt,
    };

    auto rows = MakeUnversionedNullableAnyRowsFromYson({nulls, nullStructs}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;

    TStringStream outputStream;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto nullArray = batch->column(0);
    auto nullStructArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(1));
    auto nestedNullArray = nullStructArray->GetFieldByName("null");

    ASSERT_EQ(nullArray->null_count(), 2);
    ASSERT_TRUE(nullStructArray->IsValid(0));
    ASSERT_TRUE(nullStructArray->IsNull(1));
    ASSERT_TRUE(nestedNullArray->IsNull(0));
}

TEST(TArrowWriterComplexTest, NestedTzType)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"tz"};

    auto type = StructLogicalType({
        {"a", "a", SimpleLogicalType(ESimpleLogicalValueType::String)},
        {"b", "b", SimpleLogicalType(ESimpleLogicalValueType::TzTimestamp)},
    }, /*removedFieldStableNames*/ {});

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], type),
    }));

    constexpr ESimpleLogicalValueType UnderlyingDateType = GetUnderlyingDateType<ESimpleLogicalValueType::TzTimestamp>();
    using TInt = TUnderlyingTimestampIntegerType<UnderlyingDateType>;

    std::vector<std::string> ysonStrings = {
        "[\"123\";\"" + MakeTzString<TInt>(0, GetTzName(0)) + "\";]",
        "[\"456\";\"" + MakeTzString<TInt>(1, GetTzName(1)) + "\";]",
    };

    auto rows = MakeUnversionedAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;
    config->EnableTzIndex = false;

    TStringStream outputStream;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto structArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(0));
    auto tzArray = std::dynamic_pointer_cast<arrow20::StructArray>(structArray->GetFieldByName("b"));
    auto timestampArray = ReadUInteger64Array(tzArray->GetFieldByName("Timestamp"));
    auto tzNameArray = ReadStringArray(tzArray->GetFieldByName("TzName"));

    ASSERT_EQ(timestampArray, std::vector<ui64>({0, 1}));
    ASSERT_EQ(tzNameArray, std::vector<std::string>({std::string(GetTzName(0)), std::string(GetTzName(1))}));
}

TEST(TArrowWriterComplexTest, NestedTzTypeWithIndices)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"tz"};

    auto type = StructLogicalType({
        {"a", "a", SimpleLogicalType(ESimpleLogicalValueType::String)},
        {"b", "b", SimpleLogicalType(ESimpleLogicalValueType::TzTimestamp)},
    }, /*removedFieldStableNames*/ {});

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(columnNames[0], type),
    }));

    constexpr ESimpleLogicalValueType UnderlyingDateType = GetUnderlyingDateType<ESimpleLogicalValueType::TzTimestamp>();
    using TInt = TUnderlyingTimestampIntegerType<UnderlyingDateType>;

    std::vector<std::string> ysonStrings = {
        "[\"123\";\"" + MakeTzString<TInt>(0, GetTzName(0)) + "\";]",
        "[\"456\";\"" + MakeTzString<TInt>(1, GetTzName(1)) + "\";]",
    };

    auto rows = MakeUnversionedAnyRowsFromYson({ysonStrings}, columnNames);

    auto config = New<TArrowFormatConfig>();
    config->EnableComplexTypes = true;
    config->EnableTzIndex = true;

    TStringStream outputStream;

    auto writer = CreateArrowWriter(
        rows.NameTable,
        &outputStream,
        tableSchemas,
        config);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);

    CheckColumnNames(batch, columnNames);

    auto structArray = std::dynamic_pointer_cast<arrow20::StructArray>(batch->column(0));
    auto tzArray = std::dynamic_pointer_cast<arrow20::StructArray>(structArray->GetFieldByName("b"));
    auto timestampArray = ReadUInteger64Array(tzArray->GetFieldByName("Timestamp"));
    auto tzIndexArray = ReadUInteger16Array(tzArray->GetFieldByName("TzIndex"));

    ASSERT_EQ(timestampArray, std::vector<ui64>({0, 1}));
    ASSERT_EQ(tzIndexArray, std::vector<ui16>({0, 1}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
