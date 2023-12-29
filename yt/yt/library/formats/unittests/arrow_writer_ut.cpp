#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/client/arrow/fbs/Message.fbs.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

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

#include <yt/yt/library/named_value/named_value.h>

#include <util/stream/null.h>
#include <util/string/hex.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <limits>
#include <stdlib.h>

namespace NYT::NTableClient {

namespace {

using namespace NChunkClient;
using namespace NFormats;
using namespace NNamedValue;
using namespace NTableClient;

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
        /*dataSink*/ std::nullopt);

    TUnversionedRowsBuilder builder;

    chunkWriter->Write(rows);
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
        .MaxRowsPerRead = static_cast<i64>(rows.size()) + 10,
        .Columnar = true};
    auto batch = ReadRowBatch(schemalessRangeChunkReader, opt);
    return batch;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateArrowWriter(TNameTablePtr nameTable,
    IOutputStream* outputStream,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    auto controlAttributes = NYT::New<TControlAttributesConfig>();
    controlAttributes->EnableTableIndex = false;
    controlAttributes->EnableRowIndex = false;
    controlAttributes->EnableRangeIndex = false;
    controlAttributes->EnableTabletIndex = false;
    return CreateWriterForArrow(
        nameTable,
        schemas,
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
        nameTable,
        schemas,
        NConcurrency::CreateAsyncAdapter(static_cast<IOutputStream*>(outputStream)),
        false,
        controlAttributes,
        0);
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow::RecordBatch> MakeBatch(const TStringStream& outputStream)
{
    auto buffer = arrow::Buffer(reinterpret_cast<const uint8_t*>(outputStream.Data()), outputStream.Size());
    arrow::io::BufferReader bufferReader(buffer);

    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> batchReader = (arrow::ipc::RecordBatchStreamReader::Open(&bufferReader)).ValueOrDie();

    auto batch = batchReader->Next().ValueOrDie();
    return batch;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> MakeAllBatch(const TStringStream& outputStream, int batchNumb)
{
    auto buffer = arrow::Buffer(reinterpret_cast<const uint8_t*>(outputStream.Data()), outputStream.Size());
    arrow::io::BufferReader bufferReader(buffer);

    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> batchReader = (arrow::ipc::RecordBatchStreamReader::Open(&bufferReader)).ValueOrDie();

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (int i = 0; i < batchNumb; i++) {
        auto batch = batchReader->Next().ValueOrDie();
        if (batch == nullptr) {
            batchReader = (arrow::ipc::RecordBatchStreamReader::Open(&bufferReader)).ValueOrDie();
            batchNumb++;
        } else {
            batches.push_back(batch);
        }
    }
    return batches;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<int64_t> ReadInteger64Array(const std::shared_ptr<arrow::Array>& array)
{
    auto int64Array = std::dynamic_pointer_cast<arrow::Int64Array>(array);
    YT_VERIFY(int64Array);
    return  {int64Array->raw_values(), int64Array->raw_values() + array->length()};
}

std::vector<int64_t> ReadIntegerDateArray(const std::shared_ptr<arrow::Array>& array)
{
    auto int32Array = std::dynamic_pointer_cast<arrow::Date32Array>(array);
    YT_VERIFY(int32Array);
    return  {int32Array->raw_values(), int32Array->raw_values() + array->length()};
}

std::vector<int64_t> ReadIntegerDate64Array(const std::shared_ptr<arrow::Array>& array)
{
    auto int32Array = std::dynamic_pointer_cast<arrow::Date64Array>(array);
    YT_VERIFY(int32Array);
    return  {int32Array->raw_values(), int32Array->raw_values() + array->length()};
}

std::vector<int64_t> ReadTimestampArray(const std::shared_ptr<arrow::Array>& array)
{
    auto int64Array = std::dynamic_pointer_cast<arrow::TimestampArray>(array);
    YT_VERIFY(int64Array);
    return  {int64Array->raw_values(), int64Array->raw_values() + int64Array->length()};
}

std::vector<uint32_t> ReadInteger32Array(const std::shared_ptr<arrow::Array>& array)
{
    auto int32Array = std::dynamic_pointer_cast<arrow::UInt32Array>(array);
    YT_VERIFY(int32Array);
    return  {int32Array->raw_values(), int32Array->raw_values() + array->length()};
}

std::vector<std::string> ReadStringArray(const std::shared_ptr<arrow::Array>& array)
{
    auto arraySize = array->length();
    auto binArray = std::dynamic_pointer_cast<arrow::BinaryArray>(array);
    YT_VERIFY(binArray);
    std::vector<std::string> stringArray;
    for (int i = 0; i < arraySize; i++) {
        stringArray.push_back(binArray->GetString(i));
    }
    return stringArray;
}

std::vector<bool> ReadBoolArray(const std::shared_ptr<arrow::Array>& array)
{
    auto arraySize = array->length();
    auto boolArray = std::dynamic_pointer_cast<arrow::BooleanArray>(array);
    YT_VERIFY(boolArray);
    std::vector<bool> result;
    for (int i = 0; i < arraySize; i++) {
        result.push_back(boolArray->Value(i));
    }
    return result;
}

std::vector<double> ReadDoubleArray(const std::shared_ptr<arrow::Array>& array)
{
    auto doubleArray = std::dynamic_pointer_cast<arrow::DoubleArray>(array);
    YT_VERIFY(doubleArray);
    return  {doubleArray->raw_values(), doubleArray->raw_values() + array->length()};
}

std::vector<float> ReadFloatArray(const std::shared_ptr<arrow::Array>& array)
{
    auto floatArray = std::dynamic_pointer_cast<arrow::FloatArray>(array);
    YT_VERIFY(floatArray);
    return  {floatArray->raw_values(), floatArray->raw_values() + array->length()};
}

std::vector<std::string> ReadStringArrayFromDict(const std::shared_ptr<arrow::Array>& array)
{
    auto dictAr = std::dynamic_pointer_cast<arrow::DictionaryArray>(array);
    YT_VERIFY(dictAr);
    auto indices = ReadInteger32Array(dictAr->indices());

    // Get values array.
    auto values = ReadStringArray(dictAr->dictionary());

    std::vector<std::string> result;
    for (size_t i = 0; i < indices.size(); i++) {
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

std::vector<std::string> ReadAnyStringArray(const std::shared_ptr<arrow::Array>& array)
{
    if (std::dynamic_pointer_cast<arrow::BinaryArray>(array)) {
        return ReadStringArray(array);
    } else if (std::dynamic_pointer_cast<arrow::DictionaryArray>(array)) {
        return ReadStringArrayFromDict(array);
    }
    YT_ABORT();
}

bool IsDictColumn(const std::shared_ptr<arrow::Array>& array)
{
    return std::dynamic_pointer_cast<arrow::DictionaryArray>(array) != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

using ColumnInteger = std::vector<int64_t>;
using ColumnInteger32 = std::vector<int32_t>;
using ColumnString = std::vector<std::string>;
using ColumnNullableString = std::vector<std::optional<std::string>>;
using ColumnBool = std::vector<bool>;
using ColumnDouble = std::vector<double>;
using ColumnFloat = std::vector<float>;

using ColumnStringWithNulls = std::vector<std::optional<std::string>>;
using ColumnBoolWithNulls = std::vector<std::optional<bool>>;
using ColumnDoubleWithNulls = std::vector<std::optional<double>>;

struct TOwnerRows
{
    std::vector<TUnversionedRow> Rows;
    std::vector<TUnversionedOwningRowBuilder> Builders;
    TNameTablePtr NameTable;
    std::vector<TUnversionedOwningRow> OwningRows;
};

////////////////////////////////////////////////////////////////////////////////

TOwnerRows MakeUnversionedIntegerRows(
    const std::vector<ColumnInteger>& column,
    const std::vector<std::string>& columnNames,
    bool isSigned = true)
{
    YT_VERIFY(column.size() > 0);

    auto nameTable = New<TNameTable>();

    std::vector<TUnversionedOwningRowBuilder> rowsBuilders(column[0].size());

    for (int colIdx = 0; colIdx < std::ssize(column); colIdx++) {
        auto columnId = nameTable->RegisterName(columnNames[colIdx]);
        for (int rowIndex = 0; rowIndex < std::ssize(column[colIdx]); rowIndex++) {
            if (isSigned) {
                rowsBuilders[rowIndex].AddValue(MakeUnversionedInt64Value(column[colIdx][rowIndex], columnId));
            } else {
                rowsBuilders[rowIndex].AddValue(MakeUnversionedUint64Value(column[colIdx][rowIndex], columnId));
            }
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

TOwnerRows MakeUnversionedFloatRows(
    const std::vector<ColumnFloat>& column,
    const std::vector<std::string>& columnNames)
{
    YT_VERIFY(column.size() > 0);

    auto nameTable = New<TNameTable>();

    std::vector<TUnversionedOwningRowBuilder> rowsBuilders(column[0].size());

    for (int colIdx = 0; colIdx < std::ssize(column); colIdx++) {
        auto columnId = nameTable->RegisterName(columnNames[colIdx]);
        for (int rowIndex = 0; rowIndex < std::ssize(column[colIdx]); rowIndex++) {
            rowsBuilders[rowIndex].AddValue(MakeUnversionedDoubleValue(column[colIdx][rowIndex], columnId));
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

TOwnerRows MakeUnversionedStringRows(
    const std::vector<ColumnString>& column,
    const std::vector<std::string>& columnNames)
{
    YT_VERIFY(column.size() > 0);
    std::vector<TString> strings;

    auto nameTable = New<TNameTable>();

    std::vector<TUnversionedOwningRowBuilder> rowsBuilders(column[0].size());

    for (int colIdx = 0; colIdx < std::ssize(column); colIdx++) {
        auto columnId = nameTable->RegisterName(columnNames[colIdx]);
        for (int rowIndex = 0; rowIndex < std::ssize(column[colIdx]); rowIndex++) {
            strings.push_back(TString(column[colIdx][rowIndex]));
            rowsBuilders[rowIndex].AddValue(MakeUnversionedStringValue(strings.back(), columnId));
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

TOwnerRows MakeUnversionedNullableStringRows(
    const std::vector<ColumnNullableString>& column,
    const std::vector<std::string>& columnNames)
{
    YT_VERIFY(column.size() > 0);
    std::vector<TString> strings;

    auto nameTable = New<TNameTable>();

    std::vector<TUnversionedOwningRowBuilder> rowsBuilders(column[0].size());

    for (int colIdx = 0; colIdx < std::ssize(column); colIdx++) {
        auto columnId = nameTable->RegisterName(columnNames[colIdx]);
        for (int rowIndex = 0; rowIndex < std::ssize(column[colIdx]); rowIndex++) {
            if (column[colIdx][rowIndex] == std::nullopt) {
                rowsBuilders[rowIndex].AddValue(MakeUnversionedNullValue( columnId));
            } else {
                strings.push_back(TString(*column[colIdx][rowIndex]));
                rowsBuilders[rowIndex].AddValue(MakeUnversionedStringValue(strings.back(), columnId));
            }
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

std::string MakeRandomString(size_t stringSize)
{
    std::string randomString;
    randomString.reserve(stringSize);
    for (size_t i = 0; i < stringSize; i++) {
        randomString += ('a' + rand() % 30);
    }
    return randomString;
}

////////////////////////////////////////////////////////////////////////////////

void CheckColumnNames(
    std::shared_ptr<arrow::RecordBatch> batch,
    const std::vector<std::string>& columnNames)
{
    EXPECT_EQ(batch->num_columns(), std::ssize(columnNames));
    for (size_t i = 0; i < columnNames.size(); i++) {
        EXPECT_EQ(batch->column_name(i), columnNames[i]);
    }
}

bool CheckMaxConst(const char* ptr) {
    ui32 constMax = 0xFFFFFFFF;
    return *(reinterpret_cast<const uint32_t*>(ptr)) == constMax;
}

////////////////////////////////////////////////////////////////////////////////

TEST(Simple, JustWork)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"integer"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema(TString(columnNames[0]), EValueType::Int64),
    }));

    TStringStream outputStream;

    ColumnInteger column = {42, 179179};

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

TEST(Simple, YT_20699_WrongAlign)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"date"};

    // In such a scheme, the metadata will have a size not a multiple of 2^8
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(TString(columnNames[0]), ESimpleLogicalValueType::Date),
    }));

    TStringStream outputStream;
    i64 ma = std::numeric_limits<int>::max();

    ColumnInteger column = {18367, ma};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames, false);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    TString data(outputStream.Data(), outputStream.Size());
    auto ptr = outputStream.Data();
    auto restSize =  outputStream.Size();
    while (restSize > 0) {
        EXPECT_TRUE(restSize >= 4);
        EXPECT_TRUE(CheckMaxConst(ptr));
        ptr += 4;
        uint32_t fbSize = *(reinterpret_cast<const uint32_t*>(ptr));
        ptr += 4;
        auto message = org::apache::arrow::flatbuf::GetMessage(ptr);
        ptr += fbSize;
        ptr += message->bodyLength();
        restSize -= (8 + fbSize + message->bodyLength());
    }
}

TEST(Simple, Data)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"data"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(TString(columnNames[0]), ESimpleLogicalValueType::Date),
    }));

    TStringStream outputStream;
    i64 ma = std::numeric_limits<int>::max();

    ColumnInteger column = {18367, ma};

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

TEST(Simple, Datatime)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"datatime"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(TString(columnNames[0]), ESimpleLogicalValueType::Datetime),
    }));

    TStringStream outputStream;

    ColumnInteger column = {1586966302, 5};

    auto rows = MakeUnversionedIntegerRows({column}, columnNames, false);

    auto writer = CreateArrowWriter(rows.NameTable, &outputStream, tableSchemas);

    EXPECT_TRUE(writer->Write(rows.Rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    auto batch = MakeBatch(outputStream);
    CheckColumnNames(batch, columnNames);

    ColumnInteger columnRes = {1586966302000, 5000};
    EXPECT_EQ(ReadIntegerDate64Array(batch->column(0)), columnRes);
}

TEST(Simple, Timestamp)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"timestamp"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(TString(columnNames[0]), ESimpleLogicalValueType::Timestamp),
    }));

    TStringStream outputStream;

    ColumnInteger column = {1586966302504185, 5000};

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

TEST(Simple, Interval)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"Interval"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(TString(columnNames[0]), ESimpleLogicalValueType::Interval),
    }));

    TStringStream outputStream;

    ColumnInteger column = {1586966302504185, 5000};

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

TEST(Simple, Float)
{
    EXPECT_TRUE(true);
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"float"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema(TString(columnNames[0]), ESimpleLogicalValueType::Float),
    }));

    TStringStream outputStream;

    ColumnFloat column = {1.2, 3.14};

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

TEST(Simple, ColumnarBatch)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"integer"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema(TString(columnNames[0]), EValueType::Int64),
    }));

    TStringStream outputStream;

    ColumnInteger column = {42, 179179};

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

TEST(Simple, RowBatch)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"integer"};

    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema(TString(columnNames[0]), EValueType::Int64),
    }));

    TStringStream outputStream;

    ColumnInteger column = {42, 179179};

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

TEST(Simple, Null)
{
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<std::string> columnNames = {"integer", "null"};
    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema(TString(columnNames[0]), EValueType::Int64),
                TColumnSchema(TString(columnNames[1]), EValueType::Null),
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

TEST(Simple, MultiTable)
{
    // First table
    std::vector<std::string> columnNames1 = {"string"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(TString(columnNames1[0]), EValueType::String),
    }));

    // Second table
    std::vector<std::string> columnNames2 = {"int"};
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(TString(columnNames2[0]), EValueType::Int64),
    }));

    // Third table
    std::vector<std::string> columnNames3 = {"int2", "string2"};
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(TString(columnNames3[0]), EValueType::Int64),
        TColumnSchema(TString(columnNames3[1]), EValueType::String),
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

    size_t firstBatchSize = 0;

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

    EXPECT_EQ(ReadStringArray(batches[0]->column(0)), stringColumn1);

    // Check second batch

    EXPECT_EQ(ReadInteger64Array(batches[1]->column(0)), intColumn1);

    // Check third batch

    EXPECT_EQ(ReadInteger64Array(batches[2]->column(1)), intColumn2);
    EXPECT_EQ(ReadStringArray(batches[2]->column(0)), stringColumn2);

    // Check fourth batch

    EXPECT_EQ(ReadStringArray(batches[3]->column(0)), stringColumn3);

    // Check fifth batch

    EXPECT_EQ(ReadInteger64Array(batches[4]->column(0)), intColumn3);

}

TEST(Simple, String)
{
    std::vector<std::string> columnNames = {"string"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema(TString(columnNames[0]), EValueType::String),
    }));

    TStringStream outputStream;

    ColumnString column = {"cat", "mouse"};

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

TEST(Simple, DictionaryString)
{
    std::vector<std::string> columnNames = {"string"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema(TString(columnNames[0]), EValueType::String),
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

TEST(Simple, EnumString)
{
    const size_t batchNumb = 10;
    const size_t rowsCount = 10;

    std::vector<std::string> columnNames = {"string", "string2", "string3"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
        TColumnSchema(TString(columnNames[0]), EValueType::String),
        TColumnSchema(TString(columnNames[1]), EValueType::String),
        TColumnSchema(TString(columnNames[2]), EValueType::String),
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

    for (size_t i = 0; i < batchNumb; i++) {
        std::vector<std::optional<std::string>> column;
        std::vector<std::optional<std::string>> constColumn;
        std::vector<std::optional<std::string>> optColumn;

        for (size_t j = 0; j < rowsCount; j++) {
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

    auto batches = MakeAllBatch(outputStream, batchNumb);

    size_t batchIndex = 0;
    for (auto& batch : batches) {
        CheckColumnNames(batch, columnNames);

        auto column = ReadAnyStringArray(batch->column(0));
        auto constColumn = ReadAnyStringArray(batch->column(1));
        auto optColumn = ReadAnyStringArray(batch->column(2));

        for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
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

TEST(Simple, DictionaryAndDirectStrings)
{
    std::vector<std::string> columnNames = {"string"};
    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema(TString(columnNames[0]), EValueType::String),
    }));

    TStringStream outputStream;

    std::string longString, longString2;
    for (int i = 0; i < 20; i++) {
        longString += 'a';
        longString2 += 'b';
    }
    ColumnString firstColumn = {longString, longString2, longString, longString2};
    ColumnString secondColumn = {"cat", "dog", "mouse", "table"};

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

TEST(StressOneBatch, Integer)
{
    // Constans.
    const size_t columnsCount = 100;
    const size_t rowsCount = 100;

    std::vector<TTableSchemaPtr> tableSchemas;
    TStringStream outputStream;

    std::vector<std::string> columnNames;
    std::vector<ColumnInteger> columnsElements(columnsCount);

    for (size_t columnIndex = 0; columnIndex < columnsCount; columnIndex++) {
        // Create column name.
        std::string ColumnName = "integer" + std::to_string(columnIndex);
        columnNames.push_back(ColumnName);

        for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
            columnsElements[columnIndex].push_back(rand());
        }
    }

    std::vector<TColumnSchema> schemas_;
    for (size_t columnIdx = 0; columnIdx < columnsCount; columnIdx++) {
        schemas_.push_back(TColumnSchema(TString(columnNames[columnIdx]), EValueType::Int64));
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

    for (size_t columnIndex = 0; columnIndex < columnsCount; columnIndex++) {
        EXPECT_EQ(ReadInteger64Array(batch->column(columnIndex)), columnsElements[columnIndex]);
    }
}

TEST(StressOneBatch, String)
{
    const size_t columnsCount = 10;
    const size_t rowsCount = 10;
    const size_t stringSize = 10;

    std::vector<TTableSchemaPtr> tableSchemas;

    TStringStream outputStream;

    std::vector<std::string> columnNames;
    std::vector<ColumnString> columnsElements(columnsCount);

    for (size_t columnIndex = 0; columnIndex < columnsCount; columnIndex++) {

        std::string ColumnName = "string" + std::to_string(columnIndex);
        columnNames.push_back(ColumnName);
        for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
            columnsElements[columnIndex].push_back(MakeRandomString(stringSize));
        }
    }

    std::vector<TColumnSchema> schemas_;
    for (size_t columnIdx = 0; columnIdx < columnsCount; columnIdx++) {
        schemas_.push_back(TColumnSchema(TString(columnNames[columnIdx]), EValueType::String));
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

    for (size_t columnIndex = 0; columnIndex < columnsCount; columnIndex++) {
        EXPECT_EQ(ReadAnyStringArray(batch->column(columnIndex)), columnsElements[columnIndex]);
    }
}

TEST(StressOneBatch, MixTypes)
{
    // Constants.
    const size_t rowsCount = 10;
    const size_t stringSize = 10;

    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema("bool", EValueType::Boolean),
                TColumnSchema("double", EValueType::Double),
                TColumnSchema("any", EValueType::Any)}));

    TStringStream outputStream;

    auto nameTable = New<TNameTable>();
    std::vector<TUnversionedOwningRowBuilder> rowsBuilders(rowsCount);

    std::vector<std::string> columnNames;

    std::vector<bool> boolColumn;
    std::vector<double> doubleColumn;
    std::vector<std::string> anyColumn;
    std::vector<TUnversionedRow> rows;

    // Fill bool column.
    std::string ColumnName = "bool";
    auto boolId = nameTable->RegisterName(ColumnName);
    columnNames.push_back(ColumnName);
    for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
        boolColumn.push_back((rand() % 2) == 0);

        rowsBuilders[rowIndex].AddValue(MakeUnversionedBooleanValue(boolColumn[rowIndex], boolId));
    }

    // Fill double column.
    ColumnName = "double";
    auto columnId = nameTable->RegisterName(ColumnName);
    columnNames.push_back(ColumnName);
    for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
        doubleColumn.push_back((double)(rand() % 100) / 10.0);
        rowsBuilders[rowIndex].AddValue(MakeUnversionedDoubleValue(doubleColumn[rowIndex], columnId));
    }

    // Fill any column.
    ColumnName = "any";
    auto anyId = nameTable->RegisterName(ColumnName);
    columnNames.push_back(ColumnName);
    for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
        std::string randomString = MakeRandomString(stringSize);

        anyColumn.push_back(randomString);

        rowsBuilders[rowIndex].AddValue(MakeUnversionedAnyValue(randomString, anyId));
    }

    std::vector<TUnversionedOwningRow> owningRows;
    for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
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

    EXPECT_EQ(ReadBoolArray(batch->column(0)), boolColumn);
    EXPECT_EQ(ReadDoubleArray(batch->column(1)), doubleColumn);
    EXPECT_EQ(ReadAnyStringArray(batch->column(2)), anyColumn);
}

TEST(StressMultiBatch, Integer)
{
    // Constants.
    const size_t columnsCount = 10;
    const size_t rowsCount = 10;
    const size_t numbOfBatch = 10;

    std::vector<std::string> columnNames;
    std::vector<TTableSchemaPtr> tableSchemas;
    std::vector<TColumnSchema> schemas_;

    for (size_t columnIdx = 0; columnIdx < columnsCount; columnIdx++) {
        std::string ColumnName = "integer" + std::to_string(columnIdx);
        columnNames.push_back(ColumnName);
        schemas_.push_back(TColumnSchema(TString(columnNames[columnIdx]), EValueType::Int64));
    }
    tableSchemas.push_back(New<TTableSchema>(schemas_));

    TStringStream outputStream;
    std::vector<std::vector<ColumnInteger>> columnsElements(numbOfBatch, std::vector<ColumnInteger>(columnsCount));

    auto nameTable = New<TNameTable>();
    for (size_t columnIndex = 0; columnIndex < columnsCount; columnIndex++) {
        std::string ColumnName = "integer" + std::to_string(columnIndex);
        nameTable->RegisterName(ColumnName);
    }
    auto writer = CreateArrowWriter(nameTable, &outputStream, tableSchemas);


    for (size_t batchIndex = 0; batchIndex < numbOfBatch; batchIndex++) {

        for (size_t columnIndex = 0; columnIndex < columnsCount; columnIndex++) {
            std::string ColumnName = "integer" + std::to_string(columnIndex);
            for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
                columnsElements[batchIndex][columnIndex].push_back(rand());
            }
        }

        auto rows = MakeUnversionedIntegerRows(columnsElements[batchIndex], columnNames);
        EXPECT_TRUE(writer->Write(rows.Rows));
    }

    writer->Close()
        .Get()
        .ThrowOnError();


    auto batches = MakeAllBatch(outputStream, numbOfBatch);

    size_t batchIndex = 0;
    for (auto& batch : batches) {
        for (size_t columnIndex = 0; columnIndex < columnsCount; columnIndex++) {
            CheckColumnNames(batch, columnNames);
            EXPECT_EQ(ReadInteger64Array(batch->column(columnIndex)), columnsElements[batchIndex][columnIndex]);
        }
        batchIndex++;
    }
}

TEST(StressMultiBatch, MixTypes)
{
    // Сonstants.
    const size_t rowsCount = 10;
    const size_t numbOfBatch = 10;
    const size_t stringSize = 10;

    std::vector<TTableSchemaPtr> tableSchemas;
    tableSchemas.push_back(New<TTableSchema>(std::vector{
                TColumnSchema("bool", EValueType::Boolean),
                TColumnSchema("double", EValueType::Double),
                TColumnSchema("any", EValueType::Any)}));

    TStringStream outputStream;

    auto nameTable = New<TNameTable>();

    std::vector<std::string> columnNames = {"bool", "double", "any"};
    auto boolId = nameTable->RegisterName(columnNames[0]);
    auto doubleId = nameTable->RegisterName(columnNames[1]);
    auto anyId = nameTable->RegisterName(columnNames[2]);

    std::vector<ColumnBoolWithNulls> boolColumns(numbOfBatch);
    std::vector<ColumnDoubleWithNulls> doubleColumns(numbOfBatch);
    std::vector<ColumnStringWithNulls> anyColumns(numbOfBatch);

    auto writer = CreateArrowWriter(nameTable, &outputStream, tableSchemas);

    std::vector<TUnversionedOwningRow> owningRows;

    for (size_t batchIndex = 0; batchIndex < numbOfBatch; batchIndex++) {
        std::vector<TUnversionedOwningRowBuilder> rowsBuilders(rowsCount);
        std::vector<TUnversionedRow> rows;

        for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
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

    auto batches = MakeAllBatch(outputStream, numbOfBatch);
    size_t batchIndex = 0;
    for (auto& batch : batches) {
        CheckColumnNames(batch, columnNames);

        auto boolAr = ReadBoolArray(batch->column(0));
        auto doubleAr = ReadDoubleArray(batch->column(1));
        auto anyAr = ReadAnyStringArray(batch->column(2));

        for (size_t rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
            if (boolColumns[batchIndex][rowIndex] == std::nullopt) {
                EXPECT_TRUE(batch->column(0)->IsNull(rowIndex));
                EXPECT_TRUE(batch->column(1)->IsNull(rowIndex));
                EXPECT_TRUE(batch->column(2)->IsNull(rowIndex));
            } else {
                EXPECT_EQ(boolAr[rowIndex], *boolColumns[batchIndex][rowIndex]);
                EXPECT_EQ(doubleAr[rowIndex], *doubleColumns[batchIndex][rowIndex]);
                EXPECT_EQ(anyAr[rowIndex], *anyColumns[batchIndex][rowIndex]);
            }
        }

        batchIndex++;
    }
}

} // namespace
} // namespace NYT::NTableClient
