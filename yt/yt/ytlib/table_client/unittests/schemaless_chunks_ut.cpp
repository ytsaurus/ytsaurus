#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/memory_reader.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NTableClient {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TChunkReaderStatistics;
using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

const int RowCount = 50000;
const TStringBuf StringValue = "She sells sea shells on a sea shore";
const TStringBuf AnyValueList = "[one; two; three]";
const TStringBuf AnyValueMap = "{a=b; c=d}";
const std::vector<TString> ColumnNames = {"c0", "c1", "c2", "c3", "c4", "c5", "c6"};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunksTest
    : public ::testing::TestWithParam<std::tuple<EOptimizeFor, TTableSchema, TColumnFilter, TLegacyReadRange>>
{
protected:
    IChunkReaderPtr MemoryReader_;
    TNameTablePtr WriteNameTable_;
    TChunkSpec ChunkSpec_;
    TColumnarChunkMetaPtr ChunkMeta_;
    TChunkedMemoryPool Pool_;
    std::vector<TUnversionedRow> Rows_;

    static TUnversionedValue CreateC0(int rowIndex, TNameTablePtr nameTable)
    {
        // Key part 0, Any.
        int id = nameTable->GetId("c0");
        switch (rowIndex / 100000) {
            case 0:
                return MakeUnversionedSentinelValue(EValueType::Null, id);
            case 1:
                return MakeUnversionedInt64Value(-65537, id);
            case 2:
                return MakeUnversionedUint64Value(65537, id);
            case 3:
                return MakeUnversionedDoubleValue(rowIndex, id);
            case 4:
                return MakeUnversionedBooleanValue(true, id);
            case 5:
                return MakeUnversionedStringValue(StringValue, id);
            default:
                YT_ABORT();
        }
    }

    static TUnversionedValue CreateC1(int rowIndex, TNameTablePtr nameTable)
    {
        //  Key part 1, Int64.
        int id = nameTable->GetId("c1");
        const int divider = 10000;
        auto value = rowIndex % (10 * divider);
        if (value < divider) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedInt64Value(value / divider - 5, id);
        }
    }

    static TUnversionedValue CreateC2(int rowIndex, TNameTablePtr nameTable)
    {
        //  Key part 2, Uint64.
        int id = nameTable->GetId("c2");
        const int divider = 1000;
        auto value = rowIndex % (10 * divider);
        if (value < divider) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedUint64Value(value / divider, id);
        }
    }

    static TUnversionedValue CreateC3(int rowIndex, TNameTablePtr nameTable)
    {
        // Key part 3, String.
        int id = nameTable->GetId("c3");
        const int divider = 100;
        auto value = rowIndex % (10 * divider);
        if (value < divider) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedStringValue(StringValue, id);
        }
    }

    static TUnversionedValue CreateC4(int rowIndex, TNameTablePtr nameTable)
    {
        // Key part 4, Boolean.
        int id = nameTable->GetId("c4");
        const int divider = 10;
        auto value = rowIndex % (10 * divider);
        if (value < divider) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedBooleanValue(value > divider * 5, id);
        }
    }

    static TUnversionedValue CreateC5(int rowIndex, TNameTablePtr nameTable)
    {
        // Key part 5, Double.
        int id = nameTable->GetId("c5");
        const int divider = 1;
        auto value = rowIndex % (10 * divider);
        if (value < divider) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedDoubleValue(value, id);
        }
    }

    static TUnversionedValue CreateC6(int rowIndex, TNameTablePtr nameTable)
    {
        // Not key, Any.
        int id = nameTable->GetId("c6");
        switch (rowIndex % 8) {
            case 0:
                return MakeUnversionedSentinelValue(EValueType::Null, id);
            case 1:
                return MakeUnversionedInt64Value(-65537, id);
            case 2:
                return MakeUnversionedUint64Value(65537, id);
            case 3:
                return MakeUnversionedDoubleValue(rowIndex, id);
            case 4:
                return MakeUnversionedBooleanValue(true, id);
            case 5:
                return MakeUnversionedStringValue(StringValue, id);
            case 6:
                return MakeUnversionedAnyValue(AnyValueList, id);
            case 7:
                return MakeUnversionedAnyValue(AnyValueMap, id);
            default:
                YT_ABORT();
        }
    }

    TUnversionedRow CreateRow(int rowIndex, TNameTablePtr nameTable)
    {
        auto row = TMutableUnversionedRow::Allocate(&Pool_, 7);
        row[0] = CreateC0(2 * rowIndex, nameTable);
        row[1] = CreateC1(2 * rowIndex, nameTable);
        row[2] = CreateC2(2 * rowIndex, nameTable);
        row[3] = CreateC3(2 * rowIndex, nameTable);
        row[4] = CreateC4(2 * rowIndex, nameTable);
        row[5] = CreateC5(2 * rowIndex, nameTable);
        row[6] = CreateC6(2 * rowIndex, nameTable);
        return row;
    }

    std::vector<TUnversionedRow> CreateRows(TNameTablePtr nameTable)
    {
        std::vector<TUnversionedRow> rows;
        for (int rowIndex = 0; rowIndex < RowCount; ++rowIndex) {
            rows.push_back(CreateRow(rowIndex, nameTable));
        }
        return rows;
    }

    void SetUp() override
    {
        auto nameTable = New<TNameTable>();
        InitNameTable(nameTable);
        Rows_ = CreateRows(nameTable);

        auto memoryWriter = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 256;
        config->Postprocess();

        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = std::get<0>(GetParam());
        options->Postprocess();

        auto chunkWriter = CreateSchemalessChunkWriter(
            config,
            options,
            New<TTableSchema>(std::get<1>(GetParam())),
            /*nameTable*/ nullptr,
            memoryWriter,
            /*dataSink*/ std::nullopt);

        WriteNameTable_ = chunkWriter->GetNameTable();
        InitNameTable(WriteNameTable_);

        chunkWriter->Write(Rows_);
        EXPECT_TRUE(chunkWriter->Close().Get().IsOK());

        MemoryReader_ = CreateMemoryReader(
            memoryWriter->GetChunkMeta(),
            memoryWriter->GetBlocks());

        ToProto(ChunkSpec_.mutable_chunk_id(), NullChunkId);
        ChunkSpec_.set_table_row_index(42);

        ChunkMeta_ = New<TColumnarChunkMeta>(*memoryWriter->GetChunkMeta());
    }

    static void InitNameTable(TNameTablePtr nameTable, int idShift = 0)
    {
        for (int id = 0; id < std::ssize(ColumnNames); ++id) {
            EXPECT_EQ(id, nameTable->GetIdOrRegisterName(ColumnNames[(id + idShift) % ColumnNames.size()]));
        }
    }

    ISchemalessChunkReaderPtr CreateReader(
        TNameTablePtr readNameTable,
        const TReadLimit& lowerReadLimit,
        const TReadLimit& upperReadLimit)
    {
        auto schema = std::get<1>(GetParam());
        auto columnFilter = std::get<2>(GetParam());

        auto chunkState = New<TChunkState>(TChunkState{
            .BlockCache = GetNullBlockCache(),
            .ChunkSpec = ChunkSpec_,
            .TableSchema = New<TTableSchema>(std::get<1>(GetParam())),
        });

        return CreateSchemalessRangeChunkReader(
            std::move(chunkState),
            ChunkMeta_,
            TChunkReaderConfig::GetDefault(),
            TChunkReaderOptions::GetDefault(),
            MemoryReader_,
            readNameTable,
            /*chunkReadOptions*/ {},
            /*sortColumns*/ schema.GetSortColumns(),
            /*omittedInaccessibleColumns*/ {},
            columnFilter,
            TReadRange(lowerReadLimit, upperReadLimit));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TSchemalessChunksTest, WithoutSampling)
{
    auto schema = std::get<1>(GetParam());

    int keyColumnCount = schema.GetKeyColumnCount();

    auto readNameTable = New<TNameTable>();
    InitNameTable(readNameTable, 4);

    auto columnFilter = std::get<2>(GetParam());
    auto expected = CreateFilteredRangedRows(
        Rows_,
        WriteNameTable_,
        readNameTable,
        columnFilter,
        std::get<3>(GetParam()),
        &Pool_,
        keyColumnCount);

    auto legacyReadRange = std::get<3>(GetParam());

    auto lowerReadLimit = ReadLimitFromLegacyReadLimit(legacyReadRange.LowerLimit(), /*isUpper*/ false, keyColumnCount);
    auto upperReadLimit = ReadLimitFromLegacyReadLimit(legacyReadRange.UpperLimit(), /*isUpper*/ true, keyColumnCount);

    auto chunkReader = CreateReader(readNameTable, lowerReadLimit, upperReadLimit);

    CheckSchemalessResult(expected, chunkReader, 0);
}

INSTANTIATE_TEST_SUITE_P(Unsorted,
    TSchemalessChunksTest,
    ::testing::Combine(
        ::testing::Values(
            EOptimizeFor::Scan,
            EOptimizeFor::Lookup),
        ::testing::Values(
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%false>[]"))),
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%false>[{name = c0; type = any}; {name = c1; type = int64}; {name = c2; type = uint64}; ]"))),
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true>[{name = c0; type = any}; {name = c1; type = int64}; "
                "{name = c2; type = uint64}; {name = c3; type = string}; {name = c4; type = boolean}; {name = c5; type = double}; "
                "{name = c6; type = any};]"))),
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%false>[{name = c0; type = any}; {name = c1; type = int64}; "
                "{name = c2; type = uint64}; {name = c3; type = string}; {name = c4; type = boolean}; {name = c5; type = double}; {name = c6; type = any};]")))),
        ::testing::Values(TColumnFilter(), TColumnFilter({2, 4})),
        ::testing::Values(
            TLegacyReadRange(),
            TLegacyReadRange(TLegacyReadLimit().SetRowIndex(RowCount / 3), TLegacyReadLimit().SetRowIndex(RowCount / 3)),
            TLegacyReadRange(TLegacyReadLimit().SetRowIndex(RowCount / 3), TLegacyReadLimit().SetRowIndex(2 * RowCount / 3)))));


INSTANTIATE_TEST_SUITE_P(Sorted,
    TSchemalessChunksTest,
    ::testing::Combine(
        ::testing::Values(
            EOptimizeFor::Scan,
            EOptimizeFor::Lookup),
        ::testing::Values(
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%false>["
                "{name = c0; type = any; sort_order = ascending};"
                "{name = c1; type = any; sort_order = ascending};"
                "{name = c2; type = any; sort_order = ascending}]"))),
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true>["
                "{name = c0; type = any; sort_order = ascending};"
                "{name = c1; type = int64; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = string; sort_order = ascending};"
                "{name = c4; type = boolean; sort_order = ascending};"
                "{name = c5; type = double; sort_order = ascending};"
                "{name = c6; type = any};]")))),
        ::testing::Values(TColumnFilter(), TColumnFilter({0, 5})),
        ::testing::Values(
            TLegacyReadRange(),
            TLegacyReadRange(TLegacyReadLimit().SetLegacyKey(YsonToKey("<type=null>#")), TLegacyReadLimit().SetLegacyKey(YsonToKey("<type=null>#"))),
            TLegacyReadRange(TLegacyReadLimit().SetLegacyKey(YsonToKey("-65537; -1; 1u; <type=null>#")), TLegacyReadLimit()),
            TLegacyReadRange(TLegacyReadLimit().SetLegacyKey(YsonToKey("-65537; -1; 1u; <type=null>#")), TLegacyReadLimit().SetLegacyKey(YsonToKey("350000.1; 1; 1; \"Z\""))))));

// ToDo(psushin):
//  1. Test sampling.
//  2. Test system columns.

////////////////////////////////////////////////////////////////////////////////

class TColumnarReadTest
    : public ::testing::Test
{
protected:
    IChunkReaderPtr MemoryReader_;
    TChunkSpec ChunkSpec_;
    TColumnarChunkMetaPtr ChunkMeta_;
    TChunkStatePtr ChunkState_;
    TSharedRange<TUnversionedRow> Rows_;

    static constexpr int N = 100003;

    const TTableSchemaPtr Schema_ = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        "<strict=%true>["
            "{name = c1; type = int64; sort_order = ascending};"
            "{name = c2; type = uint64};"
            "{name = c3; type = string};"
            "{name = c4; type = boolean};"
            "{name = c5; type = double};"
            "{name = c6; type = any};"
        "]")));

    void SetUp() override
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

        for (int i = 0; i < N; ++i) {
            builder.AddRow(
                i % 10,
                i / 10,
                Format("c3_%v", i),
                i % 7 == 0,
                i * i / 2.0,
                TYsonString(Format("{key=%v;value=%v}", i, i + 10)));
        }

        Rows_ = builder.Build();

        chunkWriter->Write(Rows_);
        EXPECT_TRUE(chunkWriter->Close().Get().IsOK());

        MemoryReader_ = CreateMemoryReader(
            memoryWriter->GetChunkMeta(),
            memoryWriter->GetBlocks());

        ToProto(ChunkSpec_.mutable_chunk_id(), NullChunkId);
        ChunkSpec_.set_table_row_index(42);

        ChunkMeta_ = New<TColumnarChunkMeta>(*memoryWriter->GetChunkMeta());

        ChunkState_ = New<TChunkState>(TChunkState{
            .BlockCache = GetNullBlockCache(),
            .ChunkSpec = ChunkSpec_,
            .TableSchema = Schema_,
        });
    }

    virtual ISchemalessUnversionedReaderPtr CreateReader(const TColumnFilter& columnFilter)
    {
        return CreateSchemalessRangeChunkReader(
            ChunkState_,
            ChunkMeta_,
            TChunkReaderConfig::GetDefault(),
            TChunkReaderOptions::GetDefault(),
            MemoryReader_,
            TNameTable::FromSchema(*Schema_),
            /*chunkReadOptions*/ {},
            /*sortColumns*/ {},
            /*omittedInaccessibleColumns*/ {},
            columnFilter,
            TReadRange());
    }
};

TEST_F(TColumnarReadTest, UnreadBatch)
{
    auto reader = CreateReader(TColumnFilter{0});
    TRowBatchReadOptions options{
        .MaxRowsPerRead = 10,
        .Columnar = true
    };
    while (auto batch = ReadRowBatch(reader, options)) {
    }
    auto statistics = reader->GetDataStatistics();
    EXPECT_EQ(N, statistics.row_count());
    EXPECT_EQ(N * 9, statistics.data_weight());
}

TEST_F(TColumnarReadTest, ReadJustC1)
{
    auto reader = CreateReader(TColumnFilter{0});
    TRowBatchReadOptions options{
        .MaxRowsPerRead = 10,
        .Columnar = true
    };
    while (auto batch = ReadRowBatch(reader, options)) {
        auto columnarBatch = batch->TryAsColumnar();
        ASSERT_TRUE(columnarBatch.operator bool());
        auto columns = columnarBatch->MaterializeColumns();
        EXPECT_EQ(1u, columns.size());
        EXPECT_EQ(0, columns[0]->Id);
    }
    auto statistics = reader->GetDataStatistics();
    EXPECT_EQ(N, statistics.row_count());
    EXPECT_EQ(N * 9, statistics.data_weight());
}

TEST_F(TColumnarReadTest, ReadAll)
{
    auto reader = CreateReader(TColumnFilter());
    TRowBatchReadOptions options{
        .MaxRowsPerRead = 10,
        .Columnar = true
    };
    while (auto batch = ReadRowBatch(reader, options)) {
        auto columnarBatch = batch->TryAsColumnar();
        ASSERT_TRUE(columnarBatch.operator bool());
        auto columns = columnarBatch->MaterializeColumns();
        EXPECT_EQ(Schema_->GetColumnCount(), std::ssize(columns));
        for (int index = 0; index < std::ssize(columns); ++index) {
            EXPECT_EQ(index, columns[index]->Id);
        }
    }
    auto statistics = reader->GetDataStatistics();
    EXPECT_EQ(N, statistics.row_count());
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessSortedChunksTest
    : public ::testing::TestWithParam<std::tuple<EOptimizeFor, int /*BlockSize*/, TTableSchema, int /*MaxDataWeightPerRead*/, int /*MaxRowsPerRead*/>>
{
protected:
    void SetUp() override
    {
        auto optimizeFor = std::get<0>(GetParam());
        int blockSize = std::get<1>(GetParam());
        Schema_ = New<TTableSchema>(std::get<2>(GetParam()));
        InitChunkWriter(optimizeFor, Schema_, blockSize);
        InitRows(1000, Schema_, WriteNameTable_);
        InitChunk();
    }

    TRowBatchReadOptions ReadOptions()
    {
        TRowBatchReadOptions options;
        options.MaxDataWeightPerRead = std::get<3>(GetParam());
        options.MaxRowsPerRead = std::get<4>(GetParam());
        return options;
    }

    TUnversionedValue CreateInt64(int rowIndex, int id)
    {
        if (rowIndex % 20 == 0) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedInt64Value((rowIndex << 10) ^ rowIndex, id);
        }
    }

    TUnversionedValue CreateUint64(int rowIndex, int id)
    {
        if (rowIndex % 20 == 0) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedUint64Value((rowIndex << 10) ^ rowIndex, id);
        }
    }

    TUnversionedValue CreateDouble(int rowIndex, int id)
    {
        if (rowIndex % 20 == 0) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedDoubleValue(static_cast<double>((rowIndex << 10) ^ rowIndex), id);
        }
    }

    TUnversionedValue CreateBoolean(int rowIndex, int id)
    {
        if (rowIndex % 20 == 0) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedBooleanValue(static_cast<bool>(rowIndex % 3), id);
        }
    }

    TUnversionedValue CreateString(int rowIndex, int id)
    {
        if (rowIndex % 20 == 0) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else {
            return MakeUnversionedStringValue(StringValue, id);
        }
    }

    TUnversionedValue CreateAny(int rowIndex, int id)
    {
        if (rowIndex % 20 == 0) {
            return MakeUnversionedSentinelValue(EValueType::Null, id);
        } else if (rowIndex % 20 < 11) {
            return MakeUnversionedAnyValue(AnyValueList, id);
        } else {
            return MakeUnversionedAnyValue(AnyValueMap, id);
        }
    }

    TUnversionedValue CreateValue(int index, int id, const TColumnSchema& columnSchema)
    {
        switch (columnSchema.GetWireType()) {
            case EValueType::Int64:
                return CreateInt64(index, id);
            case EValueType::Uint64:
                return CreateUint64(index, id);
            case EValueType::Double:
                return CreateDouble(index, id);
            case EValueType::Boolean:
                return CreateBoolean(index, id);
            case EValueType::String:
                return CreateString(index, id);
            case EValueType::Any:
                return CreateAny(index, id);
            case EValueType::Null:
            case EValueType::Composite:
            case EValueType::TheBottom:
            case EValueType::Min:
            case EValueType::Max:
                break;
        }
        YT_ABORT();
    }

    TUnversionedRow CreateRowAt(int position, TTableSchemaPtr schema, TNameTablePtr nameTable)
    {
        auto row = TMutableUnversionedRow::Allocate(&Pool_, schema->GetColumnCount());
        for (int index = 0; index < std::ssize(schema->Columns()); ++index) {
            const auto& column = schema->Columns()[index];
            row[index] = CreateValue(position, nameTable->GetIdOrRegisterName(column.Name()), column);
        }
        return row;
    }

    TUnversionedRow CreateRow(int rowIndex, TTableSchemaPtr schema, TNameTablePtr nameTable)
    {
        return CreateRowAt(2 * rowIndex, schema, nameTable);
    }

    TUnversionedRow CreateMissingRow(int rowIndex, TTableSchemaPtr schema, TNameTablePtr nameTable)
    {
        return CreateRowAt(2 * rowIndex + 1, schema, nameTable);
    }

    TUnversionedRow CreateSentinelRow(TUnversionedRow lastRow, TTableSchemaPtr schema, TNameTablePtr nameTable)
    {
        TMutableUnversionedRow result = TMutableUnversionedRow::Allocate(&Pool_, lastRow.GetCount());
        for (ui64 i = 0; i < lastRow.GetCount(); ++i) {
            const auto& column = schema->Columns()[i];
            int id = nameTable->GetIdOrRegisterName(column.Name());
            const auto& value = lastRow[i];
            switch (value.Type) {
                case EValueType::Int64: {
                    YT_ASSERT(value.Data.Int64 < std::numeric_limits<i64>::max());
                    result[i] = MakeUnversionedInt64Value(value.Data.Int64 + 1, id);
                    break;
                }
                case EValueType::Uint64:
                    YT_ASSERT(value.Data.Uint64 < std::numeric_limits<ui64>::max());
                    result[i] = MakeUnversionedUint64Value(value.Data.Uint64 + 1, id);
                    break;
                case EValueType::Double:
                    YT_ASSERT(isfinite(value.Data.Double));
                    result[i] = MakeUnversionedDoubleValue(2*abs(value.Data.Double), id);
                    break;
                case EValueType::Boolean:
                case EValueType::String:
                case EValueType::Null:
                case EValueType::Composite:
                case EValueType::TheBottom:
                case EValueType::Min:
                case EValueType::Max:
                case EValueType::Any:
                    result[i] = RowBuffer_->CaptureValue(value);
                    break;
                default:
                    YT_ABORT();
            }
        }
        return result;
    }

    void InitRows(int rowCount, TTableSchemaPtr schema, TNameTablePtr nameTable)
    {
        RowBuffer_ = New<TRowBuffer>();
        std::vector<TUnversionedRow> rows;

        for (int index = 0; index < rowCount; ++index) {
            rows.push_back(CreateRow(index, schema, nameTable));
        }

        std::sort(
            rows.begin(),
            rows.end(),
            [&] (TUnversionedRow lhs, TUnversionedRow rhs) {
                return CompareRows(lhs, rhs, schema->GetKeyColumnCount()) < 0;
            });
        rows.erase(
            std::unique(
                rows.begin(),
                rows.end(),
                [&] (TUnversionedRow lhs, TUnversionedRow& rhs) {
                    return CompareRows(lhs, rhs, schema->GetKeyColumnCount()) == 0;
                }),
            rows.end());

        Rows_ = std::move(rows);

        sentinelRow_ = CreateSentinelRow(Rows_.back(), schema, nameTable);
        YT_ASSERT(CompareRows(Rows_.back(), sentinelRow_, schema->GetKeyColumnCount()) < 0);
    }

    void InitChunkWriter(EOptimizeFor optimizeFor, TTableSchemaPtr schema, int blockSize)
    {
        for (const auto& sortColumn : Schema_->GetSortColumns()) {
            SortOrders_.push_back(sortColumn.SortOrder);
        }

        MemoryWriter_ = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = blockSize;
        config->Postprocess();

        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = optimizeFor;
        options->ValidateSorted = schema->IsSorted();
        options->ValidateUniqueKeys = schema->IsUniqueKeys();
        options->Postprocess();

        ChunkWriter_ = CreateSchemalessChunkWriter(
            config,
            options,
            schema,
            /*nameTable*/ nullptr,
            MemoryWriter_);

        WriteNameTable_ = ChunkWriter_->GetNameTable();
    }

    void InitChunk()
    {

        ChunkWriter_->Write(Rows_);
        EXPECT_TRUE(ChunkWriter_->Close().Get().IsOK());

        MemoryReader_ = CreateMemoryReader(
            MemoryWriter_->GetChunkMeta(),
            MemoryWriter_->GetBlocks());

        ToProto(ChunkSpec_.mutable_chunk_id(), NullChunkId);
        ChunkSpec_.set_table_row_index(42);
    }


    TUnversionedRow ExtractKeyPrefix(const TUnversionedRow& row, int prefixLength)
    {
        auto key = TMutableUnversionedRow::Allocate(&Pool_, prefixLength);
        for (int valueIndex = 0; valueIndex < prefixLength; ++valueIndex) {
            key[valueIndex] = row[valueIndex];
        }
        return key;
    }

    TUnversionedRow ExtractKey(TUnversionedRow row)
    {
        return ExtractKeyPrefix(row, Schema_->GetKeyColumnCount());
    }

    TReadLimit CreateReadLimit(TUnversionedRow row, bool upper)
    {
        auto bound = TOwningKeyBound::FromRow(TUnversionedOwningRow(row), !upper, upper);
        return TReadLimit(bound);
    }

    TCachedVersionedChunkMetaPtr FetchMeta()
    {
        auto asyncCachedMeta = MemoryReader_->GetMeta(/*chunkReadOptions*/ {})
            .Apply(BIND(
                &TCachedVersionedChunkMeta::Create,
                /*prepareColumnarMeta*/ false,
                /*memoryTracker*/ nullptr));

        return WaitFor(asyncCachedMeta).ValueOrThrow();
    }

    ISchemalessChunkReaderPtr LookupRows(
        TSharedRange<TLegacyKey> keys,
        const TSortColumns& sortColumns)
    {
        auto options = New<TChunkReaderOptions>();
        options->DynamicTable = true;

        auto chunkState = New<TChunkState>(TChunkState{
            .BlockCache = GetNullBlockCache(),
            .ChunkSpec = ChunkSpec_,
            .TableSchema = Schema_,
        });

        return CreateSchemalessLookupChunkReader(
            std::move(chunkState),
            FetchMeta(),
            TChunkReaderConfig::GetDefault(),
            options,
            MemoryReader_,
            WriteNameTable_,
            /*chunkReadOptions*/ {},
            sortColumns,
            /*omittedInaccessibleColumns*/ {},
            /*columnFilter*/ {},
            keys);
    }


    ISchemalessChunkReaderPtr LookupRanges(
        TSharedRange<TLegacyKey> keys,
        const TSortColumns& sortColumns)
    {
        auto options = New<TChunkReaderOptions>();
        options->DynamicTable = true;

        auto chunkState = New<TChunkState>(TChunkState{
            .BlockCache = GetNullBlockCache(),
            .ChunkSpec = ChunkSpec_,
            .TableSchema = Schema_,
        });

        return CreateSchemalessKeyRangesChunkReader(
            std::move(chunkState),
            FetchMeta(),
            TChunkReaderConfig::GetDefault(),
            options,
            MemoryReader_,
            WriteNameTable_,
            /*chunkReadOptions*/ {},
            sortColumns,
            /*omittedInaccessibleColumns*/ {},
            /*columnFilter*/ {},
            keys);
    }

    ISchemalessChunkReaderPtr ReadRange(
        const NChunkClient::TReadRange& readRange,
        const TSortColumns& sortColumns)
    {
        auto options = New<TChunkReaderOptions>();
        options->DynamicTable = true;

        auto meta = FetchMeta();

        auto chunkState = New<TChunkState>(TChunkState{
            .BlockCache = GetNullBlockCache(),
            .ChunkSpec = ChunkSpec_,
            .TableSchema = Schema_,
        });

        return CreateSchemalessRangeChunkReader(
            std::move(chunkState),
            meta,
            TChunkReaderConfig::GetDefault(),
            options,
            MemoryReader_,
            WriteNameTable_,
            /*chunkReadOptions*/ {},
            sortColumns,
            /*omittedInaccessibleColumns*/ {},
            /*columnFilter*/ {},
            readRange);
    }

    TTableSchemaPtr Schema_;
    TMemoryWriterPtr MemoryWriter_;
    IChunkReaderPtr MemoryReader_;
    TNameTablePtr WriteNameTable_;
    TChunkSpec ChunkSpec_;
    ISchemalessChunkWriterPtr ChunkWriter_;
    std::vector<ESortOrder> SortOrders_;

    TChunkedMemoryPool Pool_;
    TRowBufferPtr RowBuffer_;
    std::vector<TUnversionedRow> Rows_;
    TUnversionedRow sentinelRow_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunksLookupTest
    : public TSchemalessSortedChunksTest {
public:
    void SetUp() override
    {
        auto optimizeFor = std::get<0>(GetParam());
        int blockSize = std::get<1>(GetParam());
        Schema_ = New<TTableSchema>(std::get<2>(GetParam()));
        InitChunkWriter(optimizeFor, Schema_, blockSize);
        InitRows(1000, Schema_, WriteNameTable_);
        InitChunk();
    }
};

TEST_P(TSchemalessChunksLookupTest, Simple)
{
    std::vector<TUnversionedRow> expected;
    std::vector<TUnversionedRow> keys;

    for (int index = 0; index < std::ssize(Rows_); ++index) {
        if (index % 10 != 0) {
            continue;
        }

        auto row = Rows_[index];
        expected.push_back(row);

        auto key = TMutableUnversionedRow::Allocate(&Pool_, Schema_->GetKeyColumnCount());
        for (int valueIndex = 0; valueIndex < Schema_->GetKeyColumnCount(); ++valueIndex) {
            key[valueIndex] = row[valueIndex];
        }
        keys.push_back(key);
    }

    auto reader = LookupRows(MakeSharedRange(keys), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetKeyColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksLookupTest, WiderKeyColumns)
{
    std::vector<TUnversionedRow> expected;
    std::vector<TUnversionedRow> keys;

    auto sortColumns = Schema_->GetSortColumns();
    sortColumns.push_back({"w1", ESortOrder::Ascending});
    sortColumns.push_back({"w2", ESortOrder::Ascending});

    for (int index = 0; index < std::ssize(Rows_); ++index) {
        if (index % 10 != 0) {
            continue;
        }

        auto row = Rows_[index];
        expected.push_back(row);

        auto key = TMutableUnversionedRow::Allocate(&Pool_, sortColumns.size());
        for (int valueIndex = 0; valueIndex < Schema_->GetKeyColumnCount(); ++valueIndex) {
            key[valueIndex] = row[valueIndex];
        }
        for (int valueIndex = Schema_->GetKeyColumnCount(); valueIndex < static_cast<int>(key.GetCount()); ++valueIndex) {
            key[valueIndex] = MakeUnversionedSentinelValue(EValueType::Null, valueIndex);
        }
        keys.push_back(key);
    }

    auto reader = LookupRows(MakeSharedRange(keys), sortColumns);
    CheckSchemalessResult(expected, reader, Schema_->GetKeyColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksLookupTest, BlockBoundaries)
{
    // Test applies to horizontal format only.
    auto optimizeFor = std::get<0>(GetParam());
    if (optimizeFor != EOptimizeFor::Lookup) return;

    auto meta = FetchMeta();
    std::vector<TUnversionedRow> keys;
    keys.push_back(meta->BlockLastKeys()[0]);
    keys.push_back(meta->BlockLastKeys()[2]);
    keys.push_back(meta->BlockLastKeys()[4]);

    std::vector<TUnversionedRow> expected;
    int pos = 0;
    for (int index = 0; index < std::ssize(Rows_); ++index) {
        if (pos < std::ssize(keys) &&
            CompareRows(keys[pos], Rows_[index], Schema_->GetKeyColumnCount()) == 0)
        {
            expected.push_back(Rows_[index]);
            ++pos;
        }
    }

    auto reader = LookupRows(MakeSharedRange(keys), Schema_->GetSortColumns());

    CheckSchemalessResult(expected, reader, Schema_->GetKeyColumnCount(), ReadOptions());
}

INSTANTIATE_TEST_SUITE_P(Sorted,
    TSchemalessChunksLookupTest,
    ::testing::Combine(
        ::testing::Values(
            EOptimizeFor::Scan,
            EOptimizeFor::Lookup),
        ::testing::Values(32, 2048),  // block size.
        ::testing::Values(
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true;unique_keys=%true>["
                "{name = c1; type = boolean; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = int64; sort_order = ascending};"
                "{name = c4; type = double};]"))),
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true;unique_keys=%true>["
                "{name = c1; type = int64; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = string; sort_order = ascending};"
                "{name = c4; type = boolean; sort_order = ascending};"
                "{name = c5; type = any};]")))),
        ::testing::Values(16_MB, 256),  // max data weight per read
        ::testing::Values(10000, 4)  // max rows per read
));

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunksKeyRangesTest
    : public TSchemalessSortedChunksTest
{
};

INSTANTIATE_TEST_SUITE_P(Sorted,
    TSchemalessChunksKeyRangesTest,
    ::testing::Combine(
        ::testing::Values(EOptimizeFor::Lookup),
        ::testing::Values(32, 2048),  // block size.
        ::testing::Values(
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true;unique_keys=%true>["
                "{name = c1; type = boolean; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = int64; sort_order = ascending};"
                "{name = c4; type = double};]"))),
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true;unique_keys=%true>["
                "{name = c1; type = int64; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = string; sort_order = ascending};"
                "{name = c4; type = boolean; sort_order = ascending};"
                "{name = c5; type = any};]"))),
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true;unique_keys=%true>["
                "{name = c1; type = int64; sort_order = ascending};"
                "{name = c2; type = string; sort_order = ascending }; ]")))),
            ::testing::Values(16_MB, 256),  // max data weight per read
            ::testing::Values(10000, 4)  // max rows per read
));

TEST_P(TSchemalessChunksKeyRangesTest, NoIntervals)
{
    std::vector<TUnversionedRow> keys{};
    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());
    CheckSchemalessResult(std::vector<TUnversionedRow>{}, reader, Schema_->GetKeyColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, Simple)
{
    std::vector<TUnversionedRow> expected;
    std::vector<TUnversionedRow> keys;

    for (int index = 0; index < std::ssize(Rows_); ++index) {
        auto row = Rows_[index];
        expected.push_back(row);

        keys.push_back(ExtractKey(row));
    }

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, MissingRows)
{
    std::vector<TUnversionedRow> expected;
    std::vector<TUnversionedRow> keys;

    for (int index = 0; index < std::ssize(Rows_); ++index) {
        expected.push_back(Rows_[index]);

        keys.push_back(ExtractKey(Rows_[index]));
        keys.push_back(ExtractKey(CreateMissingRow(index, Schema_, WriteNameTable_)));
    }

    // KeyRanges reader expects sorted keys.
    std::sort(keys.begin(), keys.end());

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, KeyPrefix)
{
    const int prefixLength = 1;
    const auto& selectedKey = ExtractKeyPrefix(Rows_[Rows_.size() / 2], prefixLength);

    std::vector<TUnversionedRow> expected;
    std::vector<TUnversionedRow> keys;

    for (int index = 0; index < std::ssize(Rows_); ++index) {
        if (ExtractKeyPrefix(Rows_[index], prefixLength) == selectedKey) {
            expected.push_back(Rows_[index]);
        }
    }

    keys.push_back(selectedKey);

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, MultipleKeyPrefixes)
{
    const int prefixLength = 2;
    const auto& key1 = ExtractKeyPrefix(Rows_[Rows_.size() / 4], prefixLength);
    const auto& key2 = ExtractKeyPrefix(Rows_[3 * Rows_.size() / 4], prefixLength);

    std::vector<TUnversionedRow> expected;
    std::vector<TUnversionedRow> keys;

    for (int index = 0; index < std::ssize(Rows_); ++index) {
        if (ExtractKeyPrefix(Rows_[index], prefixLength) == key1 ||
            ExtractKeyPrefix(Rows_[index], prefixLength) == key2) {
            expected.push_back(Rows_[index]);
        }
    }

    keys.push_back(key1);
    keys.push_back(key2);

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());

    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, ChunkSpecRange)
{
    int lo = Rows_.size() / 10;
    int hi = 2 * Rows_.size() / 10;

    std::vector<TUnversionedRow> expected;
    for (int i = lo; i < hi; i++) {
        expected.push_back(Rows_[i]);
    }

    std::vector<TUnversionedRow> keys;
    for (int i = 0; i < std::ssize(Rows_); i++) {
        if (keys.empty() || ExtractKey(Rows_[i]) != keys.back()) {
            keys.push_back(ExtractKey(Rows_[i]));
        }
    }

    TReadLimit chunkSpecLowerLimit = CreateReadLimit(ExtractKey(Rows_[lo]), false);
    TReadLimit chunkSpecUpperLimit = CreateReadLimit(ExtractKey(Rows_[hi]), true);

    ToProto(ChunkSpec_.mutable_lower_limit(), chunkSpecLowerLimit);
    ToProto(ChunkSpec_.mutable_upper_limit(), chunkSpecUpperLimit);

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());

    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, ChunkSpecRangeWithKeyPrefix)
{
    const int prefixLength = 2;

    int lo = Rows_.size() / 10;
    int hi = 2 * Rows_.size() / 10;

    std::vector<TUnversionedRow> expected;
    for (int i = lo; i < hi; i++) {
        expected.push_back(Rows_[i]);
    }

    std::vector<TUnversionedRow> keys;
    for (int i = 0; i < std::ssize(Rows_); i++) {
        const auto prefix = ExtractKeyPrefix(Rows_[i], prefixLength);
        if (keys.empty() || prefix != keys.back()) {
            keys.push_back(prefix);
        }
    }

    TReadLimit chunkSpecLowerLimit = CreateReadLimit(ExtractKey(Rows_[lo]), false);
    TReadLimit chunkSpecUpperLimit = CreateReadLimit(ExtractKey(Rows_[hi]), true);
    ToProto(ChunkSpec_.mutable_lower_limit(), chunkSpecLowerLimit);
    ToProto(ChunkSpec_.mutable_upper_limit(), chunkSpecUpperLimit);

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, ChunkSpecRangeMultipleKeyPrefies)
{
    const int prefixLength = 1;

    int lo = Rows_.size() / 10;
    int hi = 9 * Rows_.size() / 10;

    std::vector<TUnversionedRow> keys;

    keys.push_back(ExtractKeyPrefix(Rows_[0], prefixLength));
    keys.push_back(ExtractKeyPrefix(Rows_.back(), prefixLength));

    std::vector<TUnversionedRow> expected;
    for (int i = lo; i < hi; i++) {
        if (ExtractKeyPrefix(Rows_[i], prefixLength) == keys[0] ||
            ExtractKeyPrefix(Rows_[i], prefixLength) == keys[1]) {
            expected.push_back(Rows_[i]);
        }
    }

    TReadLimit chunkSpecLowerLimit = CreateReadLimit(ExtractKey(Rows_[lo]), false);
    TReadLimit chunkSpecUpperLimit = CreateReadLimit(ExtractKey(Rows_[hi]), true);
    ToProto(ChunkSpec_.mutable_lower_limit(), chunkSpecLowerLimit);
    ToProto(ChunkSpec_.mutable_upper_limit(), chunkSpecUpperLimit);

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, ChunkSpecTrivialRange)
{
    const int prefixLength = 2;
    std::vector<TUnversionedRow> expected;

    std::vector<TUnversionedRow> keys;
    for (int i = 100; i < 200; i++) {
        auto prefix = ExtractKeyPrefix(Rows_[i], prefixLength);
        if (keys.empty() || prefix != keys.back()) {
            keys.push_back(prefix);
        }
        expected.push_back(Rows_[i]);
    }

    TReadLimit chunkSpecLowerLimit = CreateReadLimit(ExtractKey(Rows_[0]), false);
    TReadLimit chunkSpecUpperLimit = CreateReadLimit(ExtractKey(sentinelRow_), true);

    ToProto(ChunkSpec_.mutable_lower_limit(), chunkSpecLowerLimit);
    ToProto(ChunkSpec_.mutable_upper_limit(), chunkSpecUpperLimit);

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, ChunkSpecEmptyRange)
{
    std::vector<TUnversionedRow> keys;
    for (int i = 0; i < std::ssize(Rows_); i++) {
        if (keys.empty() || ExtractKey(Rows_[i]) != keys.back()) {
            keys.push_back(ExtractKey(Rows_[i]));
        }
    }

    const int rowIndex = Rows_.size() / 4;
    TReadLimit chunkSpecLowerLimit = CreateReadLimit(ExtractKey(Rows_[rowIndex]), false);
    TReadLimit chunkSpecUpperLimit = CreateReadLimit(ExtractKey(Rows_[rowIndex]), true);

    ToProto(ChunkSpec_.mutable_lower_limit(), chunkSpecLowerLimit);
    ToProto(ChunkSpec_.mutable_upper_limit(), chunkSpecUpperLimit);

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());

    CheckSchemalessResult(std::vector<TUnversionedRow>{},
                          reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesTest, BlockBoundaries)
{
    auto meta = FetchMeta();
    std::vector<TUnversionedRow> keys;
    keys.push_back(meta->BlockLastKeys()[0]);
    keys.push_back(meta->BlockLastKeys()[2]);
    keys.push_back(meta->BlockLastKeys()[4]);

    std::vector<TUnversionedRow> expected;
    int pos = 0;
    for (int index = 0; index < std::ssize(Rows_); ++index) {
        if (pos < std::ssize(keys) &&
            CompareRows(keys[pos], Rows_[index], Schema_->GetKeyColumnCount()) == 0)
        {
            expected.push_back(Rows_[index]);
            ++pos;
        }
    }

    auto reader = LookupRanges(MakeSharedRange(keys), Schema_->GetSortColumns());

    CheckSchemalessResult(expected, reader, Schema_->GetKeyColumnCount(), ReadOptions());
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunksRangeTest
    : public TSchemalessSortedChunksTest
{
};

INSTANTIATE_TEST_SUITE_P(Sorted,
    TSchemalessChunksRangeTest,
    ::testing::Combine(
        ::testing::Values(EOptimizeFor::Lookup),
        ::testing::Values(32, 2048),  // block size.
        ::testing::Values(
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true;unique_keys=%true>["
                "{name = c1; type = boolean; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = int64; sort_order = ascending};"
                "{name = c4; type = double};]"))),
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true;unique_keys=%true>["
                "{name = c1; type = int64; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = string; sort_order = ascending};"
                "{name = c4; type = boolean; sort_order = ascending};"
                "{name = c5; type = any};]"))),
            ConvertTo<TTableSchema>(TYsonString(TStringBuf("<strict=%true;unique_keys=%true>["
                "{name = c1; type = int64; sort_order = ascending};"
                "{name = c2; type = string; sort_order = ascending }; ]")))),
            ::testing::Values(16_MB, 256),  // max data weight per read
            ::testing::Values(10000, 4)  // max rows per read
));

TEST_P(TSchemalessChunksRangeTest, BlockBoundaries)
{
    auto meta = FetchMeta();

    auto lowerBound = TOwningKeyBound::FromRow(
            TUnversionedOwningRow(meta->BlockLastKeys()[0]),
            /*inclusive*/ true, /*upper*/ false);

    auto upperBound = TOwningKeyBound::FromRow(
            TUnversionedOwningRow(meta->BlockLastKeys()[1]),
            /*inclusive*/ true, /*upper*/ true);

    auto lowerReadLimit = TReadLimit(lowerBound);
    auto upperReadLimit = TReadLimit(upperBound);

    auto reader = ReadRange(TReadRange(lowerReadLimit, upperReadLimit), Schema_->GetSortColumns());

    int keyLength = Schema_->GetKeyColumnCount();

    std::vector<TUnversionedRow> expected;
    for (int i = 0; i < std::ssize(Rows_); i++) {
        const auto row = Rows_[i];
        if (TestKey(TRange<TUnversionedValue>(row.begin(), keyLength),
                ToKeyBoundRef(lowerBound), SortOrders_) &&
            TestKey(TRange<TUnversionedValue>(row.begin(), keyLength),
                ToKeyBoundRef(upperBound), SortOrders_))
        {
            expected.push_back(row);
        }
    }

    CheckSchemalessResult(expected, reader, keyLength, ReadOptions());
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunksKeyRangesLargeRowsTest
    : public TSchemalessSortedChunksTest
{
protected:
    TRowBufferPtr Buffer_;

    void SetUp() override
    {
        Buffer_ = New<TRowBuffer>();
        auto optimizeFor = std::get<0>(GetParam());
        int blockSize = std::get<1>(GetParam());
        Schema_ = New<TTableSchema>(std::get<2>(GetParam()));
        InitChunkWriter(optimizeFor, Schema_, blockSize);
        Rows_ = CreateRows();
        InitChunk();
    }

    static constexpr int NumRows = 1024;
    static constexpr int NumKeys = 16;

    static constexpr int KeysPerKey0 = 4;
    static constexpr int RowsPerKey = NumRows / NumKeys;

    std::vector<TUnversionedRow> CreateRows()
    {
        std::vector<TUnversionedRow> rows;

        rows.reserve(NumRows);
        for (int i = 0; i < NumRows; i++) {
            auto row = TMutableUnversionedRow::Allocate(&Pool_, 3);
            row[0] = MakeKey(0, i / RowsPerKey);
            row[1] = MakeKey(1, i / RowsPerKey);
            row[2] = MakeValue(i);

            rows.push_back(row);
        }
        return rows;
    }

    TUnversionedValue MakeString(char prefixChar, int index)
    {
        char buf[32];
        snprintf(buf, 32, "%08x", index);
        std::string fullVal = std::string(10, prefixChar) + std::string(buf);
        auto val = MakeUnversionedStringValue(fullVal.c_str());
        return Buffer_->CaptureValue(val);
    }

    TUnversionedValue MakeKey(int position, int index)
    {
        if (position == 0) {
            return MakeString('k', index / KeysPerKey0);
        } else {
            return MakeString('k', index % KeysPerKey0);
        }
    }

    TUnversionedValue MakeValue(int index)
    {
        return MakeString('v', index);
    }

    void FillKey(int keyIndex, TMutableUnversionedRow* row)
    {
        (*row)[0] = MakeKey(0, keyIndex);
        (*row)[1] = MakeKey(1, keyIndex);
    }

    void AddRows(int keyIndex, std::vector<TUnversionedRow>* out)
    {
        auto keyRow = TMutableUnversionedRow::Allocate(&Pool_, 2);
        FillKey(keyIndex, &keyRow);
        for (int i = 0; i < RowsPerKey; i++) {
            auto row = TMutableUnversionedRow::Allocate(&Pool_, 3);
            row[0] = keyRow[0];
            row[1] = keyRow[1];
            row[2] = MakeValue(keyIndex * RowsPerKey + i);
            out->push_back(row);
        }
    }
};

TEST_P(TSchemalessChunksKeyRangesLargeRowsTest, MultipleIntervals)
{
    const int keyCount = 4;
    const std::array<int, keyCount> keyIndices{0, 3, 7, 15};

    std::vector<TUnversionedRow> keyRows;
    std::vector<TUnversionedRow> expected;

    for (int keyIndex : keyIndices) {
        auto keyRow = TMutableUnversionedRow::Allocate(&Pool_, 2);
        keyRow[0] = MakeKey(0, keyIndex);
        keyRow[1] = MakeKey(1, keyIndex);

        keyRows.push_back(keyRow);

        for (int j = 0; j < RowsPerKey; j++) {
            auto row = TMutableUnversionedRow::Allocate(&Pool_, 3);
            row[0] = keyRow[0];
            row[1] = keyRow[1];
            row[2] = MakeValue(keyIndex * RowsPerKey + j);
            expected.push_back(row);
        }
    }

    auto keys = MakeSharedRange<TUnversionedRow>(keyRows);
    auto reader = LookupRanges(keys, Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetKeyColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesLargeRowsTest, ChunkSpecRange)
{
    auto loKey = TMutableUnversionedRow::Allocate(&Pool_, 2);
    loKey[0] = MakeKey(0, /*keyIndex*/ 3);
    loKey[1] = MakeKey(1, /*keyIndex*/ 3);

    auto hiKey = TMutableUnversionedRow::Allocate(&Pool_, 2);
    hiKey[0] = MakeKey(0, /*keyIndex*/ 14);
    hiKey[1] = MakeKey(1, /*keyIndex*/ 14);

    ToProto(ChunkSpec_.mutable_lower_limit(), CreateReadLimit(loKey, false));
    ToProto(ChunkSpec_.mutable_upper_limit(), CreateReadLimit(hiKey, true));

    std::vector<TUnversionedRow> keyRows;
    for (int key0 : {0, 1, 3}) {
        auto row = TMutableUnversionedRow::Allocate(&Pool_, 1);
        row[0] = MakeString('k', key0);
        keyRows.push_back(row);
    }

    std::vector<TUnversionedRow> expected;
    for (int keyIndex : {3, 4, 5, 6, 7, 12, 13}) {
        AddRows(keyIndex, &expected);
    }

    auto reader = LookupRanges(MakeSharedRange(keyRows), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesLargeRowsTest, ChunkSpecInvertedInclusivenessRange)
{
    auto loKey = TMutableUnversionedRow::Allocate(&Pool_, 2);
    loKey[0] = MakeKey(0, /*keyIndex*/ 3);
    loKey[1] = MakeKey(1, /*keyIndex*/ 3);

    auto hiKey = TMutableUnversionedRow::Allocate(&Pool_, 2);
    hiKey[0] = MakeKey(0, /*keyIndex*/ 14);
    hiKey[1] = MakeKey(1, /*keyIndex*/ 14);

    ToProto(ChunkSpec_.mutable_lower_limit(), TReadLimit(
        TOwningKeyBound::FromRow(TUnversionedOwningRow(loKey), false, false)));

    ToProto(ChunkSpec_.mutable_upper_limit(), TReadLimit(
        TOwningKeyBound::FromRow(TUnversionedOwningRow(hiKey), true, true)));

    std::vector<TUnversionedRow> keyRows;
    for (int key0 : {0, 1, 3}) {
        auto row = TMutableUnversionedRow::Allocate(&Pool_, 1);
        row[0] = MakeString('k', key0);
        keyRows.push_back(row);
    }

    std::vector<TUnversionedRow> expected;
    for (int keyIndex : {4, 5, 6, 7, 12, 13, 14}) {
        AddRows(keyIndex, &expected);
    }

    auto reader = LookupRanges(MakeSharedRange(keyRows), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesLargeRowsTest, ChunkSpecRangePrefixes)
{
    // In this test, ChunkSpec read limits include prefixes of full keys.
    // It is not expected to happen. However, KeyRangesReader should defensively support
    // this case.

    auto loKey = TMutableUnversionedRow::Allocate(&Pool_, 1);
    loKey[0] = MakeKey(0, /*keyIndex*/ 4);

    auto hiKey = TMutableUnversionedRow::Allocate(&Pool_, 1);
    hiKey[0] = MakeKey(0, /*keyIndex*/ 12);

    ToProto(ChunkSpec_.mutable_lower_limit(), CreateReadLimit(loKey, false));
    ToProto(ChunkSpec_.mutable_upper_limit(), CreateReadLimit(hiKey, true));

    std::vector<TUnversionedRow> keyRows;
    for (int keyIndex : {3, 5, 9, 11, 12}) {
        auto row = TMutableUnversionedRow::Allocate(&Pool_, 2);
        FillKey(keyIndex, &row);
        keyRows.push_back(row);
    }

    std::vector<TUnversionedRow> expected;
    for (int keyIndex : {5, 9, 11}) {
        AddRows(keyIndex, &expected);
    }

    auto reader = LookupRanges(MakeSharedRange(keyRows), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesLargeRowsTest, ChunkSpecDegenerateFullRange)
{
    // Construct a [[], []] range.
    ToProto(ChunkSpec_.mutable_lower_limit(), TReadLimit(TOwningKeyBound::FromRow(
        TUnversionedOwningRow(TUnversionedValueRange()), /*inclusive*/ true, /*upper*/ false)));

    ToProto(ChunkSpec_.mutable_upper_limit(), TReadLimit(TOwningKeyBound::FromRow(
        TUnversionedOwningRow(TUnversionedValueRange()), /*inclusive*/ true, /*upper*/ true)));

    const std::vector<int> indexes{0, 3, 5, 11, 12, 15};

    std::vector<TUnversionedRow> keyRows;
    for (int keyIndex : indexes) {
        auto row = TMutableUnversionedRow::Allocate(&Pool_, 2);
        FillKey(keyIndex, &row);
        keyRows.push_back(row);
    }

    std::vector<TUnversionedRow> expected;
    for (int keyIndex : indexes) {
        AddRows(keyIndex, &expected);
    }

    auto reader = LookupRanges(MakeSharedRange(keyRows), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

TEST_P(TSchemalessChunksKeyRangesLargeRowsTest, ChunkSpecDegenerateEmptyRange)
{
    // Construct a [[], []) range.
    ToProto(ChunkSpec_.mutable_lower_limit(), TReadLimit(TOwningKeyBound::FromRow(
        TUnversionedOwningRow(TUnversionedValueRange()), /*inclusive*/ true, /*upper*/ false)));

    ToProto(ChunkSpec_.mutable_upper_limit(), TReadLimit(TOwningKeyBound::FromRow(
        TUnversionedOwningRow(TUnversionedValueRange()), /*inclusive*/ false, /*upper*/ true)));

    const std::vector<int> indexes{0, 3, 5, 11, 12, 15};

    std::vector<TUnversionedRow> keyRows;
    for (int keyIndex : indexes) {
        auto row = TMutableUnversionedRow::Allocate(&Pool_, 2);
        FillKey(keyIndex, &row);
        keyRows.push_back(row);
    }

    std::vector<TUnversionedRow> expected{};

    auto reader = LookupRanges(MakeSharedRange(keyRows), Schema_->GetSortColumns());
    CheckSchemalessResult(expected, reader, Schema_->GetColumnCount(), ReadOptions());
}

INSTANTIATE_TEST_SUITE_P(Sorted,
    TSchemalessChunksKeyRangesLargeRowsTest,
    ::testing::Combine(
        ::testing::Values(EOptimizeFor::Lookup),
        ::testing::Values(256),  // block size.
        ::testing::Values(ConvertTo<TTableSchema>(TYsonString(TStringBuf(
            "<strict=%true;unique_keys=%true>["
            "{name = k0; type = string; sort_order = ascending};"
            "{name = k1; type = string; sort_order = ascending};"
            "{name = v; type = string; }; ]")))
        ),
        ::testing::Values(16_MB, 256),  // max data weight per read
        ::testing::Values(10000, 4)  // max rows per read
));

} // namespace
} // namespace NYT::NTableClient
