#include <yt/core/test_framework/framework.h>
#include "table_client_helpers.h"

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/chunk_state.h>
#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/compression/public.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NTableClient {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TChunkReaderStatistics;
using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

int RowCount = 50000;
auto StringValue = AsStringBuf("She sells sea shells on a sea shore");
auto AnyValueList = AsStringBuf("[one; two; three]");
auto AnyValueMap = AsStringBuf("{a=b; c=d}");

std::vector<TString> ColumnNames = {"c0", "c1", "c2", "c3", "c4", "c5", "c6"};

class TSchemalessChunksTest
    : public ::testing::TestWithParam<std::tuple<EOptimizeFor, TTableSchema, TColumnFilter, TReadRange>>
{
public:
    static void SetUpTestCase()
    {
        auto nameTable = New<TNameTable>();
        InitNameTable(nameTable);
        Rows_ = CreateRows(nameTable);
    }

protected:
    IChunkReaderPtr MemoryReader_;
    TNameTablePtr WriteNameTable_;
    TChunkSpec ChunkSpec_;
    TColumnarChunkMetaPtr ChunkMeta_;

    static TChunkedMemoryPool Pool_;
    static std::vector<TUnversionedRow> Rows_;

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
                Y_UNREACHABLE();
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
                Y_UNREACHABLE();
        }
    }

    static std::vector<TUnversionedRow> CreateRows(TNameTablePtr nameTable)
    {
        std::vector<TUnversionedRow> rows;
        for (int rowIndex = 0; rowIndex < RowCount; ++rowIndex) {
            auto row = TMutableUnversionedRow::Allocate(&Pool_, 7);
            row[0] = CreateC0(rowIndex, nameTable);
            row[1] = CreateC1(rowIndex, nameTable);
            row[2] = CreateC2(rowIndex, nameTable);
            row[3] = CreateC3(rowIndex, nameTable);
            row[4] = CreateC4(rowIndex, nameTable);
            row[5] = CreateC5(rowIndex, nameTable);
            row[6] = CreateC6(rowIndex, nameTable);
            rows.push_back(row);
        }
        return rows;
    }

    virtual void SetUp() override
    {
        auto memoryWriter = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 256;

        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = std::get<0>(GetParam());
        auto chunkWriter = CreateSchemalessChunkWriter(
            config,
            options,
            std::get<1>(GetParam()),
            memoryWriter);

        WriteNameTable_ = chunkWriter->GetNameTable();
        InitNameTable(WriteNameTable_);

        chunkWriter->Write(Rows_);
        EXPECT_TRUE(chunkWriter->Close().Get().IsOK());

        MemoryReader_ = CreateMemoryReader(
            std::move(memoryWriter->GetChunkMeta()),
            std::move(memoryWriter->GetBlocks()));

        ToProto(ChunkSpec_.mutable_chunk_id(), NullChunkId);
        ChunkSpec_.set_table_row_index(42);
        ChunkMeta_ = New<TColumnarChunkMeta>(memoryWriter->GetChunkMeta());
    }

    static void InitNameTable(TNameTablePtr nameTable, int idShift = 0)
    {
        for (int id = 0; id < ColumnNames.size(); ++id) {
            EXPECT_EQ(id, nameTable->GetIdOrRegisterName(ColumnNames[(id + idShift) % ColumnNames.size()]));
        }
    }

    std::vector<TUnversionedRow> CreateExpected(
        const std::vector<TUnversionedRow>& initial,
        TNameTablePtr writeNameTable,
        TNameTablePtr readNameTable,
        TColumnFilter columnFilter,
        TReadRange readRange,
        int keyColumnCount)
    {
        std::vector<TUnversionedRow> rows;

        int lowerRowIndex = 0;
        if (readRange.LowerLimit().HasRowIndex()) {
            lowerRowIndex = readRange.LowerLimit().GetRowIndex();
        }

        int upperRowIndex = initial.size();
        if (readRange.UpperLimit().HasRowIndex()) {
            upperRowIndex = readRange.UpperLimit().GetRowIndex();
        }

        auto fulfillLowerKeyLimit = [&] (TUnversionedRow row) {
            return !readRange.LowerLimit().HasKey() ||
                CompareRows(
                    row.Begin(),
                    row.Begin() + keyColumnCount,
                    readRange.LowerLimit().GetKey().Begin(),
                    readRange.LowerLimit().GetKey().End()) >= 0;
        };

        auto fulfillUpperKeyLimit = [&] (TUnversionedRow row) {
            return !readRange.UpperLimit().HasKey() ||
               CompareRows(
                   row.Begin(),
                   row.Begin() + keyColumnCount,
                   readRange.UpperLimit().GetKey().Begin(),
                   readRange.UpperLimit().GetKey().End()) < 0;
        };

        for (int rowIndex = lowerRowIndex; rowIndex < upperRowIndex; ++rowIndex) {
            auto initialRow = initial[rowIndex];
            if (fulfillLowerKeyLimit(initialRow) && fulfillUpperKeyLimit(initialRow)) {
                auto row = TMutableUnversionedRow::Allocate(&Pool_, initialRow.GetCount());
                int count = 0;
                for (const auto* it = initialRow.Begin(); it != initialRow.End(); ++it) {
                    auto name = writeNameTable->GetName(it->Id);
                    auto readerId = readNameTable->GetId(name);

                    if (columnFilter.Contains(readerId)) {
                        row[count] = *it;
                        row[count].Id = readerId;
                        ++count;
                    }
                }
                row.SetCount(count);
                rows.push_back(row);
            }
        }

        return rows;
    }
};

TChunkedMemoryPool TSchemalessChunksTest::Pool_;
std::vector<TUnversionedRow> TSchemalessChunksTest::Rows_;

////////////////////////////////////////////////////////////////////////////////

TEST_P(TSchemalessChunksTest, WithoutSampling)
{
    auto readNameTable = New<TNameTable>();
    InitNameTable(readNameTable, 4);

    auto columnFilter = std::get<2>(GetParam());
    auto expected = CreateExpected(
        Rows_,
        WriteNameTable_,
        readNameTable,
        columnFilter,
        std::get<3>(GetParam()),
        std::get<1>(GetParam()).GetKeyColumnCount());

    auto chunkState = New<TChunkState>(
        GetNullBlockCache(),
        ChunkSpec_,
        nullptr,
        nullptr,
        nullptr,
        nullptr);

    TClientBlockReadOptions blockReadOptions;
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();

    auto chunkReader = CreateSchemalessChunkReader(
        std::move(chunkState),
        ChunkMeta_,
        New<TChunkReaderConfig>(),
        New<TChunkReaderOptions>(),
        MemoryReader_,
        readNameTable,
        blockReadOptions,
        TKeyColumns(),
        columnFilter,
        std::get<3>(GetParam()));

    CheckSchemalessResult(expected, chunkReader, 0);
}

INSTANTIATE_TEST_CASE_P(Unsorted,
    TSchemalessChunksTest,
    ::testing::Combine(
        ::testing::Values(
            EOptimizeFor::Scan,
            EOptimizeFor::Lookup),
        ::testing::Values(
            ConvertTo<TTableSchema>(TYsonString("<strict=%false>[]")),
            ConvertTo<TTableSchema>(TYsonString("<strict=%false>[{name = c0; type = any}; {name = c1; type = int64}; {name = c2; type = uint64}; ]")),
            ConvertTo<TTableSchema>(TYsonString("<strict=%true>[{name = c0; type = any}; {name = c1; type = int64}; "
                "{name = c2; type = uint64}; {name = c3; type = string}; {name = c4; type = boolean}; {name = c5; type = double}; "
                "{name = c6; type = any};]")),
            ConvertTo<TTableSchema>(TYsonString("<strict=%false>[{name = c0; type = any}; {name = c1; type = int64}; "
                "{name = c2; type = uint64}; {name = c3; type = string}; {name = c4; type = boolean}; {name = c5; type = double}; {name = c6; type = any};]"))),
        ::testing::Values(TColumnFilter(), TColumnFilter({2, 4})),
        ::testing::Values(
            TReadRange(),
            TReadRange(TReadLimit().SetRowIndex(RowCount / 3), TReadLimit().SetRowIndex(RowCount / 3)),
            TReadRange(TReadLimit().SetRowIndex(RowCount / 3), TReadLimit().SetRowIndex(2 * RowCount / 3)))));


INSTANTIATE_TEST_CASE_P(Sorted,
    TSchemalessChunksTest,
    ::testing::Combine(
        ::testing::Values(
            EOptimizeFor::Scan,
            EOptimizeFor::Lookup),
        ::testing::Values(
            ConvertTo<TTableSchema>(TYsonString("<strict=%false>["
                "{name = c0; type = any; sort_order = ascending};"
                "{name = c1; type = any; sort_order = ascending};"
                "{name = c2; type = any; sort_order = ascending}]")),
            ConvertTo<TTableSchema>(TYsonString("<strict=%true>["
                "{name = c0; type = any; sort_order = ascending};"
                "{name = c1; type = int64; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = string; sort_order = ascending};"
                "{name = c4; type = boolean; sort_order = ascending};"
                "{name = c5; type = double; sort_order = ascending};"
                "{name = c6; type = any};]"))),
        ::testing::Values(TColumnFilter(), TColumnFilter({0, 5})),
        ::testing::Values(
            TReadRange(),
            TReadRange(TReadLimit().SetKey(YsonToKey("<type=null>#")), TReadLimit().SetKey(YsonToKey("<type=null>#"))),
            TReadRange(TReadLimit().SetKey(YsonToKey("-65537; -1; 1u; <type=null>#")), TReadLimit()),
            TReadRange(TReadLimit().SetKey(YsonToKey("-65537; -1; 1u; <type=null>#")), TReadLimit().SetKey(YsonToKey("350000.1; 1; 1; \"Z\""))))));

// ToDo(psushin):
//  1. Test sampling.
//  2. Test system columns.

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunksLookupTest
    : public ::testing::TestWithParam<std::tuple<EOptimizeFor, TTableSchema>>
{
protected:
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

    TUnversionedValue CreateValue(int rowIndex, int id, const TColumnSchema& columnSchema)
    {
        switch (columnSchema.GetPhysicalType()) {
            case EValueType::Int64:
                return CreateInt64(rowIndex, id);
            case EValueType::Uint64:
                return CreateUint64(rowIndex, id);
            case EValueType::Double:
                return CreateDouble(rowIndex, id);
            case EValueType::Boolean:
                return CreateBoolean(rowIndex, id);
            case EValueType::String:
                return CreateString(rowIndex, id);
            case EValueType::Any:
                return CreateAny(rowIndex, id);
            default:
                Y_UNREACHABLE();
        }
    }

    TUnversionedRow CreateRow(int rowIndex, const TTableSchema& schema, TNameTablePtr nameTable)
    {
        auto row = TMutableUnversionedRow::Allocate(&Pool_, schema.Columns().size());
        for (int index = 0; index < schema.Columns().size(); ++index) {
            const auto& column = schema.Columns()[index];
            row[index] = CreateValue(rowIndex, nameTable->GetIdOrRegisterName(column.Name()), column);
        }
        return row;
    }

    void InitRows(int rowCount, const TTableSchema& schema, TNameTablePtr nameTable)
    {
        std::vector<TUnversionedRow> rows;

        for (int index = 0; index < rowCount; ++index) {
            rows.push_back(CreateRow(index, schema, nameTable));
        }

        std::sort(
            rows.begin(),
            rows.end(), [&] (const TUnversionedRow& lhs, const TUnversionedRow& rhs) {
                return CompareRows(lhs, rhs, schema.GetKeyColumnCount()) < 0;
            });
        rows.erase(
            std::unique(
                rows.begin(),
                rows.end(), [&] (const TUnversionedRow& lhs, const TUnversionedRow& rhs) {
                    return CompareRows(lhs, rhs, schema.GetKeyColumnCount()) == 0;
                }),
            rows.end());

        Rows_ = std::move(rows);
    }

    void InitChunk(int rowCount, EOptimizeFor optimizeFor, const TTableSchema& schema)
    {
        auto memoryWriter = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 2 * 1024;

        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = optimizeFor;
        options->ValidateSorted = schema.IsSorted();
        options->ValidateUniqueKeys = schema.IsUniqueKeys();
        auto chunkWriter = CreateSchemalessChunkWriter(
            config,
            options,
            schema,
            memoryWriter);

        WriteNameTable_ = chunkWriter->GetNameTable();
        InitRows(rowCount, schema, WriteNameTable_);

        chunkWriter->Write(Rows_);
        EXPECT_TRUE(chunkWriter->Close().Get().IsOK());

        MemoryReader_ = CreateMemoryReader(
            std::move(memoryWriter->GetChunkMeta()),
            std::move(memoryWriter->GetBlocks()));

        ToProto(ChunkSpec_.mutable_chunk_id(), NullChunkId);
        ChunkSpec_.set_table_row_index(42);
    }

    ISchemalessChunkReaderPtr LookupRows(
        TSharedRange<TKey> keys,
        const TKeyColumns& keyColumns)
    {
        auto options = New<TChunkReaderOptions>();
        options->DynamicTable = true;

        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();

        auto asyncCachedMeta = TCachedVersionedChunkMeta::Load(
            MemoryReader_,
            blockReadOptions,
            Schema_);
        auto chunkMeta = WaitFor(asyncCachedMeta)
            .ValueOrThrow();
        chunkMeta->InitBlockLastKeys(keyColumns);

        auto chunkState = New<TChunkState>(
            GetNullBlockCache(),
            ChunkSpec_,
            nullptr,
            nullptr,
            nullptr,
            nullptr);

        return CreateSchemalessChunkReader(
            std::move(chunkState),
            chunkMeta,
            New<TChunkReaderConfig>(),
            options,
            MemoryReader_,
            WriteNameTable_,
            blockReadOptions,
            keyColumns,
            TColumnFilter(),
            keys);
    }

    virtual void SetUp() override
    {
        auto optimizeFor = std::get<0>(GetParam());
        Schema_ = std::get<1>(GetParam());
        InitChunk(1000, optimizeFor, Schema_);
    }

    TTableSchema Schema_;
    IChunkReaderPtr MemoryReader_;
    TNameTablePtr WriteNameTable_;
    TChunkSpec ChunkSpec_;

    TChunkedMemoryPool Pool_;
    std::vector<TUnversionedRow> Rows_;
};

TEST_P(TSchemalessChunksLookupTest, Simple)
{
    std::vector<TUnversionedRow> expected;
    std::vector<TUnversionedRow> keys;

    for (int index = 0; index < Rows_.size(); ++index) {
        if (index % 10 != 0) {
            continue;
        }

        auto row = Rows_[index];
        expected.push_back(row);

        auto key = TMutableUnversionedRow::Allocate(&Pool_, Schema_.GetKeyColumnCount());
        for (int valueIndex = 0; valueIndex < Schema_.GetKeyColumnCount(); ++valueIndex) {
            key[valueIndex] = row[valueIndex];
        }
        keys.push_back(key);
    }

    auto reader = LookupRows(MakeSharedRange(keys), Schema_.GetKeyColumns());
    CheckSchemalessResult(expected, reader, Schema_.GetKeyColumnCount());
}

TEST_P(TSchemalessChunksLookupTest, WiderKeyColumns)
{
    std::vector<TUnversionedRow> expected;
    std::vector<TUnversionedRow> keys;

    TKeyColumns keyColumns = Schema_.GetKeyColumns();
    keyColumns.push_back("w1");
    keyColumns.push_back("w2");

    for (int index = 0; index < Rows_.size(); ++index) {
        if (index % 10 != 0) {
            continue;
        }

        auto row = Rows_[index];
        expected.push_back(row);

        auto key = TMutableUnversionedRow::Allocate(&Pool_, keyColumns.size());
        for (int valueIndex = 0; valueIndex < Schema_.GetKeyColumnCount(); ++valueIndex) {
            key[valueIndex] = row[valueIndex];
        }
        for (int valueIndex = Schema_.GetKeyColumnCount(); valueIndex < key.GetCount(); ++valueIndex) {
            key[valueIndex] = MakeUnversionedSentinelValue(EValueType::Null, valueIndex);
        }
        keys.push_back(key);
    }

    auto reader = LookupRows(MakeSharedRange(keys), keyColumns);
    CheckSchemalessResult(expected, reader, Schema_.GetKeyColumnCount());
}

INSTANTIATE_TEST_CASE_P(Sorted,
    TSchemalessChunksLookupTest,
    ::testing::Combine(
        ::testing::Values(
            EOptimizeFor::Scan,
            EOptimizeFor::Lookup),
        ::testing::Values(
            ConvertTo<TTableSchema>(TYsonString("<strict=%true;unique_keys=%true>["
                "{name = c1; type = boolean; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = int64; sort_order = ascending};"
                "{name = c4; type = double};]")),
            ConvertTo<TTableSchema>(TYsonString("<strict=%true;unique_keys=%true>["
                "{name = c1; type = int64; sort_order = ascending};"
                "{name = c2; type = uint64; sort_order = ascending};"
                "{name = c3; type = string; sort_order = ascending};"
                "{name = c4; type = boolean; sort_order = ascending};"
                "{name = c5; type = any};]")))));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
