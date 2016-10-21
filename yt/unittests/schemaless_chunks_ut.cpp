#include "framework.h"
#include "table_client_helpers.h"

#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>
#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/compression/public.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/yson/string.h>

namespace NYT {
namespace NTableClient {
namespace {

using namespace NChunkClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

int RowCount = 50000;
auto StringValue = STRINGBUF("She sells sea shells on a sea shore");
auto AnyValueList = STRINGBUF("[one; two; three]");
auto AnyValueMap = STRINGBUF("{a=b; c=d}");

std::vector<Stroka> ColumnNames = {"c0", "c1", "c2", "c3", "c4", "c5", "c6"};

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
    TChunkSpec Spec_;

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
             YUNREACHABLE();
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
                YUNREACHABLE();
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

        EXPECT_TRUE(chunkWriter->Open().Get().IsOK());
        chunkWriter->Write(Rows_);
        EXPECT_TRUE(chunkWriter->Close().Get().IsOK());

        MemoryReader_ = CreateMemoryReader(
            std::move(memoryWriter->GetChunkMeta()),
            std::move(memoryWriter->GetBlocks()));

        ToProto(Spec_.mutable_chunk_id(), NullChunkId);
        Spec_.mutable_chunk_meta()->MergeFrom(memoryWriter->GetChunkMeta());
        Spec_.set_table_row_index(42);

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

    auto chunkReader = CreateSchemalessChunkReader(
        Spec_,
        New<TChunkReaderConfig>(),
        New<TChunkReaderOptions>(),
        MemoryReader_,
        readNameTable,
        GetNullBlockCache(),
        TKeyColumns(),
        columnFilter,
        { std::get<3>(GetParam()) });

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
            TReadRange(TReadLimit().SetKey(BuildKey("<type=null>#")), TReadLimit().SetKey(BuildKey("<type=null>#"))),
            TReadRange(TReadLimit().SetKey(BuildKey("-65537; -1; 1u; <type=null>#")), TReadLimit().SetKey(BuildKey("350000.1; 1; 1; \"Z\""))))));

// ToDo(psushin):
//  1. Test sampling.
//  2. Test system columns.

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
