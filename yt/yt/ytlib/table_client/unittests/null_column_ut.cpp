#include <yt/yt/core/test_framework/framework.h>

#include "column_format_ut.h"

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/ytlib/table_chunk_format/null_column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/null_column_writer.h>

namespace NYT::NTableClient {
namespace {

using namespace NTableChunkFormat;

////////////////////////////////////////////////////////////////////////////////

static const ui16 TestColumnId = 0;

std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter)
{
    return CreateUnversionedNullColumnWriter(blockWriter);
}

std::vector<TUnversionedOwningRow> CreateNullRows(size_t count)
{
    return std::vector<TUnversionedOwningRow>(count, MakeUnversionedOwningRow(std::nullopt));
}

TEST(TUnversionedNullColumnTest, ReadValues)
{
    auto writeRead = [] (const std::vector<TUnversionedOwningRow>& rows) {
        const auto& [data, meta] = TSingleColumnWriter(CreateColumnWriter).WriteSingleSegmentBlock(rows);
        return TSingleColumnReader(CreateUnversionedNullColumnReader).ReadBlock(data, meta, TestColumnId);
    };

    {
        const auto expected = CreateNullRows(10);
        EXPECT_EQ(expected, writeRead(expected));
    }

    {
        const auto expected = CreateNullRows(8043);
        EXPECT_EQ(expected, writeRead(expected));
    }

    {
        const auto expected = CreateNullRows(1004080);
        EXPECT_EQ(expected, writeRead(expected));
    }
}

TEST(TUnversionedNullColumnTest, DoesNotMaintainColumnMetaWhenDisabled)
{
    TDataBlockWriter blockWriter(
        /*enableSegmentMetaInBlocks*/ true,
        /*enableColumnMetaInChunkMeta*/ false);
    auto columnWriter = CreateColumnWriter(&blockWriter);

    i64 rowCount = 0;
    for (int blockIndex = 0; blockIndex < 2; ++blockIndex) {
        auto owningRows = CreateNullRows(10);
        std::vector<TUnversionedRow> rows;
        rows.reserve(owningRows.size());
        for (const auto& row : owningRows) {
            rows.push_back(row);
        }

        columnWriter->WriteUnversionedValues(rows);
        rowCount += rows.size();

        auto block = blockWriter.DumpBlock(blockIndex, rowCount);
        EXPECT_FALSE(block.Data.empty());
        EXPECT_EQ(0, columnWriter->ColumnMeta().segments_size());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
