#include "framework.h"

#include "column_format_ut.h"

#include <yt/ytlib/table_chunk_format/schemaless_column_writer.h>
#include <yt/ytlib/table_chunk_format/column_reader.h>

#include <yt/ytlib/table_chunk_format/public.h>

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// TODO(psushin): more and better tests.
TEST(TSchemalessColumnTest, Simple)
{
    TUnversionedOwningRowBuilder builder;
    std::vector<TUnversionedOwningRow> rows;

    // Empty row.
    rows.push_back(builder.FinishRow());

    // One value.
    builder.AddValue(MakeUnversionedInt64Value(18, 0));
    rows.push_back(builder.FinishRow());

    // Two values.
    builder.AddValue(MakeUnversionedDoubleValue(0.01, 1));
    builder.AddValue(MakeUnversionedBooleanValue(false, 2));
    rows.push_back(builder.FinishRow());

    // Three values.
    builder.AddValue(MakeUnversionedStringValue(STRINGBUF("This is string"), 3));
    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 4));
    builder.AddValue(MakeUnversionedAnyValue(STRINGBUF("{a = b}"), 5));
    rows.push_back(builder.FinishRow());

    std::vector<TUnversionedRow> expected(rows.size());
    std::transform(
        rows.begin(),
        rows.end(),
        expected.begin(),
        [](TUnversionedOwningRow owningRow) {
            return owningRow.Get();
        });

    TDataBlockWriter blockWriter;
    auto columnWriter = CreateSchemalessColumnWriter(0, &blockWriter);

    columnWriter->WriteUnversionedValues(MakeRange(expected));
    columnWriter->FinishCurrentSegment();

    auto block = blockWriter.DumpBlock(0, 8);
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);

    auto columnData = codec->Compress(block.Data);
    auto columnMeta = columnWriter->ColumnMeta();

    std::vector<TColumnIdMapping> idMapping;
    for (int index = 0; index < 6; ++index) {
        idMapping.push_back({index, index});
    }

    auto reader = CreateSchemalessColumnReader(columnMeta, idMapping, 0);
    reader->ResetBlock(columnData, 0);

    EXPECT_EQ(expected.size(), reader->GetReadyUpperRowIndex());

    std::vector<ui32> valueCounts(expected.size());
    reader->GetValueCounts(TMutableRange<ui32>(valueCounts.data(), valueCounts.size()));

    EXPECT_EQ(std::vector<ui32>({0, 1, 2, 3}), valueCounts);

    TChunkedMemoryPool pool;
    std::vector<TMutableUnversionedRow> actual;
    for (int valueCount : valueCounts) {
        actual.push_back(TMutableUnversionedRow::Allocate(&pool, valueCount));
    }

    reader->ReadValues(TMutableRange<TMutableUnversionedRow>(actual.data(), actual.size()));

    EXPECT_EQ(expected.size(), actual.size());

    for (int rowIndex = 0; rowIndex < expected.size(); ++rowIndex) {
        EXPECT_EQ(expected[rowIndex].GetCount(), actual[rowIndex].GetCount());
        for (int valueIndex = 0; valueIndex < expected[rowIndex].GetCount(); ++valueIndex) {
            EXPECT_EQ(expected[rowIndex][valueIndex].Type, actual[rowIndex][valueIndex].Type);
            EXPECT_EQ(expected[rowIndex][valueIndex].Id, actual[rowIndex][valueIndex].Id);
            if (expected[rowIndex][valueIndex].Type != EValueType::Any) {
                EXPECT_EQ(expected[rowIndex][valueIndex], actual[rowIndex][valueIndex])
                    << ToString(expected[rowIndex]) << " " << ToString(actual[rowIndex]);
            }
        }
    }
 }

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
