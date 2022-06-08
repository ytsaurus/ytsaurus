#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/table_chunk_format/schemaless_column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/yt/ytlib/table_chunk_format/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT::NTableClient {
namespace {

using namespace NTableChunkFormat;

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
    builder.AddValue(MakeUnversionedStringValue(TStringBuf("This is string"), 3));
    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 4));
    builder.AddValue(MakeUnversionedAnyValue(TStringBuf("{a = b}"), 5));
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

    // Make two separate writes.
    columnWriter->WriteUnversionedValues(MakeRange(expected.data(), 2));
    columnWriter->WriteUnversionedValues(MakeRange(expected.data() + 2, 2));
    columnWriter->FinishCurrentSegment();

    auto block = blockWriter.DumpBlock(0, 8);
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);

    auto columnData = codec->Compress(block.Data);
    auto columnMeta = columnWriter->ColumnMeta();

    std::vector<int> idMapping;
    for (int index = 0; index < 6; ++index) {
        idMapping.push_back(index);
    }

    auto reader = CreateSchemalessColumnReader(columnMeta, idMapping);
    reader->SetCurrentBlock(columnData, 0);

    EXPECT_EQ(std::ssize(expected), reader->GetReadyUpperRowIndex());

    std::vector<ui32> valueCounts(expected.size());
    reader->ReadValueCounts(TMutableRange<ui32>(valueCounts.data(), valueCounts.size()));

    EXPECT_EQ(std::vector<ui32>({0, 1, 2, 3}), valueCounts);

    TChunkedMemoryPool pool;
    std::vector<TMutableUnversionedRow> actual;
    for (int valueCount : valueCounts) {
        auto row = TMutableUnversionedRow::Allocate(&pool, valueCount);
        row.SetCount(0);
        actual.push_back(row);
    }

    reader->ReadValues(TMutableRange<TMutableUnversionedRow>(actual.data(), actual.size()));
    CheckSchemafulResult(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
