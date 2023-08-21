#include "column_format_ut.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/table_chunk_format/public.h>
#include <yt/yt/ytlib/table_chunk_format/string_column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/string_column_reader.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

namespace NYT::NTableClient {
namespace {

using namespace NTableChunkFormat;

////////////////////////////////////////////////////////////////////////////////

TEST(TAnyColumnTest, Simple)
{
    TUnversionedOwningRowBuilder builder;
    std::vector<TUnversionedOwningRow> rows;

    builder.AddValue(MakeUnversionedInt64Value(-42, 0));
    rows.push_back(builder.FinishRow());

    builder.AddValue(MakeUnversionedUint64Value(777, 0));
    rows.push_back(builder.FinishRow());

    builder.AddValue(MakeUnversionedDoubleValue(0.01, 0));
    rows.push_back(builder.FinishRow());

    builder.AddValue(MakeUnversionedBooleanValue(false, 0));
    rows.push_back(builder.FinishRow());

    builder.AddValue(MakeUnversionedBooleanValue(true, 0));
    rows.push_back(builder.FinishRow());

    builder.AddValue(MakeUnversionedStringValue(TStringBuf("This is string"), 0));
    rows.push_back(builder.FinishRow());

    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 0));
    rows.push_back(builder.FinishRow());

    builder.AddValue(MakeUnversionedAnyValue(TStringBuf("{a = b}"), 0));
    rows.push_back(builder.FinishRow());

    builder.AddValue(MakeUnversionedAnyValue(TStringBuf("[]"), 0));
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
    auto columnWriter = CreateUnversionedAnyColumnWriter(0, TColumnSchema(), &blockWriter);

    columnWriter->WriteUnversionedValues(MakeRange(expected));
    columnWriter->FinishCurrentSegment();

    auto block = blockWriter.DumpBlock(0, 8);
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);

    auto columnData = codec->Compress(block.Data);
    auto columnMeta = columnWriter->ColumnMeta();

    auto reader = CreateUnversionedAnyColumnReader(columnMeta, 0, 0, std::nullopt, TColumnSchema());
    reader->SetCurrentBlock(columnData, 0);
    reader->Rearm();

    EXPECT_EQ(std::ssize(expected), reader->GetReadyUpperRowIndex());

    TChunkedMemoryPool pool;
    std::vector<TMutableUnversionedRow> actual;
    for (int i = 0; i < std::ssize(expected); ++i) {
        actual.push_back(TMutableUnversionedRow::Allocate(&pool, 1));
    }

    reader->ReadValues(TMutableRange<TMutableUnversionedRow>(actual.data(), actual.size()));
    CheckSchemafulResult(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
