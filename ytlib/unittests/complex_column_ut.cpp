#include <yt/core/test_framework/framework.h>

#include "column_format_ut.h"
#include "table_client_helpers.h"

#include <yt/ytlib/table_chunk_format/string_column_writer.h>
#include <yt/ytlib/table_chunk_format/string_column_reader.h>

#include <yt/ytlib/table_chunk_format/public.h>

#include <yt/ytlib/table_client/public.h>
#include <yt/client/table_client/unversioned_row.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TComplexColumnTest, Simple)
{
    TUnversionedOwningRowBuilder builder;
    std::vector<TUnversionedOwningRow> owningRows;

    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 0));
    owningRows.push_back(builder.FinishRow());
    builder.AddValue(MakeUnversionedCompositeValue(AsStringBuf("{a = b}"), 0));
    owningRows.push_back(builder.FinishRow());
    builder.AddValue(MakeUnversionedCompositeValue(AsStringBuf("[100]"), 0));
    owningRows.push_back(builder.FinishRow());

    std::vector<TUnversionedRow> rows;
    for (const auto& owningRow : owningRows) {
        rows.push_back(owningRow);
    }

    TDataBlockWriter blockWriter;
    auto columnWriter = CreateUnversionedComplexColumnWriter(0, &blockWriter);
    columnWriter->WriteUnversionedValues(MakeRange(rows));
    columnWriter->FinishCurrentSegment();

    auto block = blockWriter.DumpBlock(0, rows.size());
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);

    auto columnData = codec->Compress(block.Data);
    auto columnMeta = columnWriter->ColumnMeta();

    auto reader = CreateUnversionedComplexColumnReader(columnMeta, 0, 0);
    reader->ResetBlock(columnData, 0);
    EXPECT_EQ(rows.size(), reader->GetReadyUpperRowIndex());

    TChunkedMemoryPool pool;
    std::vector<TMutableUnversionedRow> actual;
    for (int i = 0; i < rows.size(); ++i) {
        actual.push_back(TMutableUnversionedRow::Allocate(&pool, 1));
    }

    reader->ReadValues(TMutableRange<TMutableUnversionedRow>(actual.data(), actual.size()));
    CheckSchemafulResult(rows, actual);
}

template <EValueType WriterType, EValueType ReaderType>
void TestCompatibility()
{
    TUnversionedOwningRowBuilder builder;
    std::vector<TUnversionedOwningRow> owningRows;

    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 0));
    owningRows.push_back(builder.FinishRow());
    builder.AddValue(MakeUnversionedStringLikeValue(WriterType, AsStringBuf("{a = b}"), 0));
    owningRows.push_back(builder.FinishRow());
    builder.AddValue(MakeUnversionedStringLikeValue(WriterType, AsStringBuf("[100]"), 0));
    owningRows.push_back(builder.FinishRow());

    std::vector<TUnversionedRow> rows;
    for (const auto& owningRow : owningRows) {
        rows.push_back(owningRow);
    }

    TDataBlockWriter blockWriter;
    std::unique_ptr<IValueColumnWriter> columnWriter;
    if constexpr (WriterType == EValueType::Composite) {
        columnWriter = CreateUnversionedComplexColumnWriter(0, &blockWriter);
    } else {
        static_assert(WriterType == EValueType::Any);
        columnWriter = CreateUnversionedAnyColumnWriter(0, &blockWriter);
    }
    columnWriter->WriteUnversionedValues(MakeRange(rows));
    columnWriter->FinishCurrentSegment();

    auto block = blockWriter.DumpBlock(0, rows.size());
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);

    auto columnData = codec->Compress(block.Data);
    auto columnMeta = columnWriter->ColumnMeta();

    std::unique_ptr<IUnversionedColumnReader> reader;
    if constexpr (ReaderType == EValueType::Composite) {
        reader = CreateUnversionedComplexColumnReader(columnMeta, 0, 0);
    } else {
        static_assert(ReaderType == EValueType::Any);
        reader = CreateUnversionedAnyColumnReader(columnMeta, 0, 0);
    }
    reader->ResetBlock(columnData, 0);
    EXPECT_EQ(rows.size(), reader->GetReadyUpperRowIndex());

    TChunkedMemoryPool pool;
    std::vector<TMutableUnversionedRow> actual;
    for (int i = 0; i < rows.size(); ++i) {
        actual.push_back(TMutableUnversionedRow::Allocate(&pool, 1));
    }

    reader->ReadValues(TMutableRange<TMutableUnversionedRow>(actual.data(), actual.size()));

    for (auto& row : actual) {
        for (auto& value : row) {
            EXPECT_TRUE(value.Type == EValueType::Null || value.Type == ReaderType) << "Reader returned value: " << ToString(value);
            if (value.Type == ReaderType) {
                value.Type = WriterType;
            }
        }
    }

    CheckSchemafulResult(rows, actual);
}

TEST(TComplexColumnTest, ComplexWriter_AnyReader_Compatibility)
{
    TestCompatibility<EValueType::Composite, EValueType::Any>();
}

TEST(TComplexColumnTest, AnyWriter_ComplexReader_Compatibility)
{
    TestCompatibility<EValueType::Any, EValueType::Composite>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
