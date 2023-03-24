#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/table_chunk_format/string_column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/string_column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT::NTableClient {
namespace {

using namespace NTableChunkFormat;

////////////////////////////////////////////////////////////////////////////////

TEST(TComplexColumnTest, Simple)
{
    TUnversionedOwningRowBuilder builder;
    std::vector<TUnversionedOwningRow> owningRows;

    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 0));
    owningRows.push_back(builder.FinishRow());
    builder.AddValue(MakeUnversionedCompositeValue(TStringBuf("{a = b}"), 0));
    owningRows.push_back(builder.FinishRow());
    builder.AddValue(MakeUnversionedCompositeValue(TStringBuf("[100]"), 0));
    owningRows.push_back(builder.FinishRow());

    std::vector<TUnversionedRow> rows;
    for (const auto& owningRow : owningRows) {
        rows.push_back(owningRow);
    }

    TDataBlockWriter blockWriter;
    auto columnWriter = CreateUnversionedCompositeColumnWriter(0, TColumnSchema(), &blockWriter);
    columnWriter->WriteUnversionedValues(MakeRange(rows));
    columnWriter->FinishCurrentSegment();

    auto block = blockWriter.DumpBlock(0, rows.size());
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);

    auto columnData = codec->Compress(block.Data);
    auto columnMeta = columnWriter->ColumnMeta();

    auto reader = CreateUnversionedCompositeColumnReader(columnMeta, 0, 0, std::nullopt, TColumnSchema());
    reader->SetCurrentBlock(columnData, 0);
    reader->Rearm();
    EXPECT_EQ(std::ssize(rows), reader->GetReadyUpperRowIndex());

    TChunkedMemoryPool pool;
    std::vector<TMutableUnversionedRow> actual;
    for (int i = 0; i < std::ssize(rows); ++i) {
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
    builder.AddValue(MakeUnversionedStringLikeValue(WriterType, TStringBuf("{a = b}"), 0));
    owningRows.push_back(builder.FinishRow());
    builder.AddValue(MakeUnversionedStringLikeValue(WriterType, TStringBuf("[100]"), 0));
    owningRows.push_back(builder.FinishRow());

    std::vector<TUnversionedRow> rows;
    for (const auto& owningRow : owningRows) {
        rows.push_back(owningRow);
    }

    TDataBlockWriter blockWriter;
    std::unique_ptr<IValueColumnWriter> columnWriter;
    if constexpr (WriterType == EValueType::Composite) {
        columnWriter = CreateUnversionedCompositeColumnWriter(0, TColumnSchema(), &blockWriter);
    } else {
        static_assert(WriterType == EValueType::Any);
        columnWriter = CreateUnversionedAnyColumnWriter(0, TColumnSchema(), &blockWriter);
    }
    columnWriter->WriteUnversionedValues(MakeRange(rows));
    columnWriter->FinishCurrentSegment();

    auto block = blockWriter.DumpBlock(0, rows.size());
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);

    auto columnData = codec->Compress(block.Data);
    auto columnMeta = columnWriter->ColumnMeta();

    std::unique_ptr<IUnversionedColumnReader> reader;
    if constexpr (ReaderType == EValueType::Composite) {
        reader = CreateUnversionedCompositeColumnReader(columnMeta, 0, 0, std::nullopt, TColumnSchema());
    } else {
        static_assert(ReaderType == EValueType::Any);
        reader = CreateUnversionedAnyColumnReader(columnMeta, 0, 0, std::nullopt, TColumnSchema());
    }
    reader->SetCurrentBlock(columnData, 0);
    reader->Rearm();
    EXPECT_EQ(std::ssize(rows), reader->GetReadyUpperRowIndex());

    TChunkedMemoryPool pool;
    std::vector<TMutableUnversionedRow> actual;
    for (int i = 0; i < std::ssize(rows); ++i) {
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

    // CheckSchemafulResult(rows, actual);
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

} // namespace
} // namespace NYT::NTableClient
