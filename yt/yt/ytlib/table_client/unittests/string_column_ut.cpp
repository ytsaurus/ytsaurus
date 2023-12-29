#include "column_format_ut.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/table_chunk_format/string_column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/string_column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/private.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTableClient {
namespace {

using namespace NCompression;
using namespace NTableChunkFormat;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

const TString A("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
const TString B("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
const TString Abra("abracadabra");
const TString Bara("barakobama");
const TString Empty("");
const TString FewSymbol("abcde");

////////////////////////////////////////////////////////////////////////////////

class TUnversionedStringColumnTest
    : public TUnversionedColumnTestBase<TString>
{
protected:
    using TUnversionedColumnTestBase<TString>::CreateColumnReader;

    std::vector<std::optional<TString>> CreateDirectDense()
    {
        return  {std::nullopt, A, B};
    }

    std::vector<std::optional<TString>> CreateDictionaryDense()
    {
        return  {Abra, Bara, std::nullopt, Bara, Abra};
    }

    std::vector<std::optional<TString>> CreateDirectRle()
    {
        // 50 * [B] + null + 50 * [A]
        std::vector<std::optional<TString>> values;
        AppendVector(&values, MakeVector(50, B));
        values.push_back(std::nullopt);
        AppendVector(&values, MakeVector(50, A));
        return values;
    }

    std::vector<std::optional<TString>> CreateDictionaryRle()
    {
        // [ 50 * [A] + 50 * [B] + null ] * 10
        std::vector<std::optional<TString>> values;
        for (int i = 0; i < 10; ++i) {
            AppendVector(&values, MakeVector(50, A));
            AppendVector(&values, MakeVector(50, B));
            values.push_back(std::nullopt);
        }
        return values;
    }

    std::optional<TString> DecodeValueFromColumn(
        const IUnversionedColumnarRowBatch::TColumn* column,
        i64 index) override
    {
        YT_VERIFY(column->StartIndex >= 0);
        index += column->StartIndex;

        ResolveRleEncoding(column, index);

        if (!ResolveDictionaryEncoding(column, index)) {
            return std::nullopt;
        }

        if (IsColumnValueNull(column, index)) {
            return std::nullopt;
        }

        return TString(DecodeStringFromColumn(*column, index));
    }

    void Write(IValueColumnWriter* columnWriter) override
    {
        // Rows 0 - 2.
        WriteSegment(columnWriter, CreateDirectDense());

        // Rows 3 - 7.
        WriteSegment(columnWriter, CreateDictionaryDense());

        // Rows 8 - 108.
        WriteSegment(columnWriter, CreateDirectRle());

        // Rows 109 - 1118.
        WriteSegment(columnWriter, CreateDictionaryRle());

        // Write-only, regression test.
        columnWriter->WriteVersionedValues(MakeRange(CreateRows({Empty, Empty, Empty, Empty})));
        columnWriter->WriteVersionedValues(MakeRange(CreateRows({FewSymbol, FewSymbol, FewSymbol, FewSymbol})));
        columnWriter->FinishCurrentSegment();
    }

    std::unique_ptr<IUnversionedColumnReader> DoCreateColumnReader() override
    {
        return CreateUnversionedStringColumnReader(
            ColumnMeta_,
            ColumnIndex,
            ColumnId,
            ESortOrder::Ascending,
            TColumnSchema());
    }

    std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateUnversionedStringColumnWriter(
            ColumnIndex,
            TColumnSchema(),
            blockWriter);
    }
};

TEST_F(TUnversionedStringColumnTest, CheckSegmentTypes)
{
    EXPECT_EQ(5, ColumnMeta_.segments_size());

    auto checkSegment = [&] (EUnversionedStringSegmentType segmentType, int segmentIndex) {
        EXPECT_EQ(segmentType, FromProto<EUnversionedStringSegmentType>(ColumnMeta_.segments(segmentIndex).type()));
    };

    checkSegment(EUnversionedStringSegmentType::DirectDense, 0);
    checkSegment(EUnversionedStringSegmentType::DictionaryDense, 1);
    checkSegment(EUnversionedStringSegmentType::DirectRle, 2);
    checkSegment(EUnversionedStringSegmentType::DictionaryRle, 3);
    checkSegment(EUnversionedStringSegmentType::DictionaryDense, 4);
}

TEST_F(TUnversionedStringColumnTest, GetEqualRange)
{
    auto reader = CreateColumnReader();
    EXPECT_EQ(std::pair(4L, 5L), reader->GetEqualRange(MakeValue(Bara), 3, 5));
    EXPECT_EQ(std::pair(7L, 8L), reader->GetEqualRange(MakeValue(Abra), 7, 50));
    EXPECT_EQ(std::pair(8L, 50L), reader->GetEqualRange(MakeValue(B), 7, 50));
    EXPECT_EQ(std::pair(1118L, 1119L), reader->GetEqualRange(MakeValue(std::nullopt), 1118, 1119));
    EXPECT_EQ(std::pair(1119L, 1119L), reader->GetEqualRange(MakeValue(A), 1118, 1119));
}

TEST_F(TUnversionedStringColumnTest, ReadValues)
{
    std::vector<std::optional<TString>> expectedValues;
    AppendVector(&expectedValues, CreateDirectDense());
    AppendVector(&expectedValues, CreateDictionaryDense());
    AppendVector(&expectedValues, CreateDirectRle());
    AppendVector(&expectedValues, CreateDictionaryRle());

    ValidateRows(CreateRows(expectedValues), 5, 205);
    ValidateColumn(expectedValues, 5, 205);
}

TEST_F(TUnversionedStringColumnTest, ReadLast)
{
    auto reader = CreateColumnReader();
    reader->SkipToRowIndex(1118);

    auto actual = AllocateRows(1);
    reader->ReadValues(TMutableRange<TMutableVersionedRow>(actual.data(), actual.size()));
    EXPECT_EQ(MakeValue(std::nullopt), actual.front().Keys()[0]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
