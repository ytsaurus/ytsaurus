#include <yt/core/test_framework/framework.h>

#include "column_format_ut.h"

#include <yt/ytlib/table_chunk_format/string_column_writer.h>
#include <yt/ytlib/table_chunk_format/string_column_reader.h>
#include <yt/ytlib/table_chunk_format/private.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NTableClient;
using namespace NCompression;

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
    std::vector<TNullable<TString>> CreateDirectDense()
    {
        return  {Null, A, B};
    }

    std::vector<TNullable<TString>> CreateDictionaryDense()
    {
        return  {Abra, Bara, Null, Bara, Abra};
    }

    std::vector<TNullable<TString>> CreateDirectRle()
    {
        // 50 * [B] + Null + 50 * [A]
        std::vector<TNullable<TString>> values;
        AppendVector(&values, MakeVector(50, B));
        values.push_back(Null);
        AppendVector(&values, MakeVector(50, A));
        return values;
    }

    std::vector<TNullable<TString>> CreateDictionaryRle()
    {
        // [ 50 * [A] + 50 * [B] + Null ] * 10
        std::vector<TNullable<TString>> values;
        for (int i = 0; i < 10; ++i) {
            AppendVector(&values, MakeVector(50, A));
            AppendVector(&values, MakeVector(50, B));
            values.push_back(Null);
        }
        return values;
    }

    virtual void Write(IValueColumnWriter* columnWriter) override
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
        columnWriter->WriteValues(MakeRange(CreateRows({Empty, Empty, Empty, Empty})));
        columnWriter->WriteValues(MakeRange(CreateRows({FewSymbol, FewSymbol, FewSymbol, FewSymbol})));
        columnWriter->FinishCurrentSegment();
    }

    virtual std::unique_ptr<IUnversionedColumnReader> CreateColumnReader() override
    {
        return CreateUnversionedStringColumnReader(ColumnMeta_, ColumnIndex, ColumnId);
    }

    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateUnversionedStringColumnWriter(
            ColumnIndex,
            blockWriter);
    }
};

TEST_F(TUnversionedStringColumnTest, CheckSegmentTypes)
{
    EXPECT_EQ(5, ColumnMeta_.segments_size());

    auto checkSegment = [&] (EUnversionedStringSegmentType segmentType, int segmentIndex) {
        EXPECT_EQ(segmentType, EUnversionedStringSegmentType(ColumnMeta_.segments(segmentIndex).type()));
    };

    checkSegment(EUnversionedStringSegmentType::DirectDense, 0);
    checkSegment(EUnversionedStringSegmentType::DictionaryDense, 1);
    checkSegment(EUnversionedStringSegmentType::DirectRle, 2);
    checkSegment(EUnversionedStringSegmentType::DictionaryRle, 3);
    checkSegment(EUnversionedStringSegmentType::DictionaryDense, 4);
}

TEST_F(TUnversionedStringColumnTest, GetEqualRange)
{
    EXPECT_EQ(std::make_pair(4L, 5L), Reader_->GetEqualRange(MakeValue(Bara), 3, 5));
    EXPECT_EQ(std::make_pair(7L, 8L), Reader_->GetEqualRange(MakeValue(Abra), 7, 50));
    EXPECT_EQ(std::make_pair(8L, 50L), Reader_->GetEqualRange(MakeValue(B), 7, 50));

    EXPECT_EQ(std::make_pair(1118L, 1119L), Reader_->GetEqualRange(MakeValue(Null), 1118, 1119));
    EXPECT_EQ(std::make_pair(1119L, 1119L), Reader_->GetEqualRange(MakeValue(A), 1118, 1119));
}

TEST_F(TUnversionedStringColumnTest, ReadValues)
{
    std::vector<TNullable<TString>> expectedValues;
    AppendVector(&expectedValues, CreateDirectDense());
    AppendVector(&expectedValues, CreateDictionaryDense());
    AppendVector(&expectedValues, CreateDirectRle());
    AppendVector(&expectedValues, CreateDictionaryRle());

    Validate(CreateRows(expectedValues), 5, 205);
}

TEST_F(TUnversionedStringColumnTest, ReadLast)
{
    Reader_->SkipToRowIndex(1118);

    auto actual = AllocateRows(1);
    Reader_->ReadValues(TMutableRange<TMutableVersionedRow>(actual.data(), actual.size()));
    EXPECT_EQ(MakeValue(Null), *actual.front().BeginKeys());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
