#include <mapreduce/yt/client/skiff.h>

#include <mapreduce/yt/interface/skiff_row.h>

#include <mapreduce/yt/io/skiff_row_table_reader.h>

#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

/////////////////////////////////////////////////////////////////////////////////////////

struct TTestSkiffRow
{
    int Num = 0;
    TString Str;
    std::optional<double> PointNum;

    TTestSkiffRow() = default;

    TTestSkiffRow(int num, const TString& str, const std::optional<double>& pointNum)
        : Num(num)
        , Str(str)
        , PointNum(pointNum)
    { }

    bool operator==(const TTestSkiffRow& other) const
    {
        return Num == other.Num && Str == other.Str && PointNum == other.PointNum;
    }
};

IOutputStream& operator<<(IOutputStream& ss, const TTestSkiffRow& row)
{
    ss << "{ Num: " << row.Num
        << ", Str: '" << row.Str
        << "', PointNum: " << (row.PointNum ? std::to_string(*row.PointNum) : "nullopt") << " }";
    return ss;
}

template <>
struct TIsSkiffRow<TTestSkiffRow>
    : std::true_type
{ };

class TTestSkiffRowParser
    : public ISkiffRowParser
{
public:
    TTestSkiffRowParser(TTestSkiffRow* row)
        : Row_(row)
    { }

    virtual ~TTestSkiffRowParser() override = default;

    virtual void Parse(NSkiff::TCheckedInDebugSkiffParser* parser) override {
        Row_->Num = parser->ParseInt64();
        Row_->Str = parser->ParseString32();

        auto tag = parser->ParseVariant8Tag();
        if (tag == 1) {
            Row_->PointNum = parser->ParseDouble();
        } else {
            Row_->PointNum = std::nullopt;
            Y_ENSURE(tag == 0, "tag value must be equal 0 or 1");
        }
    }

private:
    TTestSkiffRow* Row_;
};

template <>
ISkiffRowParserPtr NYT::CreateSkiffParser<TTestSkiffRow>(TTestSkiffRow* row)
{
    return ::MakeIntrusive<TTestSkiffRowParser>(row);
}

template <>
NSkiff::TSkiffSchemaPtr NYT::GetSkiffSchema<TTestSkiffRow>()
{
    return NSkiff::CreateTupleSchema({
        NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Int64)->SetName("num"),
        NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::String32)->SetName("str"),
        NSkiff::CreateVariant8Schema({
            CreateSimpleTypeSchema(NSkiff::EWireType::Nothing),
            CreateSimpleTypeSchema(NSkiff::EWireType::Double)})
        ->SetName("pointNum")});
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TBadSkiffRow
{
    TString Value;

    bool operator==(const TBadSkiffRow& other) const
    {
        return Value == other.Value;
    }
};

template <>
struct TIsSkiffRow<TBadSkiffRow>
    : std::true_type
{ };

class TBadSkiffRowParser
    : public ISkiffRowParser
{
public:
    TBadSkiffRowParser(TBadSkiffRow* row)
        : Row_(row)
    { }

    virtual ~TBadSkiffRowParser() override = default;

    virtual void Parse(NSkiff::TCheckedInDebugSkiffParser* parser) override {
        Row_->Value = parser->ParseString32();
    }

private:
    TBadSkiffRow* Row_;
};

template <>
ISkiffRowParserPtr NYT::CreateSkiffParser<TBadSkiffRow>(TBadSkiffRow* row)
{
    return ::MakeIntrusive<TBadSkiffRowParser>(row);
}

template <>
NSkiff::TSkiffSchemaPtr NYT::GetSkiffSchema<TBadSkiffRow>()
{
    return NSkiff::CreateTupleSchema({
        NSkiff::CreateSimpleTypeSchema(NSkiff::EWireType::Int32)->SetName("value")
    });
}

/////////////////////////////////////////////////////////////////////////////////////////

namespace {

void Write(TTestFixture& fixture, const TString& path, const TVector<TNode>& rows)
{
    auto writer = fixture.GetClient()->CreateTableWriter<TNode>(path);
    for (const auto& row : rows) {
        writer->AddRow(row);
    };
    writer->Finish();
}

TVector<TTestSkiffRow> WriteSimpleTable(TTestFixture& fixture, const TString& path)
{
    TVector<TNode> rows;
    constexpr int rowCount = 100;
    rows.reserve(rowCount);
    TVector<TTestSkiffRow> expectedRows;
    expectedRows.reserve(rowCount);
    for (int i = 0; i < rowCount; ++i) {
        auto node = TNode()("num", i)("str", ToString(i * i));
        if (i % 2 == 0) {
            rows.push_back(node("pointNum", i + 0.1));
            expectedRows.push_back({i, ToString(i * i), i + 0.1});
        } else {
            rows.push_back(node);
            expectedRows.push_back({i, ToString(i * i), std::nullopt});
        }
    }

    Write(fixture, path, rows);

    return expectedRows;
}

} // namespace

/////////////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(SkiffRowTest) {
    Y_UNIT_TEST(ReadingSimpleBasic) {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto path = fixture.GetWorkingDir() + "/table";
        TVector<TTestSkiffRow> expectedRows = WriteSimpleTable(fixture, path);

        auto reader = client->CreateTableReader<TTestSkiffRow>(path);
        TVector<TTestSkiffRow> gotRows;

        ui64 expectedRowIndex = 0;
        for (; reader->IsValid(); reader->Next(), ++expectedRowIndex) {
            UNIT_ASSERT(!reader->IsRawReaderExhausted());
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRowIndex(), expectedRowIndex);
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), 0);
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRangeIndex(), 0);

            gotRows.push_back(reader->MoveRow());
        }
        UNIT_ASSERT(reader->IsRawReaderExhausted());

        for (size_t i = 0; i < expectedRows.size(); ++i) {
            const auto& expectedRow = expectedRows[i];
            const auto& gotRow = gotRows[i];
            UNIT_ASSERT_VALUES_EQUAL(gotRow, expectedRow);
        }
    }

    Y_UNIT_TEST(ReadingSimpleSkipRow) {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto path = fixture.GetWorkingDir() + "/table";
        TVector<TTestSkiffRow> writtenRows = WriteSimpleTable(fixture, path);

        auto reader = client->CreateTableReader<TTestSkiffRow>(path);
        TVector<TTestSkiffRow> gotRows;

        ui64 expectedRowIndex = 0;
        for (; reader->IsValid(); reader->Next(), expectedRowIndex += 2) {
            UNIT_ASSERT(!reader->IsRawReaderExhausted());
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRowIndex(), expectedRowIndex);
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), 0);
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRangeIndex(), 0);

            gotRows.push_back(reader->MoveRow());
            reader->Next();  // additional `Next` for skipping one row
        }
        UNIT_ASSERT(reader->IsRawReaderExhausted());

        for (size_t i = 0; i < writtenRows.size(); i += 2) {
            const auto& expectedRow = writtenRows[i];
            const auto& gotRow = gotRows[i / 2];

            UNIT_ASSERT_VALUES_EQUAL(gotRow, expectedRow); 
        }
    }

    Y_UNIT_TEST(ReadingTableWithRanges) {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto path = fixture.GetWorkingDir() + "/table1";
        TVector<TTestSkiffRow> writtenRows = WriteSimpleTable(fixture, path);

        auto reader = client->CreateTableReader<TTestSkiffRow>(TRichYPath(path)
            .AddRange(TReadRange::FromRowIndices(0, 10))
            .AddRange(TReadRange::FromRowIndices(40, 50))
            .AddRange(TReadRange::FromRowIndices(90, 100)));
        TVector<TTestSkiffRow> gotRows;

        TVector<ui64> expectedRowIndices;
        expectedRowIndices.reserve(30);
        for (ui64 i = 0; i < 100; ++i) {
            if (i < 10 || (i >= 40 && i < 50) || i >= 90) {
                expectedRowIndices.push_back(i);
            }
        }

        size_t currentRowShift = 0;
        for (; reader->IsValid(); reader->Next(), ++currentRowShift) {
            UNIT_ASSERT(!reader->IsRawReaderExhausted());
            UNIT_ASSERT_VALUES_EQUAL(reader->GetTableIndex(), 0);

            ui64 expectedRowIndex = expectedRowIndices[currentRowShift];
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRowIndex(), expectedRowIndex);

            ui32 expectedRangeIndex = 0;
            if (expectedRowIndex >= 40 && expectedRowIndex < 50) {
                expectedRangeIndex = 1;
            } else if (expectedRowIndex >= 90) {
                expectedRangeIndex = 2;
            }
            UNIT_ASSERT_VALUES_EQUAL(reader->GetRangeIndex(), expectedRangeIndex);

            gotRows.push_back(reader->MoveRow());
        }
        UNIT_ASSERT(reader->IsRawReaderExhausted());

        size_t currentGotRowIndex = 0;
        for (auto rowIndex : expectedRowIndices) {
            const auto& expectedRow = writtenRows[rowIndex];
            const auto& gotRow  = gotRows[currentGotRowIndex++];
            UNIT_ASSERT_VALUES_EQUAL(gotRow, expectedRow);
        }
    }

    Y_UNIT_TEST(ReadingWithIncorrectParser) {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto path = fixture.GetWorkingDir() + "/table";

        auto writer = fixture.GetClient()->CreateTableWriter<TNode>(path);
        writer->AddRow(TNode()("value", 123));
        writer->Finish();

        auto reader = client->CreateTableReader<TBadSkiffRow>(path);
        UNIT_ASSERT(reader->IsValid());
        UNIT_ASSERT(!reader->IsRawReaderExhausted());

        UNIT_ASSERT_EXCEPTION_SATISFIES(reader->GetRow(), yexception, [](const yexception& ex) {
            return TString(ex.what()).Contains("Premature end of stream while parsing Skiff");
        });
    }
}

/////////////////////////////////////////////////////////////////////////////////////////