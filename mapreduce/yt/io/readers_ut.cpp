#include <mapreduce/yt/io/node_table_reader.h>
#include <mapreduce/yt/io/proto_table_reader.h>
#include <mapreduce/yt/io/skiff_table_reader.h>
#include <mapreduce/yt/io/stream_table_reader.h>
#include <mapreduce/yt/io/yamr_table_reader.h>

#include <mapreduce/yt/skiff/checked_parser.h>
#include <mapreduce/yt/skiff/skiff_schema.h>

#include <library/cpp/yson/node/node_io.h>

#include <mapreduce/yt/io/ut_row.pb.h>

#include <library/cpp/unittest/registar.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NSkiff;

////////////////////////////////////////////////////////////////////

class TRetryEmulatingRawTableReader
    : public TRawTableReader
{
public:
    TRetryEmulatingRawTableReader(const TString& string)
        : String_(string)
        , Stream_(String_)
    { }

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&) override
    {
        if (RetriesLeft_ == 0) {
            return false;
        }
        Stream_ = TStringStream(String_);
        --RetriesLeft_;
        return true;
    }

    void ResetRetries() override
    {
        RetriesLeft_ = 10;
    }

    bool HasRangeIndices() const override
    {
        return false;
    }

private:
    size_t DoRead(void* buf, size_t len) override
    {
        switch (DoReadCallCount_++) {
            case 0:
                return Stream_.Read(buf, std::min(len, String_.size() / 2));
            case 1:
                ythrow yexception() << "Just wanted to test you";
            default:
                return Stream_.Read(buf, len);
        }
    }

private:
    const TString String_;
    TStringStream Stream_;
    int RetriesLeft_ = 10;
    int DoReadCallCount_ = 0;
};

////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(Readers)
{
    Y_UNIT_TEST(YsonGood)
    {
        auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>("{a=13;b = \"string\"}; {c = {d=12}}");

        TNodeTableReader reader(proxy);
        TVector<TNode> expectedRows = {TNode()("a", 13)("b", "string"), TNode()("c", TNode()("d", 12))};
        for (const auto& expectedRow : expectedRows) {
            UNIT_ASSERT(reader.IsValid());
            UNIT_ASSERT_EQUAL(reader.GetRow(), expectedRow);
            reader.Next();
        }
        UNIT_ASSERT(!reader.IsValid());
    }

    Y_UNIT_TEST(YsonBad)
    {
        auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>("{a=13;-b := \"string\"}; {c = {d=12}}");
        UNIT_ASSERT_EXCEPTION(TNodeTableReader(proxy).GetRow(), yexception);
    }

    Y_UNIT_TEST(SkiffGood)
    {
        const char arr[] = "\x00\x00" "\x94\x88\x01\x00\x00\x00\x00\x00" "\x06\x00\x00\x00""foobar" "\x01"
                           "\x00\x00" "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF" "\x03\x00\x00\x00""abc"    "\x00";
        auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

        TSkiffSchemaPtr schema = CreateVariant16Schema({
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::Int64)->SetName("a"),
                CreateSimpleTypeSchema(EWireType::String32)->SetName("b"),
                CreateSimpleTypeSchema(EWireType::Boolean)->SetName("c")
            })
        });

        TSkiffTableReader reader(proxy, schema);
        TVector<TNode> expectedRows = {
            TNode()("a", 100500)("b", "foobar")("c", true),
            TNode()("a", -1)("b", "abc")("c", false),
        };
        for (const auto& expectedRow : expectedRows) {
            UNIT_ASSERT(reader.IsValid());
            UNIT_ASSERT_EQUAL(reader.GetRow(), expectedRow);
            reader.Next();
        }
        UNIT_ASSERT(!reader.IsValid());
    }

    Y_UNIT_TEST(SkiffBad)
    {
        const char arr[] = "\x00\x00" "\x94\x88\x01\x00\x00\x00\x00\x00" "\xFF\x00\x00\x00""foobar" "\x01";
        auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

        TSkiffSchemaPtr schema = CreateVariant16Schema({
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::Int64)->SetName("a"),
                CreateSimpleTypeSchema(EWireType::String32)->SetName("b"),
                CreateSimpleTypeSchema(EWireType::Boolean)->SetName("c")
            })
        });

        UNIT_ASSERT_EXCEPTION(TSkiffTableReader(proxy, schema).GetRow(), yexception);
    }

    Y_UNIT_TEST(ProtobufGood)
    {
        using NTesting::TRow;

        const char arr[] = "\x13\x00\x00\x00" "\x0A""\x06""foobar" "\x10""\x0F" "\x19""\x94\x88\x01\x00\x00\x00\x00\x00"
                           "\x10\x00\x00\x00" "\x0A""\x03""abc"    "\x10""\x1F" "\x19""\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
        auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

        TLenvalProtoTableReader reader(proxy, {TRow::descriptor()});
        TRow row1, row2;
        row1.SetString("foobar");
        row1.SetInt32(15);
        row1.SetFixed64(100500);

        row2.SetString("abc");
        row2.SetInt32(31);
        row2.SetFixed64(-1);

        TVector<TRow> expectedRows = {row1, row2};
        for (const auto& expectedRow : expectedRows) {
            TRow row;
            UNIT_ASSERT(reader.IsValid());
            reader.ReadRow(&row);
            UNIT_ASSERT_VALUES_EQUAL(row.GetString(), expectedRow.GetString());
            UNIT_ASSERT_VALUES_EQUAL(row.GetInt32(), expectedRow.GetInt32());
            UNIT_ASSERT_VALUES_EQUAL(row.GetFixed64(), expectedRow.GetFixed64());
            reader.Next();
        }
        UNIT_ASSERT(!reader.IsValid());
    }

    Y_UNIT_TEST(ProtobufBad)
    {
        const char arr[] = "\x13\x00\x00\x00" "\x0F""\x06""foobar" "\x10""\x0F" "\x19""\x94\x88\x01\x00\x00\x00\x00\x00";
        auto proxy = ::MakeIntrusive<TRetryEmulatingRawTableReader>(TString(arr, sizeof(arr) - 1));

        NTesting::TRow row;
        UNIT_ASSERT_EXCEPTION(TLenvalProtoTableReader(proxy, {NTesting::TRow::descriptor()}).ReadRow(&row), yexception);
    }
}

////////////////////////////////////////////////////////////////////
