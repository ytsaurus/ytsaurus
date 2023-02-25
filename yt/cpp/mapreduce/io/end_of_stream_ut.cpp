#include <yt/cpp/mapreduce/io/node_table_reader.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;

////////////////////////////////////////////////////////////////////

class TStringRawTableReader
    : public TRawTableReader
{
public:
    TStringRawTableReader(const TString& string)
        : String_(string)
        , Stream_(String_)
    { }

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&) override
    {
        return false;
    }

    void ResetRetries() override
    { }

    bool HasRangeIndices() const override
    {
        return false;
    }

private:
    size_t DoRead(void* buf, size_t len) override
    {
        return Stream_.Read(buf, len);
    }

private:
    const TString String_;
    TStringStream Stream_;
};

////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(EndOfStream)
{
    void DoTest(bool addEos)
    {
        auto proxy = ::MakeIntrusive<TStringRawTableReader>(TString::Join(
            "{a=13;b = \"string\"}; {c = {d=12}};",
            "<key_switch=%true>#; {e = 42};",
            addEos ? "<end_of_stream=%true>#" : ""
        ));

        TNodeTableReader reader(proxy);
        TVector<TNode> expectedRows = {TNode()("a", 13)("b", "string"), TNode()("c", TNode()("d", 12))};
        for (const auto& expectedRow : expectedRows) {
            UNIT_ASSERT(reader.IsValid());
            UNIT_ASSERT(!reader.IsEndOfStream());
            UNIT_ASSERT(!reader.IsRawReaderExhausted());
            UNIT_ASSERT_EQUAL(reader.GetRow(), expectedRow);
            reader.Next();
        }

        UNIT_ASSERT(!reader.IsValid());
        UNIT_ASSERT(!reader.IsEndOfStream());
        UNIT_ASSERT(!reader.IsRawReaderExhausted());

        reader.NextKey();
        reader.Next();
        expectedRows = {TNode()("e", 42)};
        for (const auto& expectedRow : expectedRows) {
            UNIT_ASSERT(reader.IsValid());
            UNIT_ASSERT(!reader.IsEndOfStream());
            UNIT_ASSERT(!reader.IsRawReaderExhausted());
            UNIT_ASSERT_EQUAL(reader.GetRow(), expectedRow);
            reader.Next();
        }

        UNIT_ASSERT(!reader.IsValid());
        if (addEos) {
            UNIT_ASSERT(reader.IsEndOfStream());
        } else {
            UNIT_ASSERT(!reader.IsEndOfStream());
        }
        UNIT_ASSERT(reader.IsRawReaderExhausted());
    }

    Y_UNIT_TEST(YsonWithEos) {
        DoTest(true);
    }

    Y_UNIT_TEST(YsonWithoutEos) {
        DoTest(false);
    }

}

////////////////////////////////////////////////////////////////////
