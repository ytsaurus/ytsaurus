#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/interface/client.h>

#include <mapreduce/yt/http/error.h>

#include <util/random/fast.h>

namespace NYT {
namespace NNativeTest {

////////////////////////////////////////////////////////////////////////////////

class TIo
    : public NTest::TTest
{
public:
    void SetUp() override
    {
        TTest::SetUp();
        Client_ = CreateClient(ServerName());
        RemoveTables();
    }

    void TearDown() override
    {
        RemoveTables();
        TTest::TearDown();
    }

    IClientPtr Client() { return Client_; }
    const char* Input() { return "tmp/input"; }

private:
    void RemoveTables()
    {
        Client()->Remove(Input(), TRemoveOptions().Force(true));
    }

    IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TIo, ReadMultipleRangesNode)
{
    auto writer = Client()->CreateTableWriter<TNode>(
        TRichYPath(Input()).SortedBy("key"));

    for (int i = 0; i < 100; ++i) {
        writer->AddRow(TNode()("key", i));
    }
    writer->Finish();

    TRichYPath path(Input());
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(10))
        .UpperLimit(TReadLimit().RowIndex(20)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().Key(30))
        .UpperLimit(TReadLimit().Key(40)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(50))
        .UpperLimit(TReadLimit().RowIndex(60)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().Key(70))
        .UpperLimit(TReadLimit().Key(80)));
    path.AddRange(TReadRange()
        .Exact(TReadLimit().RowIndex(90)));
    path.AddRange(TReadRange()
        .Exact(TReadLimit().Key(95)));

    auto reader = Client()->CreateTableReader<TNode>(path);
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout << row["key"].AsInt64() << ", " << reader->GetRowIndex() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TIo, ReadMultipleRangesYaMR)
{
    auto writer = Client()->CreateTableWriter<TYaMRRow>(
        TRichYPath(Input()).SortedBy("key"));

    for (int i = 0; i < 100; ++i) {
        auto key = Sprintf("%02d", i);
        writer->AddRow({key, TStringBuf(), TStringBuf()});
    }
    writer->Finish();

    TRichYPath path(Input());
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(10))
        .UpperLimit(TReadLimit().RowIndex(20)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().Key("30"))
        .UpperLimit(TReadLimit().Key("40")));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(50))
        .UpperLimit(TReadLimit().RowIndex(60)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().Key("70"))
        .UpperLimit(TReadLimit().Key("80")));
    path.AddRange(TReadRange()
        .Exact(TReadLimit().RowIndex(90)));
    path.AddRange(TReadRange()
        .Exact(TReadLimit().Key("95")));

    auto reader = Client()->CreateTableReader<TYaMRRow>(path);
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout << row.Key << ", " << reader->GetRowIndex() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka RandomBytes() {
    static TReallyFastRng32 RNG(42);
    ui64 value = RNG.GenRand64();
    return Stroka((const char*)&value, sizeof(value));
}

} // namespace

YT_TEST(TIo, ReadErrorInTrailers)
{
    {
        auto writer = Client()->CreateTableWriter<NYT::TNode>(Input());
        for (int i = 0; i != 10000; ++i) {
            NYT::TNode node;
            node["key"] = RandomBytes();
            node["subkey"] = RandomBytes();
            node["value"] = RandomBytes();
            writer->AddRow(node);
        }
        NYT::TNode brokenNode;
        brokenNode["not_a_yamr_key"] = "ПЫЩ";
        writer->AddRow(brokenNode);
    }

    auto reader = Client()->CreateTableReader<NYT::TYaMRRow>(Input());

    // we expect first record to be read ok and error will come only later
    // in http trailer
    ASSERT_TRUE(reader->IsValid());

    auto readRemaining = [&]() {
        for (; reader->IsValid() ; reader->Next()) {
        }
    };
    ASSERT_THROW(readRemaining(), NYT::TErrorResponse);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TIo, ReadUncanonicalPath)
{
    auto writer = Client()->CreateTableWriter<TNode>(
        TRichYPath(Input()).SortedBy("key"));

    for (int i = 0; i < 100; ++i) {
        writer->AddRow(TNode()("key", i));
    }
    writer->Finish();

    TRichYPath path(Input() + Stroka("[#10:#20,30:40,#50:#60,70:80,#90,95]"));

    auto reader = Client()->CreateTableReader<TNode>(path);
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout << row["key"].AsInt64() << ", " << reader->GetRowIndex() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTest
} // namespace NYT

