#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/interface/client.h>

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

    auto reader = Client()->CreateTableReader<TYaMRRow>(path);
    for (; reader->IsValid(); reader->Next()) {
        const auto& row = reader->GetRow();
        Cout << row.Key << ", " << reader->GetRowIndex() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTest
} // namespace NYT

