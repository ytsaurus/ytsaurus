#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/interface/client.h>

namespace NYT {
namespace NNativeTest {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public NTest::TTest
{
public:
    void SetUp() override
    {
        TTest::SetUp();
        Client_ = CreateClient(ServerName());
        RemoveTables();
        WaitForTabletCell();
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
        TRemoveOptions options;
        options.Force(true);

        Client()->Remove(Input(), options);
    }

    void WaitForTabletCell()
    {
        while (true) {
            auto tabletCells = Client()->List("//sys/tablet_cells");
            if (!tabletCells.empty()) {
                Stroka healthPath = TStringBuilder() <<
                    "//sys/tablet_cells/" <<
                    tabletCells.front().AsString() <<
                    "/@health";

                if (Client()->Get(healthPath).AsString() == "good") {
                    return;
                }
            }
            Sleep(TDuration::MilliSeconds(500));
        }
    }

    IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TTablet, MountUnmount)
{
    TNode schema;
    schema.Add(TNode()("name", "a")("type", "int64"));

    Client()->Create(
        Input(),
        NT_TABLE,
        TCreateOptions().Attributes(TNode()
            ("dynamic", true)
            ("schema", schema))
    );

    Client()->MountTable(Input());

    Stroka tabletStatePath = TStringBuilder() <<
        //AddPathPrefix(TYPath(Input())) <<
        Input() <<
        "/@tablets/0/state";

    while (Client()->Get(tabletStatePath) != "mounted") {
        Sleep(TDuration::MilliSeconds(500));
    }

    Client()->UnmountTable(Input());

    while (Client()->Get(tabletStatePath) != "unmounted") {
        Sleep(TDuration::MilliSeconds(500));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNativeTest
} // namespace NYT

