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

    void CreateDynamicTable()
    {
        TNode schema;
        schema.Add(TNode()("name", "a")("type", "int64")("sort_order", "ascending"));
        schema.Add(TNode()("name", "b")("type", "int64"));

        Client()->Create(
            Input(),
            NT_TABLE,
            TCreateOptions().Attributes(TNode()
                ("dynamic", true)
                ("schema", schema))
        );
    }

    void WaitForTabletState(const TYPath& path, const char* state)
    {
        while (Client()->Get(path) != state) {
            Sleep(TDuration::MilliSeconds(500));
        }
    }

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
    CreateDynamicTable();

    Stroka tabletStatePath = TStringBuilder() << Input() << "/@tablets/0/state";

    Client()->MountTable(Input());
    WaitForTabletState(tabletStatePath, "mounted");

    Client()->RemountTable(Input());
    WaitForTabletState(tabletStatePath, "mounted");

    Client()->UnmountTable(Input());
    WaitForTabletState(tabletStatePath, "unmounted");
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TTablet, Reshard)
{
    CreateDynamicTable();

    Stroka tabletStatePath = TStringBuilder() << Input() << "/@tablets/0/state";

    Client()->MountTable(Input());
    WaitForTabletState(tabletStatePath, "mounted");

    TNode::TList rows;
    for (int i = 0; i < 16; ++i) {
        rows.push_back(TNode()("a", i));
    }

    Client()->InsertRows(Input(), rows);

    Client()->UnmountTable(Input());
    WaitForTabletState(tabletStatePath, "unmounted");

    yvector<TKey> pivotKeys;
    pivotKeys.push_back(TKey());
    pivotKeys.push_back(4);
    pivotKeys.push_back(8);
    pivotKeys.push_back(12);

    Client()->ReshardTable(Input(), pivotKeys);

    Stroka tabletsPath = TStringBuilder() << Input() << "/@tablets";
    EXPECT_EQ(Client()->Get(tabletsPath).Size(), 4);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNativeTest
} // namespace NYT

