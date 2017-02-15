#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>

#include <mapreduce/library/temptable/temptable.h>


namespace NYT {
namespace NLibraryTest {

using namespace NMR;

namespace {

class TWithBTTable
    : public NTest::TTest
{
public:
    void SetUp() override {
        TTest::SetUp();
        Server.Reset(new TServer(ServerName()));
    }

    void TearDown() override {
        TTest::TearDown();
    }

    TServer& GetServer() {
        return *Server;
    }

    THolder<TServer> Server;
};

} // anonymous namespace

YT_TEST(TWithBTTable, EmptyTable) {
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
    }
    TClient client(GetServer());
    TTable table(client, tableName);
    UNIT_ASSERT(table.IsEmpty());
}

YT_TEST(TWithBTTable, NonEmptyTable) {
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
        TClient client(GetServer());
        TUpdate update(client, tableName);
        update.AddSub("a", "a", "a");
        update.AddSub("b", "b", "b");
    }
    TClient client(GetServer());
    TTable table(client, tableName);
    UNIT_ASSERT(table.IsEmpty());
}

} // NLibraryTest
} // NYT
