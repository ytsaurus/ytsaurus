#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>


namespace NYT {
namespace NCommonTest {

using namespace NMR;

namespace {

class TFixture
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

private:
    THolder<TServer> Server;
};


void DoIteratorTest(TServer& server, const char* tableName) {
    Cout << "=======" << tableName << "=======" << Endl;
    TClient client(server);
    TTable table(client, tableName);

    {
        yvector<const char*> keys = { "a", "b", "c", "d", "e", "g", "i", "j", "aa", "bb", "0", "ee", "", "zzz" };

        for (auto& key : keys) {
#define CALL_METHOD(table, method, key) \
{   \
    auto it = table.method(key); \
    Cout << "\tKey: " << key     \
        << " Method: " #method   \
        << " IsValid: " << it.IsValid() \
        << " GetTableIndex: " << it.GetTableIndex() \
        << " GetRecordIndex: " << it.GetRecordIndex() \
        << "\n"; \
}

        CALL_METHOD(table, Find, key);
        CALL_METHOD(table, LowerBound, key);
        CALL_METHOD(table, UpperBound, key);

#undef CALL_METHOD
        }
    }

}
/*
void PrintTable(TServer& server, const char* tableName) {
    Cout << "~~~~~~" << tableName << "~~~~~~" << Endl;
    TClient client(server);
    TTable table(client, tableName);
    for (auto&& it = table.Begin(); it != table.End(); ++it) {
        Cout << it.GetKey().AsString()
            << "\t" << it.GetSubKey().AsString()
            << "\t" << it.GetValue().AsString()
            << "\n";
    }
}
*/
} // anonymous namespace

YT_TEST(TFixture, IteratorMethods) {
    const char* TABLE = "tmp/table";
    {
        TClient client(GetServer());
        TUpdate update(client, TABLE);
        update.AddSub("a", "a", "0");
        update.AddSub("c", "c", "2");
        update.AddSub("e", "e", "2");
        update.AddSub("g", "g", "2");
        update.AddSub("i", "i", "2");
    }
    const char* SORTED_TABLE = "tmp/sorted_table";
    {
        TClient client(GetServer());
        TUpdate updateSorted(client, SORTED_TABLE, UM_SORTED);
        updateSorted.AddSub("a", "a", "0");
        updateSorted.AddSub("c", "c", "2");
        updateSorted.AddSub("e", "e", "2");
        updateSorted.AddSub("g", "g", "2");
        updateSorted.AddSub("i", "i", "2");
    }
    const char* EMPTY_TABLE = "tmp/empty_table";
    {
        TClient client(GetServer());
        TUpdate update(client, EMPTY_TABLE);
    }

    const char* UNEXIST_TABLE = "tmp/unexist_table";

    //PrintTable(GetServer(), TABLE);
    //PrintTable(GetServer(), SORTED_TABLE);

    DoIteratorTest(GetServer(), TABLE);
    DoIteratorTest(GetServer(), SORTED_TABLE);
    DoIteratorTest(GetServer(), EMPTY_TABLE);
    DoIteratorTest(GetServer(), UNEXIST_TABLE);

    {
        TClient client(GetServer());
        client.Drop(TABLE);
        client.Drop(SORTED_TABLE);
        client.Drop(EMPTY_TABLE);
    }
}

} // NCommonTest
} // NYT
