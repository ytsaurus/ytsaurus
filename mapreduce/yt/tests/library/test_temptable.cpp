#include <mapreduce/yt/tests/lib/lib.h>
#include <mapreduce/yt/tests/util/helpers.h>

#include <mapreduce/interface/all.h>

#include <mapreduce/library/temptable/temptable.h>


namespace NYT {
namespace NCommonTest {

using namespace NMR;

namespace {


class TWithBTTableTestFixture
    : public NTest::TTest
{
public:
    static constexpr auto OUT_TABLE = "temptable_test_out_table";

    void SetUp() override {
        TTest::SetUp();
        Server.Reset(new TServer(ServerName()));
    }

    void TearDown() override {
        DropTables();
        TTest::TearDown();
    }

    TServer& GetServer() {
        return *Server;
    }

    void DropTables() {
        TClient client(GetServer());
        client.Drop(OUT_TABLE);
    }

    THolder<TServer> Server;
};

class TCopyMap
    : public IMap
{
    OBJECT_METHODS(TCopyMap);

public:
    void DoSub(TValue k, TValue s, TValue v, TUpdate& update) override {
        update.AddSub(k, s, v);
    }
};


class TFailMap
    : public IMap
{
    OBJECT_METHODS(TFailMap);

public:
    void DoSub(TValue, TValue, TValue, TUpdate&) override {
        throw yexception();
    }
};

} // anonymous namespace

YT_TEST(TWithBTTableTestFixture, SimpleTestEmptyTable) {
    Cout << "====Empty table====\n";
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTableTestFixture, SimpleTestNonEmptyTable) {
    Cout << "====Non-empty table====\n";
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
        TClient client(GetServer());
        TUpdate update(client, tableName);
        update.AddSub("a", "a", "a");
        update.AddSub("b", "b", "b");
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTableTestFixture, SimpleTestNonEmptyTableException) {
    Cout << "====Non-empty table with exception====\n";
    Stroka tableName;
    try {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
        TClient client(GetServer());
        TUpdate update(client, tableName);
        update.AddSub("a", "a", "a");
        throw yexception();
        update.AddSub("b", "b", "b");
    } catch (const yexception& /* ex */) {
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTableTestFixture, NonFailedOpEmptyTableTest) {
    Cout << "====Non-failed map with empty table====\n";
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
        GetServer().Map(tableName, OUT_TABLE, new TCopyMap);
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTableTestFixture, NonFailedOpNonEmptyTableTest) {
    Cout << "====Non-failed map with non-empty table====\n";
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
        {
            TClient client(GetServer());
            TUpdate update(client, tableName);
            update.AddSub("a", "a", "a");
            update.AddSub("b", "b", "b");
        }
        GetServer().Map(tableName, OUT_TABLE, new TCopyMap);
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTableTestFixture, FailedOpEmptyTableTest) {
    Cout << "====Failed map with empty table====\n";
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
        GetServer().Map(tableName, OUT_TABLE, new TFailMap);
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTableTestFixture, FailedOpNonEmptyTableTest) {
    Cout << "====Failed map with non-empty table====\n";
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
        {
            TClient client(GetServer());
            TUpdate update(client, tableName);
            update.AddSub("a", "a", "a");
            update.AddSub("b", "b", "b");
        }
        GetServer().Map(tableName, OUT_TABLE, new TFailMap);
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

} // NCommonTest
} // NYT


using namespace NYT::NCommonTest;
REGISTER_SAVELOAD_CLASS(0x00000002, TCopyMap);
REGISTER_SAVELOAD_CLASS(0x00000003, TFailMap);
