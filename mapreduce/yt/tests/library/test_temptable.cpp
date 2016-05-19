#include <mapreduce/yt/tests/lib/lib.h>
#include <mapreduce/yt/tests/operations/fail_map.h>
#include <mapreduce/yt/tests/operations/id_map.h>
#include <mapreduce/yt/tests/util/table_printer.h>

#include <mapreduce/interface/all.h>

#include <mapreduce/library/temptable/temptable.h>


namespace NYT {
namespace NLibraryTest {

using namespace NMR;
using namespace NTestOps;
using namespace NTestUtil;

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
    Cout << "====Empty table====" << Endl;
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTable, NonEmptyTable) {
    Cout << "====Non-empty table====" << Endl;
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

YT_TEST(TWithBTTable, NonEmptyTableException) {
    Cout << "====Non-empty table with exception====" << Endl;
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

YT_TEST(TWithBTTable, NonFailedOpEmptyTable) {
    static constexpr auto OUT_TABLE = "temptable_test_out_table_NonFailedOpEmptyTable";
    Cout << "====Non-failed map with empty table====" << Endl;
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
        GetServer().Map(tableName, OUT_TABLE, new TIdMap);
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTable, NonFailedOpNonEmptyTable) {
    static constexpr auto OUT_TABLE = "temptable_test_out_table_NonFailedOpNonEmptyTable";
    Cout << "====Non-failed map with non-empty table====" << Endl;
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
        GetServer().Map(tableName, OUT_TABLE, new TIdMap);
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTable, FailedOpEmptyTable) {
    static constexpr auto OUT_TABLE = "temptable_test_out_table_FailedOpEmptyTable";
    Cout << "====Failed map with empty table====" << Endl;
    Stroka tableName;
    {
        WithUniqBTTable t(GetServer(), "tmp/");
        tableName = t.Name();
        GetServer().Map(tableName, OUT_TABLE, new TFailMap);
    }
    PrintTable(GetServer(), ~tableName, Cout);
}

YT_TEST(TWithBTTable, FailedOpNonEmptyTable) {
    static constexpr auto OUT_TABLE = "temptable_test_out_table_FailedOpNonEmptyTable";
    Cout << "====Failed map with non-empty table====" << Endl;
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

} // NLibraryTest
} // NYT
