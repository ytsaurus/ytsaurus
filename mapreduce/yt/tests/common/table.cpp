#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>


namespace NYT {
namespace NCommonTest {

using namespace NMR;

namespace {

class TTableFixture
    : public NTest::TTest
{
public:
    static constexpr auto TABLE = "tmp/table";
    static constexpr auto SORTED_TABLE = "tmp/sorted_table";
    static constexpr auto EMPTY_TABLE = "tmp/empty_table";
    static constexpr auto UNEXIST_TABLE = "tmp/unexist_table";

    void SetUp() override {
        TTest::SetUp();
        Server.Reset(new TServer(ServerName()));
        CreateTables();
    }

    void TearDown() override {
        DropTables();
        TTest::TearDown();
    }

    TServer& GetServer() {
        return *Server;
    }

protected:
    void PrintTable(const char* tableName) {
        Cout << "~~~~~~" << tableName << "~~~~~~" << Endl;
        TClient client(GetServer());
        TTable table(client, tableName);
        for (auto&& it = table.Begin(); it != table.End(); ++it) {
            Cout << it.GetKey().AsString()
                << "\t" << it.GetSubKey().AsString()
                << "\t" << it.GetValue().AsString()
                << "\n";
        }
    }

private:
    void CreateTables() {
        {
            TClient client(GetServer());
            TUpdate update(client, TABLE);
            update.AddSub("a", "a", "0");
            update.AddSub("c", "c", "2");
            update.AddSub("e", "e", "2");
            update.AddSub("g", "g", "2");
            update.AddSub("i", "i", "2");
        }
        {
            TClient client(GetServer());
            TUpdate updateSorted(client, SORTED_TABLE, UM_SORTED);
            updateSorted.AddSub("a", "a", "0");
            updateSorted.AddSub("c", "c", "2");
            updateSorted.AddSub("e", "e", "2");
            updateSorted.AddSub("g", "g", "2");
            updateSorted.AddSub("i", "i", "2");
        }
        {
            TClient client(GetServer());
            TUpdate update(client, EMPTY_TABLE);
        }
    }

    void DropTables() {
        TClient client(GetServer());
        client.Drop(TABLE);
        client.Drop(SORTED_TABLE);
        client.Drop(EMPTY_TABLE);
    }

    THolder<TServer> Server;
};

Stroka ItToString(const TTableIterator&& it) {
    TStringStream ss;
    ss << "IsValid: " << it.IsValid()
        << " GetTableIndex: " << it.GetTableIndex()
        << " GetRecordIndex: " << it.GetRecordIndex();
    return ss.Str();
}


void TestTableMethods(TServer& server, const char* tableName) {
    Cout << "=======" << tableName << "=======" << Endl;
    TClient client(server);
    TTable table(client, tableName);

    { // Begin, End, IsEmpty, IsSorted, IsWriteLocked, GetRecordCount
        Cout << "TTable.Begin: " << ItToString(table.Begin()) << "\n";
        Cout << "TTable.End: " << ItToString(table.End()) << "\n";
        Cout << "TTable.IsEmpty: " << table.IsEmpty() << "\n";
        Cout << "TTable.IsSorted: " << table.IsSorted() << "\n";
        Cout << "TTable.IsWriteLocked: " << table.IsWriteLocked() << "\n";
        Cout << "TTable.GetRecordCount: " << table.GetRecordCount() << "\n";
    }
    { // Find, LowerBound, UpperBound.
        yvector<const char*> keys = { "a", "b", "c", "d", "e", "g", "i", "j", "aa", "bb", "0", "ee", "", "zzz" };
        for (auto& key : keys) {
            Cout << "TTable.Find key='" << key << "': " << ItToString(table.Find(key)) << "\n";
        }
        for (auto& key : keys) {
            Cout << "TTable.LowerBound key='" << key << "': " << ItToString(table.LowerBound(key)) << "\n";
        }
        for (auto& key : keys) {
            Cout << "TTable.UpperBound key='" << key << "': " << ItToString(table.UpperBound(key)) << "\n";
        }
    }
}

} // anonymous namespace

YT_TEST(TTableFixture, TestMethods) {
    TestTableMethods(GetServer(), TABLE);
    TestTableMethods(GetServer(), SORTED_TABLE);
    TestTableMethods(GetServer(), EMPTY_TABLE);
    TestTableMethods(GetServer(), UNEXIST_TABLE);
}

} // NCommonTest
} // NYT
