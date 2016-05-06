#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>

#include <utility>


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
            update.AddSub("a", "a", "a");
            update.AddSub("c", "c", "c");
            update.AddSub("e", "e", "e");
            update.AddSub("g", "g", "g");
            update.AddSub("i", "i", "i");
        }
        {
            TClient client(GetServer());
            TUpdate updateSorted(client, SORTED_TABLE, UM_SORTED);
            updateSorted.AddSub("a", "a", "a");
            updateSorted.AddSub("c", "c", "c");
            updateSorted.AddSub("e", "e", "e");
            updateSorted.AddSub("g", "g", "g");
            updateSorted.AddSub("i", "i", "i");
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

    { // Args = {}: Begin, End, IsEmpty, IsSorted, IsWriteLocked, GetRecordCount.
        auto headerTemplate = "TTable.%s: ";
        Cout << Sprintf(headerTemplate, "Begin") << ItToString(table.Begin()) << "\n";
        Cout << Sprintf(headerTemplate, "End")<< ItToString(table.End()) << "\n";
        Cout << Sprintf(headerTemplate, "IsEmpty") << table.IsEmpty() << "\n";
        Cout << Sprintf(headerTemplate, "IsSorted") << table.IsSorted() << "\n";
        Cout << Sprintf(headerTemplate, "IsWriteLocked") << table.IsWriteLocked() << "\n";
        Cout << Sprintf(headerTemplate, "GetRecordCount") << table.GetRecordCount() << "\n";
    }
    { // Args = {key}: Find, LowerBound, UpperBound, GetValueByKey.
        yvector<const char*> keys = {
                "a", "b", "c",
                "d", "e", "g",
                "i", "j", "aa",
                "bb", "0", "ee",
                "", "zzz"
            };
        auto headerTemplate = "TTable.%s key='%s': ";
        for (const auto& key : keys) {
            Cout << Sprintf(headerTemplate, "Find", key)
                << ItToString(table.Find(key)) << "\n";
        }
        for (const auto& key : keys) {
            Cout << Sprintf(headerTemplate, "LowerBound", key)
                << ItToString(table.LowerBound(key)) << "\n";
        }
        for (const auto& key : keys) {
            Cout << Sprintf(headerTemplate, "UpperBound", key)
                << ItToString(table.UpperBound(key)) << "\n";
        }
        for (const auto& key : keys) {
            yvector<char> value;
            bool res = table.GetValueByKey(key, &value);
            Cout << Sprintf(headerTemplate, "GetValueByKey", key)
                << res << " '" << TStringBuf(value.begin(), value.size()) << "'\n";
        }
    }
    { // Args = {key, subkey}: FindSub, LowerBoundSub, UpperBoundSub, GetValueByKeySub
        using TKeySubkey = std::pair<const char*, const char*>;
        yvector<TKeySubkey> args = {
                { "a", "a" },
                { "a", "zzz" },
                { "a", "" },
                { "a", "0" },
                { "b", "a"},
                { "b", "b"},
                { "i", "i"},
                { "i", "a"},
                { "i", "zzz"},
                { "0", "0"},
                { "zzz", "zzz"},
                { "", "" }
            };
        auto headerTemplate = "TTable.%s key='%s' subkey='%s': ";
        for (const auto& arg : args) {
            Cout << Sprintf(headerTemplate, "FindSub", arg.first, arg.second)
                << ItToString(table.FindSub(arg.first, arg.second)) << "\n";
        }
        for (const auto& arg : args) {
            Cout << Sprintf(headerTemplate, "LowerBoundSub", arg.first, arg.second)
                << ItToString(table.LowerBoundSub(arg.first, arg.second)) << "\n";
        }
        for (const auto& arg : args) {
            Cout << Sprintf(headerTemplate, "UpperBoundSub", arg.first, arg.second)
                << ItToString(table.UpperBoundSub(arg.first, arg.second)) << "\n";
        }
        for (const auto& arg : args) {
            yvector<char> value;
            bool res = table.GetValueByKeySub(arg.first, arg.second, &value);
            Cout << Sprintf(headerTemplate, "GetValueByKeySub", arg.first, arg.second)
                << res << " '" << TStringBuf(value.begin(), value.size()) << "'\n";
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
