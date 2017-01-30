#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>

#include <util/generic/vector.h>

#include <limits>
#include <utility>


namespace NYT {
namespace NCommonTest {

namespace {

class TTable
    : public NTest::TTest
{
public:
    static constexpr auto TABLE = "tmp/table_test/table";
    static constexpr auto SORTED_TABLE = "tmp/table_test/sorted_table";
    static constexpr auto EMPTY_TABLE = "tmp/table_test/empty_table";
    static constexpr auto UNEXIST_TABLE = "tmp/table_test/unexist_table";

    void SetUp() override {
        TTest::SetUp();
        Server.Reset(new NMR::TServer(ServerName()));
        CreateTables();
    }

    void TearDown() override {
        DropTables();
        TTest::TearDown();
    }

    NMR::TServer& GetServer() {
        return *Server;
    }

private:
    yvector<yvector<Stroka>> GetData() const {
        static yvector<yvector<Stroka>> data =  {
            { "a", "a", "a" },
            { "c", "c", "c" },
            { "e", "e", "e" },
            { "g", "g", "g" },
            { "i", "i", "i" }
        };
        return data;
    }

    void CreateTables() {
        {
            NMR::TClient client(GetServer());
            NMR::TUpdate update(client, TABLE);
            auto&& data = GetData();
            for (const auto& d : data) {
                update.AddSub(d[0], d[1], d[2]);
            }
        }
        {
            NMR::TClient client(GetServer());
            NMR::TUpdate updateSorted(client, SORTED_TABLE, NMR::UM_SORTED);
            auto&& data = GetData();
            for (const auto& d : data) {
                updateSorted.AddSub(d[0], d[1], d[2]);
            }
        }
        {
            NMR::TClient client(GetServer());
            NMR::TUpdate update(client, EMPTY_TABLE);
        }
    }

    void DropTables() {
        NMR::TClient client(GetServer());
        client.Drop(TABLE);
        client.Drop(SORTED_TABLE);
        client.Drop(EMPTY_TABLE);
        client.Drop(UNEXIST_TABLE);
    }

    THolder<NMR::TServer> Server;
};

Stroka ItToString(const NMR::TTableIterator&& it) {
    TStringStream ss;
    ss << "IsValid: " << it.IsValid()
        << " GetRecordIndex: " << it.GetRecordIndex();
    return ss.Str();
}

void TestTableMethods(NMR::TServer& server, const char* tableName) {
    Cout << "=======" << tableName << "=======" << Endl;
    NMR::TClient client(server);
    NMR::TTable table(client, tableName);

    { // Args = {}: Begin, End, IsEmpty, IsSorted, IsWriteLocked, GetRecordCount.
        auto headerTemplate = "TTable.%s: ";
        Cout << Sprintf(headerTemplate, "Begin") << ItToString(table.Begin()) << "\n";
        Cout << Sprintf(headerTemplate, "End")<< ItToString(table.End()) << "\n";
        Cout << Sprintf(headerTemplate, "IsEmpty") << table.IsEmpty() << "\n";
        Cout << Sprintf(headerTemplate, "IsSorted") << table.IsSorted() << "\n";
        Cout << Sprintf(headerTemplate, "IsWriteLocked") << table.IsWriteLocked() << "\n";
        Cout << Sprintf(headerTemplate, "GetRecordCount") << table.GetRecordCount() << "\n";
    }
    {
        const yvector<ui64> INDICIES = {
            0, 1, 2, 3, 4, 5, 6, 100, std::numeric_limits<ui64>::max()
        };
        const bool USE_SUBKEY = true;

        auto headerTemplate = "TTable.GetIteratorByIndex: index=%" PRIu64 " useSub: %d ";
        for (auto index : INDICIES) {
            Cout << Sprintf(headerTemplate, index, USE_SUBKEY)
                << ItToString(table.GetIteratorByIndex(index, USE_SUBKEY)) << "\n";
            Cout << Sprintf(headerTemplate, index, !USE_SUBKEY)
                << ItToString(table.GetIteratorByIndex(index, !USE_SUBKEY)) << "\n";
        }
    }
    { // Args = {key}: Find, LowerBound, UpperBound, GetValueByKey.
        const yvector<const char*> KEYS = {
                "a", "b", "c",
                "d", "e", "g",
                "i", "j", "aa",
                "bb", "0", "ee",
                "", "zzz"
            };
        auto headerTemplate = "TTable.%s key='%s': ";
        for (const auto& key : KEYS) {
            Cout << Sprintf(headerTemplate, "Find", key)
                << ItToString(table.Find(key)) << "\n";
        }
        for (const auto& key : KEYS) {
            Cout << Sprintf(headerTemplate, "LowerBound", key)
                << ItToString(table.LowerBound(key)) << "\n";
        }
        for (const auto& key : KEYS) {
            Cout << Sprintf(headerTemplate, "UpperBound", key)
                << ItToString(table.UpperBound(key)) << "\n";
        }
        yvector<char> value;
        for (const auto& key : KEYS) {
            bool res = table.GetValueByKey(key, &value);
            Cout << Sprintf(headerTemplate, "GetValueByKey", key)
                << res << " '" << TStringBuf(value.begin(), value.size()) << "'\n";
        }
    }
    { // Args = {key, subkey}: FindSub, LowerBoundSub, UpperBoundSub, GetValueByKeySub
        using TKeySubkey = std::pair<const char*, const char*>;
        const yvector<TKeySubkey> ARGS = {
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
        for (const auto& arg : ARGS) {
            Cout << Sprintf(headerTemplate, "FindSub", arg.first, arg.second)
                << ItToString(table.FindSub(arg.first, arg.second)) << "\n";
        }
        for (const auto& arg : ARGS) {
            Cout << Sprintf(headerTemplate, "LowerBoundSub", arg.first, arg.second)
                << ItToString(table.LowerBoundSub(arg.first, arg.second)) << "\n";
        }
        for (const auto& arg : ARGS) {
            Cout << Sprintf(headerTemplate, "UpperBoundSub", arg.first, arg.second)
                << ItToString(table.UpperBoundSub(arg.first, arg.second)) << "\n";
        }
        yvector<char> value;
        for (const auto& arg : ARGS) {
            bool res = table.GetValueByKeySub(arg.first, arg.second, &value);
            Cout << Sprintf(headerTemplate, "GetValueByKeySub", arg.first, arg.second)
                << res << " '" << TStringBuf(value.begin(), value.size()) << "'\n";
        }
    }
}

} // anonymous namespace

YT_TEST(TTable, MethodsTable) {
    TestTableMethods(GetServer(), TABLE);
}

YT_TEST(TTable, MethodsSortedTable) {
    TestTableMethods(GetServer(), SORTED_TABLE);
}

YT_TEST(TTable, MethodsEmptyTable) {
    TestTableMethods(GetServer(), EMPTY_TABLE);
}

YT_TEST(TTable, MethodsUnexistTable) {
    TestTableMethods(GetServer(), UNEXIST_TABLE);
}

} // NCommonTest
} // NYT
