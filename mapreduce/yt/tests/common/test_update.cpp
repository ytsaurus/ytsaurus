#include <mapreduce/yt/tests/lib/lib.h>
#include <mapreduce/yt/tests/util/helpers.h>

#include <mapreduce/interface/all.h>

#include <util/generic/algorithm.h>

#include <limits>
#include <utility>


namespace NYT {
namespace NCommonTest {

using namespace NMR;

namespace {

using TData = yvector<yvector<Stroka>>;


class TUpdateTestFixture
    : public NTest::TTest
{
public:
    static constexpr auto TABLE = "tmp/update_test/table";
    static constexpr auto SORTED_TABLE = "tmp/update_test/sorted_table";
    static constexpr auto EMPTY_TABLE = "tmp/update_test/empty_table";
    static constexpr auto UNEXIST_TABLE = "tmp/update_test/unexist_table";

    static yvector<decltype(TABLE)> GetTables()  {
        return {
            TABLE,
            SORTED_TABLE,
            EMPTY_TABLE,
            UNEXIST_TABLE
        };
    }

    static yvector<EUpdateMode> GetUpdateModes()  {
        return {
            UM_REPLACE,
            UM_APPEND,
            UM_SORTED,
            UM_APPEND_SORTED
        };
    }

    static TData GetTableData() {
        static TData data =  {
            { "a", "a", "a" },
            { "c", "c", "c" },
            { "e", "e", "e" },
            { "g", "g", "g" },
            { "i", "i", "i" }
        };
        return data;
    }

    static TData GetBigTestData() {
        auto toAdd = GetTableData();
        toAdd.push_back({ "", "1", "2" });
        toAdd.push_back({ "1", "", "1" });
        toAdd.push_back({ "1", "3", "" });
        toAdd.push_back({ "a", "", "" });
        toAdd.push_back({ "", "b", "" });
        toAdd.push_back({ "", "", "c" });
        toAdd.push_back({ "", "", "" });
        for (int i = 0; i < 200; ++i) {
            Stroka key = Sprintf("%d", (int) 'a' + (i % 26));
            Stroka subkey = Sprintf("%d", (int) 'a' + (i % 7));
            toAdd.push_back({ key, subkey, key });
        }
        return toAdd;
    }

    static TData GetSmallSortedTestData() {
        yvector<yvector<Stroka>> toAdd;
        toAdd.push_back({ "xx", "xx", "xxx" });
        toAdd.push_back({ "xy", "xy", "yyy"  });
        toAdd.push_back({ "xz", "xz", "zzz"  });
        toAdd.push_back({ "yx", "yx", "xxx" });
        toAdd.push_back({ "yy", "yy", "yyy"  });
        toAdd.push_back({ "yz", "yz", "zzz"  });
        toAdd.push_back({ "zx", "zx", "xxx" });
        toAdd.push_back({ "zy", "zy", "yyy"  });
        toAdd.push_back({ "zz", "zz", "zzz"  });
        return toAdd;
    }

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

    void RefreshTables() {
        DropTables();
        CreateTables();
    }

private:
    void CreateTables() {
        {
            TClient client(GetServer());
            TUpdate update(client, TABLE);
            for (auto&& d : GetTableData()) {
                update.AddSub(d[0], d[1], d[2]);
            }
        }
        {
            TClient client(GetServer());
            TUpdate updateSorted(client, SORTED_TABLE, UM_SORTED);
            for (auto&& d : GetTableData()) {
                updateSorted.AddSub(d[0], d[1], d[2]);
            }
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
        client.Drop(UNEXIST_TABLE);
    }

    THolder<TServer> Server;
};



} // anonymous namespace


///////////////////////////////////////////////////////////////////////////////

void DoTestSingleUpdate(TServer& server, TUpdateTable&& updateTable, TData&& toAdd) {
    try {
        TClient client(server);
        TUpdate up(client, updateTable);
        for (auto&& d : toAdd) {
            Y_VERIFY(d.size() == 3);
            up.AddSub(d[0], d[1], d[2]);
        }
    } catch (const yexception& ex) {
        Cout << "EXCEPTION OCCURED" << Endl;
    }
}

YT_TEST(TUpdateTestFixture, TestSingleUpdate) {
    Cout << "======TEST SINGLE UPDATE======\n";
    for (auto tableName : GetTables()) {
        for (auto updateMode : GetUpdateModes()) {
            RefreshTables();
            DoTestSingleUpdate(GetServer(), TUpdateTable(tableName, updateMode), GetBigTestData());
            Cout << "~~~~~~" << tableName << "::" << (int) updateMode << "::GetBigTestData~~~~~~\n";
            PrintTable(GetServer(), tableName, Cout);

            RefreshTables();
            DoTestSingleUpdate(GetServer(), TUpdateTable(tableName, updateMode), GetSmallSortedTestData());
            Cout << "~~~~~~" << tableName << "::" << (int) updateMode << "::GetSmallSortedTestData~~~~~~\n";
            PrintTable(GetServer(), tableName, Cout);
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

void DoTestMultiUpdate(TServer& server, const yvector<TUpdateTable>& updates, TData&& toAdd) {
    try {
        TClient client(server);
        TUpdate up(client, updates);

        const auto TABLES_COUNT = up.GetUpdateTableCount();
        Y_VERIFY((int) updates.size() == TABLES_COUNT);

        int curTableInd = 0;
        for (size_t i = 0, end = toAdd.size(); i < end; ++i) {
            auto&& d = toAdd[i];
            Y_VERIFY(d.size() == 3);
            up.AddSub(d[0], d[1], d[2]);

            if (i % 7 == 0) {
                curTableInd = (curTableInd + 1) % TABLES_COUNT;
                up.SetCurrentTable(curTableInd);
            }
        }
    } catch (const yexception& ex) {
        Cout << "EXCEPTION OCCURED" << Endl;
    }
}


YT_TEST(TUpdateTestFixture, TestMultiUpdate) {
    Cout << "======TEST MULTI UPDATE======\n";

    #define X(name, mode) TUpdateTable(name, mode)

    static const yvector<yvector<TUpdateTable>> allUpdates = {
        {
            X(EMPTY_TABLE, UM_APPEND),
            X(UNEXIST_TABLE, UM_APPEND)
        },
        {
            X(EMPTY_TABLE, UM_APPEND),
            X(TABLE, UM_REPLACE)
        },
        {
            X(SORTED_TABLE, UM_APPEND),
            X(UNEXIST_TABLE, UM_REPLACE)
        },
        {
            X(SORTED_TABLE, UM_APPEND),
            X(TABLE, UM_REPLACE)
        },
        {
            X(SORTED_TABLE, UM_SORTED),
            X(TABLE, UM_SORTED)
        },
        {
            X(SORTED_TABLE, UM_APPEND_SORTED),
            X(TABLE, UM_APPEND_SORTED),
            X(UNEXIST_TABLE, UM_APPEND)
        },
    };

    #undef X

    for (const auto& updates : allUpdates) {
        RefreshTables();
        DoTestMultiUpdate(GetServer(), updates, GetBigTestData());
        for (const auto& update : updates) {
            Cout << "~~~~~~" << update.Name << "::" << (int) update.Mode << "::GetBigTestData~~~~~~\n";
            PrintTable(GetServer(), ~update.Name, Cout);
        }

        RefreshTables();
        DoTestMultiUpdate(GetServer(), updates, GetSmallSortedTestData());
        for (const auto& update : updates) {
            Cout << "~~~~~~" << update.Name << "::" << (int) update.Mode << "::GetSmallSortedTestData~~~~~~\n";
            PrintTable(GetServer(), ~update.Name, Cout);
        }
    }
}

} // NCommonTest
} // NYT
