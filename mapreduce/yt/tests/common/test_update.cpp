#include "helpers.h"

#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>

#include <util/generic/algorithm.h>

#include <limits>
#include <utility>


namespace NYT {
namespace NCommonTest {

using namespace NMR;

namespace {


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

    static yvector<yvector<Stroka>> GetTableData() {
        static yvector<yvector<Stroka>> data =  {
            { "a", "a", "a" },
            { "c", "c", "c" },
            { "e", "e", "e" },
            { "g", "g", "g" },
            { "i", "i", "i" }
        };
        return data;
    }

    static yvector<yvector<Stroka>> GetTestData(bool isSorted) {
        auto toAdd = GetTableData();
        toAdd.push_back({ "", "1", "2" });
        toAdd.push_back({ "1", "", "1" });
        toAdd.push_back({ "1", "3", "" });
        toAdd.push_back({ "a", "", "" });
        toAdd.push_back({ "", "b", "" });
        toAdd.push_back({ "", "", "c" });
        toAdd.push_back({ "", "", "" });

        if (isSorted) {
            Sort(toAdd.begin(), toAdd.end(),
                [](const yvector<Stroka>& v1, const yvector<Stroka>& v2) {
                    return v1[0] < v2[0] || (v1[0] == v2[0] && v1[1] < v2[1]);
                }
            );
        }

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

void DoTestSingleUpdate(TServer& server, TUpdateTable&& updateTable, bool isSorted = true) {
    const auto&& toAdd = TUpdateTestFixture::GetTestData(isSorted);

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
    PrintTable(server, ~updateTable.Name, Cout);

}

//void TestMultiUpdate(yvector<TUpdateTable>&& updates) {
//}

YT_TEST(TUpdateTestFixture, TestSingleUpdate) {
    constexpr bool ADD_SORTED = true;
    for (auto tableName : GetTables()) {
        for (auto updateMode : GetUpdateModes()) {
            RefreshTables();
            Cout << "~~~~~~" << tableName << "::" << (int) updateMode << "::add sorted data~~~~~~\n";
            DoTestSingleUpdate(GetServer(), TUpdateTable(tableName, updateMode));

            RefreshTables();
            Cout << "~~~~~~" << tableName << "::" << (int) updateMode << "::add not sorted data~~~~~~\n";
            DoTestSingleUpdate(GetServer(), TUpdateTable(tableName, updateMode), ADD_SORTED);
        }
    }
}

} // NCommonTest
} // NYT
