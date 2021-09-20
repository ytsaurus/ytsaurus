#include "id_map.h"
#include "id_reduce.h"

#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>


namespace NYT {
namespace NCommonTest {

using namespace NMR;
using namespace NTestOps;

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public NTest::TTest
{
public:
    void SetUp() override
    {
        TTest::SetUp();
        Server_ = StealToHolder(new TServer(ServerName()));
        RemoveTables();
    }

    void TearDown() override
    {
        RemoveTables();
        TTest::TearDown();
    }

    TServer& Server() { return *Server_; }
    const char* Input() { return "tmp/input"; }
    const char* Output() { return "tmp/output"; }

    void PrintTable(const TString& tableName) {
        TClient client(Server());
        TTable table(client, tableName);
        Cout << "IsSorted: " << table.IsSorted() << Endl;
        for (TTableIterator i = table.Begin(); i != table.End(); ++i) {
            Cout <<
                "key = " << i.GetKey().AsStringBuf() <<
                ", subkey = " << i.GetSubKey().AsStringBuf() <<
                ", value = " << i.GetValue().AsStringBuf() <<
            Endl;
        }
    }

    void SimpleFillTable(const TString& tableName) {
        TClient client(Server());
        TUpdate update(client, tableName);
        for (int i = 0; i < 10; ++i) {
            auto key = Sprintf("%d", (i * 3) % 19);
            auto subkey = Sprintf("%d", (i * 7) % 13);
            auto value = "0";
            update.AddSub(key, subkey, value);
        }
    }

private:
    void RemoveTables()
    {
        TClient client(Server());
        client.Drop(Input());
        client.Drop(Output());
    }

    THolder<TServer> Server_;
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TOperation, IdMap)
{
    SimpleFillTable(Input());
    Server().Map(Input(), Output(), new TIdMap);
    PrintTable(Output());
}

YT_TEST(TOperation, IdReduce)
{
    SimpleFillTable(Input());
    Server().Reduce(Input(), Output(), new TIdReduce);
    PrintTable(Output());
}

YT_TEST(TOperation, SelfSort)
{
    SimpleFillTable(Input());
    Server().Sort(Input());
    PrintTable(Input());
}

YT_TEST(TOperation, Sort)
{
    SimpleFillTable(Input());
    Server().Sort(Input(), Output());
    PrintTable(Output());
}

YT_TEST(TOperation, IdMapIdReduce)
{
    SimpleFillTable(Input());
    Server().MapReduce(Input(), Output(), new TIdMap, new TIdReduce);
    PrintTable(Output());
}

YT_TEST(TOperation, SingleMerge) {
    SimpleFillTable(Input());
    Server().Sort(Input());
    TVector<TString> srcTables = { Input() };
    Server().Merge(srcTables, Output());
    PrintTable(Output());
}

YT_TEST(TOperation, MultiMerge) {
    const TVector<TString> srcs = { "tmp/t0", "tmp/t1", "tmp/t2" };
    {
        TClient client(Server());
        TUpdate update0(client, srcs[0]);
        TUpdate update1(client, srcs[1]);
        TUpdate update2(client, srcs[2]);
        for (int i = 0; i < 228; ++i) {
            auto key = Sprintf("%d", (i * 3) % 19);
            auto subkey = Sprintf("%d", (i * 7) % 13);
            auto value = "0";
            int ind = i % 3;
            switch (ind) {
                case 0: update0.AddSub(key, subkey, value); break;
                case 1: update1.AddSub(key, subkey, value); break;
                case 2:
                default: update2.AddSub(key, subkey, value); break;
            }
        }
    }
    for (size_t i = 0; i < srcs.size(); ++i) {
        Server().Sort(srcs[i]);
    }
    Server().Merge(srcs, Output());
    PrintTable(Output());
}

YT_TEST(TOperation, CopyN2N) {
    constexpr auto TABLE_SORTED = "tmp/src/sorted";
    constexpr auto TABLE_SIMPLE = "tmp/src/simple";
    { // Prepare source tables.
        SimpleFillTable(TABLE_SIMPLE);
        SimpleFillTable(TABLE_SORTED);
        Server().Sort(TABLE_SORTED);
    }

    const TVector<std::pair<EUpdateMode, EUpdateMode>> modes = {
        { UM_REPLACE, UM_REPLACE },
        { UM_REPLACE, UM_APPEND },
        { UM_REPLACE, UM_SORTED },

        { UM_APPEND, UM_REPLACE },
        { UM_APPEND, UM_APPEND },
        { UM_APPEND, UM_SORTED },

        { UM_SORTED, UM_REPLACE },
        { UM_SORTED, UM_APPEND },
        { UM_SORTED, UM_SORTED },
    };

    for (size_t i = 0; i < modes.size(); ++i) {
        auto DST1 = Sprintf("tmp/dst/test1/case%dunexist1", (int) i);
        auto DST2 = Sprintf("tmp/dst/test1/case%dunexist2", (int) i);

        TCopyParams params;
        params.SrcTables.push_back(TInputTable(TABLE_SORTED));
        params.SrcTables.push_back(TInputTable(TABLE_SIMPLE));
        params.DstTables.push_back(TUpdateTable(DST1, modes[i].first));
        params.DstTables.push_back(TUpdateTable(DST2, modes[i].second));

        Server().Copy(params);

        Cout << "=======TEST1 CASE" << i << "=======" << Endl;
        Cout << "~~~TABLE1~~~" << Endl;
        PrintTable(DST1);
        Cout << "~~~TABLE2~~~" << Endl;
        PrintTable(DST2);

        Server().Drop(DST1);
        Server().Drop(DST2);
    }

    for (size_t i = 0; i < modes.size(); ++i) {
        auto DST1 = Sprintf("tmp/dst/test2/case%dsimple1", (int) i);
        auto DST2 = Sprintf("tmp/dst/test2/case%dsimple2", (int) i);

        SimpleFillTable(DST1);
        SimpleFillTable(DST2);

        TCopyParams params;
        params.SrcTables.push_back(TInputTable(TABLE_SORTED));
        params.SrcTables.push_back(TInputTable(TABLE_SIMPLE));
        params.DstTables.push_back(TUpdateTable(DST1, modes[i].first));
        params.DstTables.push_back(TUpdateTable(DST2, modes[i].second));

        Server().Copy(params);

        Cout << "=======TEST2 CASE" << i << "=======" << Endl;
        Cout << "~~~TABLE1~~~" << Endl;
        PrintTable(DST1);
        Cout << "~~~TABLE2~~~" << Endl;
        PrintTable(DST2);

        Server().Drop(DST1);
        Server().Drop(DST2);
    }

    for (size_t i = 0; i < modes.size(); ++i) {
        auto DST1 = Sprintf("tmp/dst/test3/case%dsorted1", (int) i);
        auto DST2 = Sprintf("tmp/dst/test3/case%dsorted2", (int) i);

        SimpleFillTable(DST1);
        SimpleFillTable(DST2);
        Server().Sort(DST1);
        Server().Sort(DST2);

        TCopyParams params;
        params.SrcTables.push_back(TInputTable(TABLE_SORTED));
        params.SrcTables.push_back(TInputTable(TABLE_SIMPLE));
        params.DstTables.push_back(TUpdateTable(DST1, modes[i].first));
        params.DstTables.push_back(TUpdateTable(DST2, modes[i].second));

        Server().Copy(params);

        Cout << "=======TEST3 CASE" << i << "=======" << Endl;
        Cout << "~~~TABLE1~~~" << Endl;
        PrintTable(DST1);
        Cout << "~~~TABLE2~~~" << Endl;
        PrintTable(DST2);

        Server().Drop(DST1);
        Server().Drop(DST2);
    }
}

} // namespace NCommonTest
} // namespace NYT
