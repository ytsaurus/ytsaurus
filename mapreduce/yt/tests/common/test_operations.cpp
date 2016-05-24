#include <mapreduce/yt/tests/operations/id_map.h>
#include <mapreduce/yt/tests/operations/id_reduce.h>

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
        Server_ = new TServer(ServerName());
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

    void PrintTable(const char* tableName) {
        TClient client(Server());
        TTable table(client, tableName);
        for (TTableIterator i = table.Begin(); i != table.End(); ++i) {
            Cout <<
                "key = " << i.GetKey().AsStringBuf() <<
                ", subkey = " << i.GetSubKey().AsStringBuf() <<
                ", value = " << i.GetValue().AsStringBuf() <<
            Endl;
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
    {
        TClient client(Server());
        TUpdate update(client, Input());
        for (int i = 0; i < 8; ++i) {
            auto key = Sprintf("%d", i);
            auto subkey = Sprintf("%d", i * 2);
            auto value = Sprintf("%d", i * 4);
            update.AddSub(key, subkey, value);
        }
    }
    Server().Map(Input(), Output(), new TIdMap);
    PrintTable(Output());
}

YT_TEST(TOperation, IdReduce)
{
    {
        TClient client(Server());
        TUpdate update(client, Input());
        for (int i = 0; i < 8; ++i) {
            auto key = Sprintf("%d", i);
            auto subkey = Sprintf("%d", i * 2);
            auto value = Sprintf("%d", i * 4);
            update.AddSub(key, subkey, value);
            update.AddSub(key, subkey, value);
        }
    }
    Server().Reduce(Input(), Output(), new TIdReduce);
    PrintTable(Output());
}

YT_TEST(TOperation, SelfSort)
{
    {
        TClient client(Server());
        TUpdate update(client, Input());
        for (int i = 0; i < 1024; ++i) {
            auto key = Sprintf("%d", (i * 13) % 17);
            auto subkey = Sprintf("%d", (i * 7) % 17);
            auto value = "0"; // Use always const value because YT sorts by value too.
            update.AddSub(key, subkey, value);
            update.AddSub(subkey, key, value);
        }
    }
    Server().Sort(Input());
    PrintTable(Input());
}

YT_TEST(TOperation, Sort)
{
    {
        TClient client(Server());
        TUpdate update(client, Input());
        for (int i = 0; i < 1024; ++i) {
            auto key = Sprintf("%d", (i * 13) % 17);
            auto subkey = Sprintf("%d", (i * 7) % 17);
            auto value = "0"; // Use always const value because YT sorts by value too.
            update.AddSub(key, subkey, value);
            update.AddSub(subkey, key, value);
        }
    }
    Server().Sort(Input(), Output());
    PrintTable(Output());
}

YT_TEST(TOperation, IdMapIdReduce)
{
    {
        TClient client(Server());
        TUpdate update(client, Input());
        for (int i = 0; i < 1024; ++i) {
            auto key = Sprintf("%d", (i * 13) % 17);
            auto subkey = Sprintf("%d", (i * 7) % 19);
            auto value = "0";
            update.AddSub(key, subkey, value);
        }
    }
    Server().MapReduce(Input(), Output(), new TIdMap, new TIdReduce);
    PrintTable(Output());
}

YT_TEST(TOperation, SingleMerge) {
    {
        TClient client(Server());
        TUpdate update(client, Input());
        for (int i = 0; i < 1024; ++i) {
            auto key = Sprintf("%d", (i * 19) % 37);
            auto subkey = Sprintf("%d", (i * 3) % 11);
            auto value = "0";
            update.AddSub(key, subkey, value);
        }
    }
    Server().Sort(Input());
    yvector<Stroka> srcTables = { Input() };
    Server().Merge(srcTables, Output());
    PrintTable(Output());
}

YT_TEST(TOperation, MultiMerge) {
    const yvector<Stroka> srcs = { "tmp/t0", "tmp/t1", "tmp/t2" };
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

} // namespace NCommonTest
} // namespace NYT
