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

YT_TEST(TOperation, IdReduce) {
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

YT_TEST(TOperation, Sort) {
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

} // namespace NCommonTest
} // namespace NYT
