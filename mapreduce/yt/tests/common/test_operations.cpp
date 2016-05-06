#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>

namespace NYT {
namespace NCommonTest {

using namespace NMR;

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

class TIdMap
    : public IMap
{
    OBJECT_METHODS(TIdMap);
public:
    void DoSub(TValue k, TValue s, TValue v, TUpdate& update) override {
        update.AddSub(k, s, v);
    }
};

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
    {
        TClient client(Server());
        TTable table(client, Output());
        for (TTableIterator i = table.Begin(); i != table.End(); ++i) {
            Cout <<
                "key = " << i.GetKey().AsStringBuf() <<
                ", subkey = " << i.GetSubKey().AsStringBuf() <<
                ", value = " << i.GetValue().AsStringBuf() <<
            Endl;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCommonTest
} // namespace NYT

using namespace NYT::NCommonTest;
REGISTER_SAVELOAD_CLASS(0x00000001, TIdMap);

