#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/interface/client.h>

namespace NYT {
namespace NNativeTest {

////////////////////////////////////////////////////////////////////////////////

class TCypress
    : public NTest::TTest
{
public:
    void SetUp() override
    {
        TTest::SetUp();
        Client_ = CreateClient(ServerName());
        RemoveNodes();
    }

    void TearDown() override
    {
        RemoveNodes();
        TTest::TearDown();
    }

    IClientPtr Client() const { return Client_; }
    const char* Node() const { return "tmp/node"; }

private:
    void RemoveNodes()
    {
        Client()->Remove(Node(), TRemoveOptions().Force(true));
    }

    IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, CreateRemove)
{
    Client()->Create(Node(), NT_STRING);

    Client()->Create(Node(), NT_STRING,
        TCreateOptions().IgnoreExisting(true));

    Client()->Remove(Node());

    Client()->Remove(Node(),
        TRemoveOptions().Force(true));
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, SetGetInt64)
{
    Client()->Create(Node(), NT_INT64);
    Client()->Set(Node(), 666);
    EXPECT_EQ(Client()->Get(Node()).AsInt64(), 666);
    Client()->Remove(Node());
}

YT_TEST(TCypress, SetGetUint64)
{
    Client()->Create(Node(), NT_UINT64);
    Client()->Set(Node(), 256u);
    EXPECT_EQ(Client()->Get(Node()).AsUint64(), 256u);
    Client()->Remove(Node());
}

YT_TEST(TCypress, SetGetDouble)
{
    Client()->Create(Node(), NT_DOUBLE);
    Client()->Set(Node(), 10.0);
    EXPECT_EQ(Client()->Get(Node()).AsDouble(), 10.0);
    Client()->Remove(Node());
}

YT_TEST(TCypress, SetGetString)
{
    Client()->Create(Node(), NT_STRING);
    Client()->Set(Node(), "foo");
    EXPECT_EQ(Client()->Get(Node()).AsString(), "foo");
    Client()->Remove(Node());
}

YT_TEST(TCypress, SetGetBoolean)
{
    Client()->Create(Node(), NT_BOOLEAN);
    Client()->Set(Node(), true);
    EXPECT_EQ(Client()->Get(Node()).AsBool(), true);
    Client()->Remove(Node());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTest
} // namespace NYT

