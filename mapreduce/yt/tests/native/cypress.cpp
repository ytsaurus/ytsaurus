#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/interface/client.h>

#include <util/stream/input.h>

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

    IClientPtr Client() const {
        return Client_;
    }

    const char* Node() const {
        return "tmp/node";
    }

    const char* Node2() const {
        return "tmp/node2";
    }

    const char* Node3() const {
        return "tmp/node3";
    }

private:
    void RemoveNodes()
    {
        Client()->Remove("//tmp/*", TRemoveOptions().Force(true).Recursive(true));
    }

    IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, CreateRemove)
{
    Client()->Create(Node(), NT_STRING);

    EXPECT_EQ(Client()->Exists(Node()), true);

    Client()->Create(Node(), NT_STRING,
        TCreateOptions().IgnoreExisting(true));

    EXPECT_EQ(Client()->Exists(Node()), true);

    Client()->Remove(Node());

    EXPECT_EQ(Client()->Exists(Node()), false);

    Client()->Remove(Node(),
        TRemoveOptions().Force(true));

    EXPECT_EQ(Client()->Exists(Node()), false);
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

YT_TEST(TCypress, List)
{
    Client()->Create(Node(), NT_MAP);

    Stroka node(Node());
    Client()->Create(node + "/int", NT_INT64);
    Client()->Create(node + "/uint", NT_UINT64);
    Client()->Create(node + "/double", NT_DOUBLE);
    Client()->Create(node + "/bool", NT_BOOLEAN);
    Client()->Create(node + "/string", NT_STRING);
    Client()->Create(node + "/map", NT_MAP);
    Client()->Create(node + "/list", NT_LIST);
    Client()->Create(node + "/file", NT_FILE);
    Client()->Create(node + "/table", NT_TABLE);
    Client()->Create(node + "/document", NT_DOCUMENT);

    auto list = Client()->List(Node(),
        TListOptions().AttributeFilter(TAttributeFilter().AddAttribute("type")));

    for (auto e : list) {
        Cout << e.AsString() << ": "
            << e.GetAttributes()["type"].AsString() << Endl;
    }

    Client()->Remove(Node(), TRemoveOptions().Force(true).Recursive(true));
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, Copy)
{
    Client()->Create(Node(), NT_STRING);
    Client()->Copy(Node(), Node2());

    EXPECT_EQ(Client()->Exists(Node()), true);
    EXPECT_EQ(Client()->Exists(Node2()), true);
}

YT_TEST(TCypress, Move)
{
    Client()->Create(Node(), NT_STRING);
    Client()->Move(Node(), Node2());

    EXPECT_EQ(Client()->Exists(Node()), false);
    EXPECT_EQ(Client()->Exists(Node2()), true);
}

YT_TEST(TCypress, Link)
{
    Client()->Create(Node(), NT_STRING);
    Client()->Link(Node(), Node2());

    EXPECT_EQ(Client()->Exists(Node()), true);
    EXPECT_EQ(Client()->Exists(Node2()), true);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, Lock_ChildKey)
{
    Client()->Create("//tmp/map-node", NT_MAP);
    Client()->Set("//tmp/map-node/child1", 1);
    Client()->Set("//tmp/map-node/child2", 2);

    auto tx1 = Client()->StartTransaction();

    // wrong lock type
    EXPECT_THROW(
        tx1->Lock("//tmp/map-node", ELockMode::LM_EXCLUSIVE, TLockOptions().ChildKey("child1")),
        yexception);

    // should be ok
    tx1->Lock("//tmp/map-node", ELockMode::LM_SHARED, TLockOptions().ChildKey("child1"));

    tx1->Set("//tmp/map-node/child1", 11);

    EXPECT_THROW(
        tx1->Lock("//tmp/map-node", ELockMode::LM_EXCLUSIVE, TLockOptions().ChildKey("non-existent-key")),
        yexception);

    auto tx2 = Client()->StartTransaction();

    // locked
    EXPECT_THROW(tx2->Set("//tmp/map-node/child1", 12), yexception);

    // lock is already taken
    EXPECT_THROW(
        tx2->Lock("//tmp/map-node", ELockMode::LM_SHARED, TLockOptions().ChildKey("child1")),
        yexception);

    // should be ok
    tx2->Lock("//tmp/map-node", ELockMode::LM_SHARED, TLockOptions().ChildKey("child2"));
    tx2->Set("//tmp/map-node/child2", 22);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, Lock_AttributeKey)
{
    Client()->Create("//tmp/table", NT_TABLE);
    Client()->Set("//tmp/table/@attribute1", 1);
    Client()->Set("//tmp/table/@attribute2", 2);

    auto tx1 = Client()->StartTransaction();

    // wrong lock type
    EXPECT_THROW(
        tx1->Lock("//tmp/table",
            ELockMode::LM_EXCLUSIVE,
            TLockOptions().AttributeKey("attribute1")),
        yexception);

    // should be ok
    tx1->Lock("//tmp/table",
        ELockMode::LM_SHARED,
        TLockOptions().ChildKey("attribute1"));

    tx1->Set("//tmp/table/@attribute1", 11);

    auto tx2 = Client()->StartTransaction();

    // lock is already taken
    EXPECT_THROW(
        tx2->Lock("//tmp/table",
            ELockMode::LM_SHARED,
            TLockOptions().ChildKey("attribute1")),
        yexception);

    EXPECT_THROW(
        tx2->Set("//tmp/table/@attribute1", 12),
        yexception);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, Concatenate)
{
    {
        auto writer = Client()->CreateFileWriter(Node());
        *writer << "foo";
        writer->Finish();
    }
    {
        auto writer = Client()->CreateFileWriter(Node2());
        *writer << "bar";
        writer->Finish();
    }
    Client()->Create(Node3(), NT_FILE);
    yvector<TYPath> nodes{Node(), Node2()};
    Client()->Concatenate(nodes, Node3());
    {
        auto reader = Client()->CreateFileReader(Node3());
        TransferData(reader.Get(), &Cout);
        Cout << Endl;
    }
    Client()->Concatenate(nodes, Node3(), TConcatenateOptions().Append(true));
    {
        auto reader = Client()->CreateFileReader(Node3());
        TransferData(reader.Get(), &Cout);
        Cout << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTest
} // namespace NYT

