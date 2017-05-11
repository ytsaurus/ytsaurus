#include "node_dbg.h"

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

YT_TEST(TCypress, Copy)
{
    Client()->Create(Node(), NT_STRING);
    Client()->Copy(Node(), Node2());

    EXPECT_EQ(Client()->Exists(Node()), true);
    EXPECT_EQ(Client()->Exists(Node2()), true);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, Copy_PreserveExpirationTime)
{
    const TString expirationTime = "2042-02-15T18:45:19.591902Z";
    for (TString path : {"//tmp/table_default", "//tmp/table_false", "//tmp/table_true"}) {
        Client()->Create(path, NT_TABLE);
        Client()->Set(path + "/@expiration_time", expirationTime);
    }

    Client()->Copy("//tmp/table_default", "//tmp/copy_table_default");
    Client()->Copy("//tmp/table_true", "//tmp/copy_table_true", TCopyOptions().PreserveExpirationTime(true));
    Client()->Copy("//tmp/table_false", "//tmp/copy_table_false", TCopyOptions().PreserveExpirationTime(false));

    EXPECT_THROW(Client()->Get("//tmp/copy_table_default/@expiration_time"), yexception);
    ASSERT_EQ(Client()->Get("//tmp/copy_table_true/@expiration_time"), TNode(expirationTime));
    EXPECT_THROW(Client()->Get("//tmp/copy_table_false/@expiration_time"), yexception);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, Move)
{
    Client()->Create(Node(), NT_STRING);
    Client()->Move(Node(), Node2());

    EXPECT_EQ(Client()->Exists(Node()), false);
    EXPECT_EQ(Client()->Exists(Node2()), true);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypress, Move_PreserveExpirationTime)
{
    const TString expirationTime = "2042-02-15T18:45:19.591902Z";
    for (TString path : {"//tmp/table_default", "//tmp/table_false", "//tmp/table_true"}) {
        Client()->Create(path, NT_TABLE);
        Client()->Set(path + "/@expiration_time", expirationTime);
    }

    Client()->Move("//tmp/table_default", "//tmp/moved_table_default");
    Client()->Move("//tmp/table_true", "//tmp/moved_table_true", TMoveOptions().PreserveExpirationTime(true));
    Client()->Move("//tmp/table_false", "//tmp/moved_table_false", TMoveOptions().PreserveExpirationTime(false));

    ASSERT_EQ(Client()->Get("//tmp/moved_table_default/@expiration_time"), TNode(expirationTime));
    ASSERT_EQ(Client()->Get("//tmp/moved_table_true/@expiration_time"), TNode(expirationTime));
    EXPECT_THROW(Client()->Get("//tmp/moved_table_false/@expiration_time"), yexception);
}

////////////////////////////////////////////////////////////////////////////////

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

