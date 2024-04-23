#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/errors.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/guid.h>
#include <util/string/builder.h>
#include <util/system/env.h>

using namespace NYT;
using namespace NYT::NTesting;

namespace {

////////////////////////////////////////////////////////////////////

class TYtPrefixGuard
{
public:
    TYtPrefixGuard(const TString& ytPrefix)
    {
        OldYtPrefix = NYT::TConfig::Get()->Prefix;
        NYT::TConfig::Get()->Prefix = ytPrefix;
    }

    TYtPrefixGuard(const TYtPrefixGuard&) = delete;

    ~TYtPrefixGuard()
    {
        NYT::TConfig::Get()->Prefix = OldYtPrefix;
    }

private:
    TString OldYtPrefix;
};

////////////////////////////////////////////////////////////////////

class TLowerRequestLimit
{
public:
    TLowerRequestLimit(IClientPtr client, TString user, int value)
        : Client_(client)
        , Prefix_("//sys/users/" + user)
        , ReadRequestRateLimit_(Client_->Get(Prefix_ + "/@read_request_rate_limit"))
        , WriteRequestRateLimit_(Client_->Get(Prefix_ + "/@write_request_rate_limit"))
        , RequestQueueSizeLimit_(Client_->Get(Prefix_ + "/@request_queue_size_limit"))
    {
        Client_->Set(Prefix_ + "/@read_request_rate_limit", value);
        Client_->Set(Prefix_ + "/@write_request_rate_limit", value);
        Client_->Set(Prefix_ + "/@request_queue_size_limit", value);
    }

    ~TLowerRequestLimit()
    {
        try {
            Client_->Set(Prefix_ + "/@read_request_rate_limit", ReadRequestRateLimit_);
            Client_->Set(Prefix_ + "/@write_request_rate_limit", WriteRequestRateLimit_);
            Client_->Set(Prefix_ + "/@request_queue_size_limit", RequestQueueSizeLimit_);
        } catch (const std::exception& ex) {
            Y_ABORT("%s", ex.what());
        } catch (...) {
            Y_ABORT();
        }
    }

private:
    const IClientPtr Client_;
    const TString Prefix_;
    const TNode ReadRequestRateLimit_;
    const TNode WriteRequestRateLimit_;
    const TNode RequestQueueSizeLimit_;
};

////////////////////////////////////////////////////////////////////

static TNode::TListType SortedStrings(TNode::TListType input) {
    std::sort(input.begin(), input.end(), [] (const TNode& lhs, const TNode& rhs) {
        return lhs.AsString() < rhs.AsString();
    });
    return input;
}

////////////////////////////////////////////////////////////////////

TEST(BatchRequestSuite, TestGet)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    client->Set(workingDir + "/foo", 5);
    client->Set(workingDir + "/bar", "bar");

    auto tx = client->StartTransaction();
    tx->Set(workingDir + "/qux", "gg");

    auto batchRequest = client->CreateBatchRequest();
    auto fooRes = batchRequest->Get(workingDir + "/foo", TGetOptions());
    auto barRes = batchRequest->Get(workingDir + "/bar", TGetOptions());
    auto quxRes = batchRequest->Get(workingDir + "/qux", TGetOptions());
    auto quxTxRes = batchRequest->WithTransaction(tx).Get(workingDir + "/qux", TGetOptions());
    auto fooAccountRes = batchRequest->Get(workingDir + "/foo",
        TGetOptions().AttributeFilter(TAttributeFilter().AddAttribute("account")));

    batchRequest->ExecuteBatch();
    EXPECT_EQ(fooRes.GetValue(), TNode(5));
    EXPECT_EQ(barRes.GetValue(), TNode("bar"));
    EXPECT_THROW(quxRes.GetValue(), TErrorResponse);
    EXPECT_EQ(quxTxRes.GetValue(), TNode("gg"));

    // Check that max_size options is passed correctly
    EXPECT_TRUE(fooAccountRes.GetValue().GetAttributes().HasKey("account"));
}

TEST(BatchRequestSuite, TestSet)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto tx = client->StartTransaction();

    auto batchRequest = client->CreateBatchRequest();
    auto fooRes = batchRequest->Set(workingDir + "/foo", 5);
    auto barRes = batchRequest->Set(workingDir + "/bar", "bar");
    auto quxTxRes = batchRequest->WithTransaction(tx).Set(workingDir + "/qux", "gg");
    auto badRes = batchRequest->WithTransaction(tx).Set(workingDir + "/unexisting/bad", "vzhukh");

    batchRequest->ExecuteBatch();

    fooRes.GetValue();
    barRes.GetValue();
    quxTxRes.GetValue();
    EXPECT_THROW(badRes.GetValue(), TErrorResponse);

    EXPECT_EQ(client->Get(workingDir + "/foo"), TNode(5));
    EXPECT_EQ(client->Get(workingDir + "/bar"), TNode("bar"));
    EXPECT_TRUE(!client->Exists(workingDir + "/qux"));
    EXPECT_EQ(tx->Get(workingDir + "/qux"), TNode("gg"));
}

TEST(BatchRequestSuite, TestList)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();

    client->Set(workingDir + "/foo", 5);
    client->Set(workingDir + "/bar", "bar");
    tx->Set(workingDir + "/qux", "gg");

    auto batchRequest = client->CreateBatchRequest();
    auto simpleRes = batchRequest->List(workingDir + "");
    auto txRes = batchRequest->WithTransaction(tx).List(workingDir + "");
    auto maxSizeRes = batchRequest->WithTransaction(tx).List(workingDir + "",
        TListOptions().MaxSize(1));
    auto attributeFilterRes = batchRequest->WithTransaction(tx).List(workingDir + "",
        TListOptions().AttributeFilter(TAttributeFilter().AddAttribute("account")));
    auto badRes = batchRequest->List(workingDir + "/missing-dir");
    batchRequest->ExecuteBatch();

    EXPECT_EQ(
        SortedStrings(simpleRes.GetValue()),
        TNode::TListType({"bar", "foo"}));

    EXPECT_EQ(
        SortedStrings(txRes.GetValue()),
        TNode::TListType({"bar", "foo", "qux"}));
    EXPECT_EQ(std::ssize(maxSizeRes.GetValue()), 1);

    {
        const auto& attributes = attributeFilterRes.GetValue().at(0).GetAttributes();
        EXPECT_TRUE(attributes.HasKey("account"));
    }

    EXPECT_THROW(badRes.GetValue(), TErrorResponse);
}

TEST(BatchRequestSuite, TestListEmptyPath)
{
    const TTestFixture fixture;
    const auto client = fixture.GetClient();
    NYT::TConfig::Get()->Prefix = fixture.GetWorkingDir() + '/';

    client->Set("foo", 5);
    client->Set("bar", "bar");

    auto batchRequest = client->CreateBatchRequest();
    auto listRes = batchRequest->List("");
    batchRequest->ExecuteBatch();

    EXPECT_EQ(
        SortedStrings(listRes.GetValue()),
        TNode::TListType({"bar", "foo"}));
}

TEST(BatchRequestSuite, TestExists)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();

    client->Set(workingDir + "/foo", 5);
    tx->Set(workingDir + "/qux", "gg");

    auto batchRequest = client->CreateBatchRequest();
    auto fooRes = batchRequest->Exists(workingDir + "/foo");
    auto badRes = batchRequest->Exists(workingDir + "/bad-unexisting-node");
    auto quxRes = batchRequest->Exists(workingDir + "/qux");
    auto quxTxRes = batchRequest->WithTransaction(tx).Exists(workingDir + "/qux");
    batchRequest->ExecuteBatch();

    EXPECT_EQ(fooRes.GetValue(), true);
    EXPECT_EQ(badRes.GetValue(), false);
    EXPECT_EQ(quxRes.GetValue(), false);
    EXPECT_EQ(quxTxRes.GetValue(), true);
}

TEST(BatchRequestSuite, TestLock)
{
    TZeroWaitLockPollIntervalGuard g;

    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();
    auto otherTx = client->StartTransaction();
    client->Set(workingDir + "/exclusive", 5);
    client->Set(workingDir + "/shared", 5);
    client->Set(workingDir + "/snapshot", 5);
    client->Set(workingDir + "/locked", 5);
    otherTx->Lock(workingDir + "/locked", NYT::ELockMode::LM_EXCLUSIVE);

    client->Create(workingDir + "/dir/child", NYT::ENodeType::NT_MAP, NYT::TCreateOptions().Recursive(true));

    client->Create(workingDir + "/foo", NYT::ENodeType::NT_TABLE);
    client->Set(workingDir + "/foo/@attr", 42);


    auto batchRequest = client->CreateBatchRequest();
    auto badLockRes = batchRequest->Lock(workingDir + "/foo", NYT::ELockMode::LM_SHARED);
    auto exclusiveLockRes = batchRequest->WithTransaction(tx).Lock(workingDir + "/exclusive", NYT::ELockMode::LM_EXCLUSIVE);
    auto sharedLockRes = batchRequest->WithTransaction(tx).Lock(workingDir + "/shared", NYT::ELockMode::LM_SHARED);
    auto snapshotLockRes = batchRequest->WithTransaction(tx).Lock(workingDir + "/snapshot", NYT::ELockMode::LM_SNAPSHOT);

    auto childLockRes = batchRequest->WithTransaction(tx).Lock(
        workingDir + "/dir",
        NYT::ELockMode::LM_SHARED,
        NYT::TLockOptions().ChildKey("child"));
    auto attributeLockRes = batchRequest->WithTransaction(tx).Lock(
        workingDir + "/foo",
        NYT::ELockMode::LM_SHARED,
        NYT::TLockOptions().AttributeKey("attr"));

    auto waitableLockRes = batchRequest->WithTransaction(tx).Lock(
        workingDir + "/locked",
        NYT::ELockMode::LM_EXCLUSIVE,
        NYT::TLockOptions().Waitable(true));

    batchRequest->ExecuteBatch();

    EXPECT_THROW(badLockRes.GetValue(), TErrorResponse);

    auto getLockAttr = [&] (const ILockPtr& lock, const TString& attrName) {
        return client->Get("#" + GetGuidAsString(lock->GetId()) + "/@" + attrName).AsString();
    };
    EXPECT_EQ(
        getLockAttr(exclusiveLockRes.GetValue(), "mode"),
        "exclusive");
    EXPECT_EQ(
        getLockAttr(sharedLockRes.GetValue(), "mode"),
        "shared");
    EXPECT_EQ(
        getLockAttr(snapshotLockRes.GetValue(), "mode"),
        "snapshot");
    EXPECT_EQ(
        getLockAttr(childLockRes.GetValue(), "child_key"),
        "child");
    EXPECT_EQ(
        getLockAttr(attributeLockRes.GetValue(), "attribute_key"),
        "attr");
    EXPECT_EQ(
        getLockAttr(waitableLockRes.GetValue(), "state"),
        "pending");

    auto waitableAcquired = waitableLockRes.GetValue()->GetAcquiredFuture();

    //
    // 1 second wait should be enough to detect problems with our code.
    EXPECT_TRUE(!waitableAcquired.Wait(TDuration::Seconds(1)));

    otherTx->Abort();

    //
    // We wait here a little bit longer to avoid flaky results in autobuild.
    // (Occasionally autobuild is slow and we don't want to have flaky tests because of this).
    EXPECT_NO_THROW(waitableAcquired.GetValue(TDuration::Seconds(5)));
}

TEST(BatchRequestSuite, TestUnlock)
{
   TZeroWaitLockPollIntervalGuard g;

   TTestFixture fixture;
   auto client = fixture.GetClient();
   auto workingDir = fixture.GetWorkingDir();
   TYPath path = workingDir + "/node";

   client->Set("//sys/@config/cypress_manager/enable_unlock_command", true);

   client->Set(path, 1);

   auto tx = client->StartTransaction();
   tx->Lock(path, ELockMode::LM_EXCLUSIVE);

   auto batchRequest = client->CreateBatchRequest();
   auto unlockFuture = batchRequest->WithTransaction(tx).Unlock(path);

   auto otherTx = client->StartTransaction();
   EXPECT_THROW(otherTx->Set(path, 2), TErrorResponse);

   batchRequest->ExecuteBatch();
   EXPECT_NO_THROW(unlockFuture.GetValueSync());

   EXPECT_NO_THROW(otherTx->Set(path, 2));
   EXPECT_EQ(otherTx->Get(path).AsInt64(), 2);
}

TEST(BatchRequestSuite, TestWaitableLock)
{
    TZeroWaitLockPollIntervalGuard g;

    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    client->Set(workingDir + "/one", 1);
    client->Set(workingDir + "/two", 2);
    client->Set(workingDir + "/three", 3);
    client->Set(workingDir + "/four", 4);

    auto tx = client->StartTransaction();
    auto otherTx1 = client->StartTransaction();
    auto otherTx2 = client->StartTransaction();

    otherTx1->Lock(workingDir + "/one", LM_EXCLUSIVE);
    otherTx1->Lock(workingDir + "/three", LM_EXCLUSIVE);

    otherTx2->Lock(workingDir + "/two", LM_EXCLUSIVE);
    otherTx2->Lock(workingDir + "/four", LM_EXCLUSIVE);

    auto batchRequest = client->CreateBatchRequest();
    auto res1 = batchRequest->WithTransaction(tx).Lock(workingDir + "/one", LM_EXCLUSIVE, TLockOptions().Waitable(true));
    auto res2 = batchRequest->WithTransaction(tx).Lock(workingDir + "/two", LM_EXCLUSIVE, TLockOptions().Waitable(true));
    auto res3 = batchRequest->WithTransaction(tx).Lock(workingDir + "/three", LM_EXCLUSIVE, TLockOptions().Waitable(true));
    auto res4 = batchRequest->WithTransaction(tx).Lock(workingDir + "/four", LM_EXCLUSIVE, TLockOptions().Waitable(true));
    batchRequest->ExecuteBatch();

    EXPECT_TRUE(!res1.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));
    EXPECT_TRUE(!res2.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));
    EXPECT_TRUE(!res3.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));
    EXPECT_TRUE(!res4.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));

    otherTx1->Abort();

    EXPECT_NO_THROW(res1.GetValue()->GetAcquiredFuture().GetValue(TDuration::MilliSeconds(500)));
    EXPECT_TRUE(!res2.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));
    EXPECT_NO_THROW(res3.GetValue()->GetAcquiredFuture().GetValue(TDuration::MilliSeconds(500)));
    EXPECT_TRUE(!res4.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));

    otherTx2->Abort();

    EXPECT_NO_THROW(res2.GetValue()->GetAcquiredFuture().GetValue(TDuration::MilliSeconds(500)));
    EXPECT_NO_THROW(res4.GetValue()->GetAcquiredFuture().GetValue(TDuration::MilliSeconds(500)));
}

TEST(BatchRequestSuite, TestCreate)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();

    auto existingNodeId = client->Create(workingDir + "/existing_node", ENodeType::NT_MAP);

    auto batchRequest = client->CreateBatchRequest();
    auto mapNodeRes = batchRequest->Create(workingDir + "/map_node", ENodeType::NT_MAP);
    auto tableNodeRes = batchRequest->Create(workingDir + "/table_node", ENodeType::NT_TABLE);
    auto txTableNodeRes = batchRequest->WithTransaction(tx).Create(workingDir + "/tx_table_node", ENodeType::NT_TABLE);
    auto recursiveMapRes = batchRequest->Create(
        workingDir + "/recursive_map_node/table",
        ENodeType::NT_TABLE,
        TCreateOptions().Recursive(true));
    auto ignoreExistingRes = batchRequest->Create(
        workingDir + "/existing_node",
        ENodeType::NT_MAP,
        TCreateOptions().IgnoreExisting(true));
    auto nodeWithAttrRes = batchRequest->Create(workingDir + "/node_with_attr", ENodeType::NT_TABLE,
        TCreateOptions().Attributes(TNode()("attr_name", "attr_value")));

    auto badRes = batchRequest->Create(workingDir + "/unexisting_map_node/table", ENodeType::NT_TABLE);

    batchRequest->ExecuteBatch();

    auto checkNode = [] (IClientBasePtr client, const TString& path, const TString& expectedType, const TNodeId& expectedNodeId) {
        const auto actualId = client->Get(path + "/@id").AsString();
        EXPECT_EQ(actualId, GetGuidAsString(expectedNodeId));
        const auto actualType = client->Get(path + "/@type").AsString();
        EXPECT_EQ(actualType, expectedType);
    };

    checkNode(client, workingDir + "/map_node", "map_node", mapNodeRes.GetValue());
    checkNode(client, workingDir + "/table_node", "table", tableNodeRes.GetValue());
    checkNode(tx, workingDir + "/tx_table_node", "table", txTableNodeRes.GetValue());

    EXPECT_EQ(client->Exists(workingDir + "/tx_table_node"), false);
    checkNode(client, workingDir + "/recursive_map_node/table", "table", recursiveMapRes.GetValue());

    EXPECT_EQ(existingNodeId, ignoreExistingRes.GetValue());

    checkNode(client, workingDir + "/node_with_attr", "table", nodeWithAttrRes.GetValue());
    EXPECT_EQ(client->Get(workingDir + "/node_with_attr/@attr_name"), TNode("attr_value"));

    EXPECT_THROW(badRes.GetValue(), TErrorResponse);
}

TEST(BatchRequestSuite, TestRemove) {
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();

    client->Set(workingDir + "/one", 1);
    tx->Set(workingDir + "/tx_one", 1);
    tx->Set(workingDir + "/tx_two", 2);
    client->Create(workingDir + "/dir1/table", ENodeType::NT_TABLE, TCreateOptions().Recursive(true));
    client->Create(workingDir + "/dir2/table", ENodeType::NT_TABLE, TCreateOptions().Recursive(true));

    auto batchRequest = client->CreateBatchRequest();
    auto oneRes = batchRequest->Remove(workingDir + "/one");
    auto noTxRes = batchRequest->Remove(workingDir + "/tx_one");
    auto txRes = batchRequest->WithTransaction(tx).Remove(workingDir + "/tx_two");
    auto nonRecursiveRes = batchRequest->Remove(workingDir + "/dir1");
    auto recursiveRes = batchRequest->Remove(workingDir + "/dir2", TRemoveOptions().Recursive(true));
    auto unexistingRes = batchRequest->Remove(workingDir + "/unexisting-path");
    auto forceRes = batchRequest->Remove(workingDir + "/unexisting-path", TRemoveOptions().Force(true));

    batchRequest->ExecuteBatch();

    oneRes.GetValue(); // check no exception
    EXPECT_EQ(client->Exists(workingDir + "/one"), false);

    EXPECT_THROW(noTxRes.GetValue(), TErrorResponse);
    EXPECT_EQ(tx->Exists(workingDir + "/tx_one"), true);

    txRes.GetValue();
    EXPECT_EQ(tx->Exists(workingDir + "/tx_two"), false);

    EXPECT_THROW(nonRecursiveRes.GetValue(), TErrorResponse);
    EXPECT_EQ(tx->Exists(workingDir + "/dir1"), true);

    recursiveRes.GetValue();
    EXPECT_EQ(tx->Exists(workingDir + "/dir2"), false);

    EXPECT_THROW(unexistingRes.GetValue(), TErrorResponse);

    forceRes.GetValue();
}


using TCheckCopyMoveFunc = std::function<
    void(
        IClientBasePtr client,
        const TString& sourcePath,
        const TString& destinationPath,
        const TString& expectedContent,
        const TNodeId& nodeId)>;

template <typename TOptions, typename TMethod>
void TestCopyMove(
    const TMethod& copyMoveOp,
    const TCheckCopyMoveFunc& checkCopyMove)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();

    client->Set(workingDir + "/simple", "simple value");
    tx->Set(workingDir + "/tx_will_not_move", "tx_will_not_move value");
    tx->Set(workingDir + "/tx_simple", "tx_simple value");
    client->Set(workingDir + "/recursive_error", "recursive_error value");
    client->Set(workingDir + "/recursive", "recursive value");
    client->Set(workingDir + "/force_error", "force_error value");
    client->Set(workingDir + "/moved_force_error", "moved_force_error value");
    client->Set(workingDir + "/force", "force value");
    client->Set(workingDir + "/moved_force", "moved_force value");

    auto batchRequest = client->CreateBatchRequest();
    auto simpleRes = (batchRequest.Get()->*copyMoveOp)(workingDir + "/simple", workingDir + "/moved_simple", TOptions());
    auto noTxRes = (batchRequest.Get()->*copyMoveOp)(workingDir + "/tx_will_not_move", workingDir + "/moved_tx_will_not_move", TOptions());
    auto txSimpleRes = (batchRequest->WithTransaction(tx).*copyMoveOp)(workingDir + "/tx_simple", workingDir + "/moved_tx_simple", TOptions());
    auto recursiveErrorRes = (batchRequest.Get()->*copyMoveOp)(
        workingDir + "/recursive_error",
        workingDir + "/recursive_error_dir/moved_recursive_error",
        TOptions());
    auto recursiveRes = (batchRequest.Get()->*copyMoveOp)(
        workingDir + "/recursive",
        workingDir + "/recursive_dir/moved_recursive",
        TOptions().Recursive(true));
    auto forceErrorRes = (batchRequest.Get()->*copyMoveOp)(workingDir + "/force_error", workingDir + "/moved_force_error", TOptions());
    auto forceRes = (batchRequest.Get()->*copyMoveOp)(workingDir + "/force", workingDir + "/moved_force", TOptions().Force(true));

    batchRequest->ExecuteBatch();

    checkCopyMove(client, workingDir + "/simple", workingDir + "/moved_simple", "simple value", simpleRes.GetValue());

    EXPECT_THROW(noTxRes.GetValue(), TErrorResponse);
    EXPECT_EQ(tx->Get(workingDir + "/tx_will_not_move").AsString(), "tx_will_not_move value");

    checkCopyMove(tx, workingDir + "/tx_simple", workingDir + "/moved_tx_simple", "tx_simple value", txSimpleRes.GetValue());

    EXPECT_THROW(recursiveErrorRes.GetValue(), TErrorResponse);
    EXPECT_EQ(tx->Get(workingDir + "/recursive_error").AsString(), "recursive_error value");

    checkCopyMove(
        client,
        workingDir + "/recursive",
        workingDir + "/recursive_dir/moved_recursive",
        "recursive value",
        recursiveRes.GetValue());

    EXPECT_THROW(forceErrorRes.GetValue(), TErrorResponse);
    EXPECT_EQ(client->Get(workingDir + "/force_error").AsString(), "force_error value");
    EXPECT_EQ(client->Get(workingDir + "/moved_force_error").AsString(), "moved_force_error value");

    checkCopyMove(
        client,
        workingDir + "/force",
        workingDir + "/moved_force",
        "force value",
        forceRes.GetValue());
}

TEST(BatchRequestSuite, TestMove) {
    auto checkMoved = [] (
        IClientBasePtr client,
        const TString& sourcePath,
        const TString& destinationPath,
        const TString& expectedContent,
        const TNodeId& nodeId)
    {
        EXPECT_EQ(client->Exists(sourcePath), false);
        EXPECT_EQ(client->Get(destinationPath).AsString(), expectedContent);
        EXPECT_EQ(client->Get(destinationPath + "/@id").AsString(), GetGuidAsString(nodeId));
    };
    TestCopyMove<TMoveOptions>(&IBatchRequest::Move, checkMoved);
}

TEST(BatchRequestSuite, TestCopy) {
    const auto checkCopied = [] (
        IClientBasePtr client,
        const TString& sourcePath,
        const TString& destinationPath,
        const TString& expectedContent,
        const TNodeId& nodeId)
    {
        EXPECT_EQ(client->Get(sourcePath).AsString(), expectedContent);
        EXPECT_EQ(client->Get(destinationPath).AsString(), expectedContent);
        EXPECT_EQ(client->Get(destinationPath + "/@id").AsString(), GetGuidAsString(nodeId));
    };

    TestCopyMove<TCopyOptions>(&IBatchRequest::Copy, checkCopied);
}

TEST(BatchRequestSuite, TestLink) {
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto tx = client->StartTransaction();

    client->Set(workingDir + "/simple", 1);
    client->Link(workingDir + "/simple", workingDir + "/existing_link1");
    client->Link(workingDir + "/simple", workingDir + "/existing_link2");
    tx->Set(workingDir + "/tx_simple", 1);

    auto batchRequest = client->CreateBatchRequest();
    auto simpleRes = batchRequest->Link(workingDir + "/simple", workingDir + "/simple_link");
    auto txErrorRes = batchRequest->Link(workingDir + "/tx_simple", workingDir + "/tx_simple_broken_link///");
    auto txRes = batchRequest->WithTransaction(tx).Link(workingDir + "/tx_simple", workingDir + "/tx_simple_link");
    auto attributesRes = batchRequest->Link(workingDir + "/simple", workingDir + "/simple_link_with_attributes",
        TLinkOptions().Attributes(
            TNode()("attr_name", "attr_value")));
    auto noRecursiveRes = batchRequest->Link(workingDir + "/simple", workingDir + "/missing_dir/simple_link");
    auto recursiveRes = batchRequest->Link(workingDir + "/simple", workingDir + "/dir/simple_link",
        TLinkOptions().Recursive(true));
    auto noIgnoreExistingRes = batchRequest->Link(workingDir + "/simple", workingDir + "/existing_link1");
    auto ignoreExistingRes = batchRequest->Link(
        workingDir + "/simple", workingDir + "/existing_link2",
        TLinkOptions().IgnoreExisting(true));

    batchRequest->ExecuteBatch();

    auto checkLink = [] (
        IClientBasePtr client,
        const TString& targetPath,
        const TString& linkPath,
        const TNodeId& nodeId)
    {
        EXPECT_EQ(client->Get(linkPath + "&/@target_path").AsString(), targetPath);
        EXPECT_EQ(client->Get(linkPath + "&/@id").AsString(), GetGuidAsString(nodeId));
    };

    checkLink(client, workingDir + "/simple", workingDir + "/simple_link", simpleRes.GetValue());

    EXPECT_THROW(txErrorRes.GetValue(), TErrorResponse);
    EXPECT_EQ(client->Exists(workingDir + "/tx_simple_missing_link"), false);

    checkLink(tx, workingDir + "/tx_simple", workingDir + "/tx_simple_link", txRes.GetValue());

    checkLink(client, workingDir + "/simple", workingDir + "/simple_link_with_attributes", attributesRes.GetValue());

    EXPECT_EQ(
        client->Get(workingDir + "/simple_link_with_attributes&/@attr_name").AsString(),
        "attr_value");

    EXPECT_THROW(noRecursiveRes.GetValue(), TErrorResponse);
    EXPECT_EQ(client->Exists(workingDir + "/missing_dir"), false);

    checkLink(client, workingDir + "/simple", workingDir + "/dir/simple_link", recursiveRes.GetValue());

    EXPECT_THROW(noIgnoreExistingRes.GetValue(), TErrorResponse);
    ignoreExistingRes.GetValue(); // check it doesn't throw
}

TEST(BatchRequestSuite, TestCanonizeYPath) {
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto batchRequest = client->CreateBatchRequest();
    auto simpleRes = batchRequest->CanonizeYPath(TRichYPath("//foo/bar"));
    auto rangeRes = batchRequest->CanonizeYPath(TRichYPath("//foo/baz[#100500]"));
    auto formatSizeRes = batchRequest->CanonizeYPath(TRichYPath("//foo/qux[#100500]").Format("yson"));
    auto errorRes = batchRequest->CanonizeYPath(TRichYPath("//foo/nix[100500").Format("yson"));

    batchRequest->ExecuteBatch();

    EXPECT_EQ(simpleRes.GetValue().Path_, "//foo/bar");

    EXPECT_EQ(rangeRes.GetValue().Path_, "//foo/baz");
    EXPECT_EQ(rangeRes.GetValue().GetRanges().Defined(), true);
    EXPECT_EQ(std::ssize(*rangeRes.GetValue().GetRanges()), 1);

    EXPECT_EQ(formatSizeRes.GetValue().Format_, "yson");

    EXPECT_THROW(errorRes.GetValue(), TErrorResponse);
}

TGUID GetOrCreateUser(const IClientBasePtr& client, const TString& user)
{
    if (!client->Exists("//sys/users/" + user)) {
        return client->Create("", NT_USER,
            TCreateOptions().Attributes(TNode()("name", user)));
    }
    return GetGuid(client->Get("//sys/users/" + user + "/@id").AsString());
}

TEST(BatchRequestSuite, TestCheckPermission)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    TString user = "some_test_user";
    GetOrCreateUser(client, user);

    auto ace = TNode()
        ("subjects", TNode().Add(user))
        ("permissions", TNode().Add("read"))
        ("action", "allow");
    client->Create(workingDir + "/read_only", NT_MAP,  TCreateOptions()
        .Attributes(TNode()
            ("inherit_acl", false)
            ("acl", TNode().Add(ace))));

    auto batchRequest = client->CreateBatchRequest();

    auto readReadOnlyResult = batchRequest->CheckPermission(user, EPermission::Read, workingDir + "/read_only");
    auto writeReadOnlyResult = batchRequest->CheckPermission(user, EPermission::Write, workingDir + "/read_only");

    batchRequest->ExecuteBatch();

    EXPECT_EQ(readReadOnlyResult.GetValueSync().Action, ESecurityAction::Allow);
    EXPECT_EQ(writeReadOnlyResult.GetValueSync().Action, ESecurityAction::Deny);
}

TEST(BatchRequestSuite, TestYtPrefix) {
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TYtPrefixGuard guard(workingDir + "/");
    auto tx = client->StartTransaction();

    client->Set(workingDir + "/four", 4);
    client->Create(workingDir + "/dir/child", NYT::ENodeType::NT_MAP, TCreateOptions().Recursive(true));

    auto batchRequest = client->CreateBatchRequest();
    auto getRes = batchRequest->Get("four");
    auto setRes = batchRequest->Set("two", 5); // just for fun
    auto listRes = batchRequest->List("dir");
    auto lockRes = batchRequest->WithTransaction(tx).Lock("four", ELockMode::LM_SHARED);
    auto existsRes = batchRequest->Exists("four");

    batchRequest->ExecuteBatch();

    EXPECT_EQ(getRes.GetValue(), TNode(4));
    setRes.GetValue(); // check no exception here
    EXPECT_EQ(client->Get(workingDir + "/two"), TNode(5));
    EXPECT_EQ(listRes.GetValue(), TVector<TNode>({"child"}));
    lockRes.GetValue(); // check no exception here
    EXPECT_EQ(existsRes.GetValue(), true);
}

TEST(BatchRequestSuite, TestRequestReset) {
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto batchRequest = client->CreateBatchRequest();
    auto getRes = batchRequest->Create(workingDir + "/foo", ENodeType::NT_MAP);
    batchRequest->ExecuteBatch();

    getRes.GetValue(); // no exception

    EXPECT_THROW(batchRequest->Get(workingDir + "/foo"), yexception);
    EXPECT_THROW(batchRequest->ExecuteBatch(), yexception);
}

TEST(BatchRequestSuite, TestBatchPartMaxSize) {
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    {
        auto batchRequest = client->CreateBatchRequest();
        for (size_t i = 0; i < 100; ++i) {
            ::TStringBuilder path;
            path << workingDir + "/foo" << i;
            batchRequest->Set(path, i);
        }
        batchRequest->ExecuteBatch();
    }

    TVector<NThreading::TFuture<TNode>> results;
    auto batchRequest = client->CreateBatchRequest();
    for (size_t i = 0; i < 100; ++i) {
        ::TStringBuilder path;
        path << workingDir + "/foo" << i;
        results.push_back(batchRequest->Get(path));
    }
    batchRequest->ExecuteBatch(TExecuteBatchOptions().Concurrency(5).BatchPartMaxSize(7));

    for (size_t i = 0; i < 100; ++i) {
        EXPECT_EQ(results[i].GetValue(), TNode(i));
    }
}

TEST(BatchRequestSuite, TestBigRequest) {
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto batchRequest = client->CreateBatchRequest();
    const TString aaa(32 * 1024, 'a');
    const TString bbb(32 * 1024, 'b');
    const TString ccc(32 * 1024, 'c');
    const TString ddd(32 * 1024, 'd');
    auto resA = batchRequest->Set(workingDir + "/aaa", aaa);
    auto resB = batchRequest->Set(workingDir + "/bbb", bbb);
    auto resC = batchRequest->Set(workingDir + "/ccc", ccc);
    auto resD = batchRequest->Set(workingDir + "/ddd", ddd);
    batchRequest->ExecuteBatch();

    // Check no exceptions.
    resA.GetValue();
    resB.GetValue();
    resC.GetValue();
    resD.GetValue();

    EXPECT_EQ(client->Get(workingDir + "/aaa"), TNode(aaa));
    EXPECT_EQ(client->Get(workingDir + "/bbb"), TNode(bbb));
    EXPECT_EQ(client->Get(workingDir + "/ccc"), TNode(ccc));
    EXPECT_EQ(client->Get(workingDir + "/ddd"), TNode(ddd));
}

TEST(BatchRequestSuite, TestBigLoad) {
    TTestFixture fixture;
    auto rootClient = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->RetryInterval = TDuration();
    TConfig::Get()->RateLimitExceededRetryInterval = TDuration();
    TConfig::Get()->RetryCount = 1000;

    rootClient->Set(workingDir + "/node", 5);

    auto client = fixture.CreateClientForUser("test-big-load");
    TLowerRequestLimit lrl(rootClient, "test-big-load", 10);

    auto createBatch = [&client, &workingDir] {
        auto batchRequest = client->CreateBatchRequest();
        TVector<NThreading::TFuture<void>> results;
        for (size_t i = 0; i != 1000; ++i) {
            results.push_back(batchRequest->Set(workingDir + "/node", 5));
        }
        return std::tuple(batchRequest, results);
    };

    {
        auto [batchRequest, results] = createBatch();
        batchRequest->ExecuteBatch(TExecuteBatchOptions().Concurrency(1000));
        for (const auto& r : results) {
            r.GetValue();
        }
    }

    {
        TConfig::Get()->RetryCount = 1;
        auto [batchRequest, results] = createBatch();
        batchRequest->ExecuteBatch(TExecuteBatchOptions().Concurrency(10));
        for (const auto& r : results) {
            r.GetValue();
        }
    }
}

TEST(BatchRequestSuite, TestTransactionGet) {
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto tx = client->StartTransaction();

    auto batch = tx->CreateBatchRequest();
    batch->Set(workingDir + "/foo", 42);
    batch->ExecuteBatch();

    EXPECT_EQ(tx->Get(workingDir + "/foo"), 42);
    EXPECT_THROW(client->Get(workingDir + "/foo"), TErrorResponse);

}

} // namespace

////////////////////////////////////////////////////////////////////
