#include "lib.h"

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/http/error.h>
#include <mapreduce/yt/common/config.h>

#include <library/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/system/env.h>

using namespace NYT;
using namespace NYT::NTesting;

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
    TLowerRequestLimit(IClientPtr client, int value)
        : Client_(client)
        , RequestRateLimit_(Client_->Get("//sys/users/root/@request_rate_limit"))
        , RequestQueueSizeLimit_(Client_->Get("//sys/users/root/@request_queue_size_limit"))
    {
        Client_->Set("//sys/users/root/@request_rate_limit", value);
        Client_->Set("//sys/users/root/@request_queue_size_limit", value);
    }

    ~TLowerRequestLimit()
    {
        Client_->Set("//sys/users/root/@request_rate_limit", RequestRateLimit_);
        Client_->Set("//sys/users/root/@request_queue_size_limit", RequestQueueSizeLimit_);
    }

private:
    IClientPtr Client_;
    const TNode RequestRateLimit_;
    const TNode RequestQueueSizeLimit_;
};

////////////////////////////////////////////////////////////////////

static TNode::TList SortedStrings(TNode::TList input) {
    std::sort(input.begin(), input.end(), [] (const TNode& lhs, const TNode& rhs) {
        return lhs.AsString() < rhs.AsString();
    });
    return input;
}

////////////////////////////////////////////////////////////////////

SIMPLE_UNIT_TEST_SUITE(BatchRequestSuite)
{
    SIMPLE_UNIT_TEST(TestGet)
    {
        auto client = CreateTestClient();
        client->Set("//testing/foo", 5);
        client->Set("//testing/bar", "bar");

        auto tx = client->StartTransaction();
        tx->Set("//testing/qux", "gg");

        TBatchRequest batchRequest;
        auto fooRes = batchRequest.Get("//testing/foo", TGetOptions());
        auto barRes = batchRequest.Get("//testing/bar", TGetOptions());
        auto quxRes = batchRequest.Get("//testing/qux", TGetOptions());
        auto quxTxRes = batchRequest.WithTransaction(tx).Get("//testing/qux", TGetOptions());
        auto fooAccountRes = batchRequest.Get("//testing/foo",
            TGetOptions().AttributeFilter(TAttributeFilter().AddAttribute("account")));

        client->ExecuteBatch(batchRequest);
        UNIT_ASSERT_VALUES_EQUAL(fooRes.GetValue(), TNode(5));
        UNIT_ASSERT_VALUES_EQUAL(barRes.GetValue(), TNode("bar"));
        UNIT_ASSERT_EXCEPTION(quxRes.GetValue(), TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(quxTxRes.GetValue(), TNode("gg"));

        // Check that max_size options is passed correctly
        UNIT_ASSERT(fooAccountRes.GetValue().GetAttributes().HasKey("account"));
    }

    SIMPLE_UNIT_TEST(TestSet)
    {
        auto client = CreateTestClient();

        auto tx = client->StartTransaction();

        TBatchRequest batchRequest;
        auto fooRes = batchRequest.Set("//testing/foo", 5);
        auto barRes = batchRequest.Set("//testing/bar", "bar");
        auto quxTxRes = batchRequest.WithTransaction(tx).Set("//testing/qux", "gg");
        auto badRes = batchRequest.WithTransaction(tx).Set("//testing/unexisting/bad", "vzhukh");

        client->ExecuteBatch(batchRequest);

        fooRes.GetValue();
        barRes.GetValue();
        quxTxRes.GetValue();
        UNIT_ASSERT_EXCEPTION(badRes.GetValue(), TErrorResponse);

        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/foo"), TNode(5));
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/bar"), TNode("bar"));
        UNIT_ASSERT(!client->Exists("//testing/qux"));
        UNIT_ASSERT_VALUES_EQUAL(tx->Get("//testing/qux"), TNode("gg"));
    }

    SIMPLE_UNIT_TEST(TestList)
    {
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();

        client->Set("//testing/foo", 5);
        client->Set("//testing/bar", "bar");
        tx->Set("//testing/qux", "gg");

        TBatchRequest batchRequest;
        auto simpleRes = batchRequest.List("//testing");
        auto txRes = batchRequest.WithTransaction(tx).List("//testing");
        auto maxSizeRes = batchRequest.WithTransaction(tx).List("//testing",
            TListOptions().MaxSize(1));
        auto attributeFilterRes = batchRequest.WithTransaction(tx).List("//testing",
            TListOptions().AttributeFilter(TAttributeFilter().AddAttribute("account")));
        auto badRes = batchRequest.List("//testing/missing-dir");
        client->ExecuteBatch(batchRequest);

        UNIT_ASSERT_VALUES_EQUAL(
            SortedStrings(simpleRes.GetValue()),
            TNode::TList({"bar", "foo"}));

        UNIT_ASSERT_VALUES_EQUAL(
            SortedStrings(txRes.GetValue()),
            TNode::TList({"bar", "foo", "qux"}));
        UNIT_ASSERT_VALUES_EQUAL(maxSizeRes.GetValue().size(), 1);

        {
            const auto& attributes = attributeFilterRes.GetValue().at(0).GetAttributes();
            UNIT_ASSERT(attributes.HasKey("account"));
        }

        UNIT_ASSERT_EXCEPTION(badRes.GetValue(), TErrorResponse);
    }

    SIMPLE_UNIT_TEST(TestExists)
    {
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();

        client->Set("//testing/foo", 5);
        tx->Set("//testing/qux", "gg");

        TBatchRequest batchRequest;
        auto fooRes = batchRequest.Exists("//testing/foo");
        auto badRes = batchRequest.Exists("//testing/bad-unexisting-node");
        auto quxRes = batchRequest.Exists("//testing/qux");
        auto quxTxRes = batchRequest.WithTransaction(tx).Exists("//testing/qux");
        client->ExecuteBatch(batchRequest);

        UNIT_ASSERT_VALUES_EQUAL(fooRes.GetValue(), true);
        UNIT_ASSERT_VALUES_EQUAL(badRes.GetValue(), false);
        UNIT_ASSERT_VALUES_EQUAL(quxRes.GetValue(), false);
        UNIT_ASSERT_VALUES_EQUAL(quxTxRes.GetValue(), true);
    }

    SIMPLE_UNIT_TEST(TestLock)
    {
        TZeroWaitLockPollIntervalGuard g;

        auto client = CreateTestClient();
        auto tx = client->StartTransaction();
        auto otherTx = client->StartTransaction();
        client->Set("//testing/exclusive", 5);
        client->Set("//testing/shared", 5);
        client->Set("//testing/snapshot", 5);
        client->Set("//testing/locked", 5);
        otherTx->Lock("//testing/locked", NYT::ELockMode::LM_EXCLUSIVE);

        client->Create("//testing/dir/child", NYT::ENodeType::NT_MAP, NYT::TCreateOptions().Recursive(true));

        client->Create("//testing/foo", NYT::ENodeType::NT_TABLE);
        client->Set("//testing/foo/@attr", 42);


        TBatchRequest batchRequest;
        auto badLockRes = batchRequest.Lock("//testing/foo", NYT::ELockMode::LM_SHARED);
        auto exclusiveLockRes = batchRequest.WithTransaction(tx).Lock("//testing/exclusive", NYT::ELockMode::LM_EXCLUSIVE);
        auto sharedLockRes = batchRequest.WithTransaction(tx).Lock("//testing/shared", NYT::ELockMode::LM_SHARED);
        auto snapshotLockRes = batchRequest.WithTransaction(tx).Lock("//testing/snapshot", NYT::ELockMode::LM_SNAPSHOT);

        auto childLockRes = batchRequest.WithTransaction(tx).Lock(
            "//testing/dir",
            NYT::ELockMode::LM_SHARED,
            NYT::TLockOptions().ChildKey("child"));
        auto attributeLockRes = batchRequest.WithTransaction(tx).Lock(
            "//testing/foo",
            NYT::ELockMode::LM_SHARED,
            NYT::TLockOptions().AttributeKey("attr"));

        auto waitableLockRes = batchRequest.WithTransaction(tx).Lock(
            "//testing/locked",
            NYT::ELockMode::LM_EXCLUSIVE,
            NYT::TLockOptions().Waitable(true));

        client->ExecuteBatch(batchRequest);

        UNIT_ASSERT_EXCEPTION(badLockRes.GetValue(), TErrorResponse);

        auto getLockAttr = [&] (const ILockPtr& lock, const TString& attrName) {
            return client->Get("//sys/locks/" + GetGuidAsString(lock->GetId()) + "/@" + attrName).AsString();
        };
        UNIT_ASSERT_VALUES_EQUAL(
            getLockAttr(exclusiveLockRes.GetValue(), "mode"),
            "exclusive");
        UNIT_ASSERT_VALUES_EQUAL(
            getLockAttr(sharedLockRes.GetValue(), "mode"),
            "shared");
        UNIT_ASSERT_VALUES_EQUAL(
            getLockAttr(snapshotLockRes.GetValue(), "mode"),
            "snapshot");
        UNIT_ASSERT_VALUES_EQUAL(
            getLockAttr(childLockRes.GetValue(), "child_key"),
            "child");
        UNIT_ASSERT_VALUES_EQUAL(
            getLockAttr(attributeLockRes.GetValue(), "attribute_key"),
            "attr");
        UNIT_ASSERT_VALUES_EQUAL(
            getLockAttr(waitableLockRes.GetValue(), "state"),
            "pending");

        auto waitableAcquired = waitableLockRes.GetValue()->GetAcquiredFuture();
        UNIT_ASSERT(!waitableAcquired.Wait(TDuration::MilliSeconds(500)));

        otherTx->Abort();

        UNIT_ASSERT_NO_EXCEPTION(waitableAcquired.GetValue(TDuration::MilliSeconds(500)));
    }

    SIMPLE_UNIT_TEST(TestWaitableLock)
    {
        TZeroWaitLockPollIntervalGuard g;

        auto client = CreateTestClient();
        client->Set("//testing/one", 1);
        client->Set("//testing/two", 2);
        client->Set("//testing/three", 3);
        client->Set("//testing/four", 4);

        auto tx = client->StartTransaction();
        auto otherTx1 = client->StartTransaction();
        auto otherTx2 = client->StartTransaction();

        otherTx1->Lock("//testing/one", LM_EXCLUSIVE);
        otherTx1->Lock("//testing/three", LM_EXCLUSIVE);

        otherTx2->Lock("//testing/two", LM_EXCLUSIVE);
        otherTx2->Lock("//testing/four", LM_EXCLUSIVE);

        TBatchRequest batch;
        auto res1 = batch.WithTransaction(tx).Lock("//testing/one", LM_EXCLUSIVE, TLockOptions().Waitable(true));
        auto res2 = batch.WithTransaction(tx).Lock("//testing/two", LM_EXCLUSIVE, TLockOptions().Waitable(true));
        auto res3 = batch.WithTransaction(tx).Lock("//testing/three", LM_EXCLUSIVE, TLockOptions().Waitable(true));
        auto res4 = batch.WithTransaction(tx).Lock("//testing/four", LM_EXCLUSIVE, TLockOptions().Waitable(true));
        client->ExecuteBatch(batch);

        UNIT_ASSERT(!res1.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));
        UNIT_ASSERT(!res2.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));
        UNIT_ASSERT(!res3.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));
        UNIT_ASSERT(!res4.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));

        otherTx1->Abort();

        UNIT_ASSERT_NO_EXCEPTION(res1.GetValue()->GetAcquiredFuture().GetValue(TDuration::MilliSeconds(500)));
        UNIT_ASSERT(!res2.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));
        UNIT_ASSERT_NO_EXCEPTION(res3.GetValue()->GetAcquiredFuture().GetValue(TDuration::MilliSeconds(500)));
        UNIT_ASSERT(!res4.GetValue()->GetAcquiredFuture().Wait(TDuration::MilliSeconds(500)));

        otherTx2->Abort();

        UNIT_ASSERT_NO_EXCEPTION(res2.GetValue()->GetAcquiredFuture().GetValue(TDuration::MilliSeconds(500)));
        UNIT_ASSERT_NO_EXCEPTION(res4.GetValue()->GetAcquiredFuture().GetValue(TDuration::MilliSeconds(500)));
    }

    SIMPLE_UNIT_TEST(TestCreate)
    {
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();

        auto existingNodeId = client->Create("//testing/existing_node", ENodeType::NT_MAP);

        TBatchRequest batchRequest;
        auto mapNodeRes = batchRequest.Create("//testing/map_node", ENodeType::NT_MAP);
        auto tableNodeRes = batchRequest.Create("//testing/table_node", ENodeType::NT_TABLE);
        auto txTableNodeRes = batchRequest.WithTransaction(tx).Create("//testing/tx_table_node", ENodeType::NT_TABLE);
        auto recursiveMapRes = batchRequest.Create(
            "//testing/recursive_map_node/table",
            ENodeType::NT_TABLE,
            TCreateOptions().Recursive(true));
        auto ignoreExistingRes = batchRequest.Create(
            "//testing/existing_node",
            ENodeType::NT_MAP,
            TCreateOptions().IgnoreExisting(true));
        auto nodeWithAttrRes = batchRequest.Create("//testing/node_with_attr", ENodeType::NT_TABLE,
            TCreateOptions().Attributes(TNode()("attr_name", "attr_value")));

        auto badRes = batchRequest.Create("//testing/unexisting_map_node/table", ENodeType::NT_TABLE);

        client->ExecuteBatch(batchRequest);

        auto checkNode = [] (IClientBasePtr client, const TString& path, const TString& expectedType, const TNodeId& expectedNodeId) {
            const auto actualId = client->Get(path + "/@id").AsString();
            UNIT_ASSERT_VALUES_EQUAL(actualId, GetGuidAsString(expectedNodeId));
            const auto actualType = client->Get(path + "/@type").AsString();
            UNIT_ASSERT_VALUES_EQUAL(actualType, expectedType);
        };

        checkNode(client, "//testing/map_node", "map_node", mapNodeRes.GetValue());
        checkNode(client, "//testing/table_node", "table", tableNodeRes.GetValue());
        checkNode(tx, "//testing/tx_table_node", "table", txTableNodeRes.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/tx_table_node"), false);
        checkNode(client, "//testing/recursive_map_node/table", "table", recursiveMapRes.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(existingNodeId, ignoreExistingRes.GetValue());

        checkNode(client, "//testing/node_with_attr", "table", nodeWithAttrRes.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/node_with_attr/@attr_name"), TNode("attr_value"));

        UNIT_ASSERT_EXCEPTION(badRes.GetValue(), TErrorResponse);
    }

    SIMPLE_UNIT_TEST(TestRemove) {
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();

        client->Set("//testing/one", 1);
        tx->Set("//testing/tx_one", 1);
        tx->Set("//testing/tx_two", 2);
        client->Create("//testing/dir1/table", ENodeType::NT_TABLE, TCreateOptions().Recursive(true));
        client->Create("//testing/dir2/table", ENodeType::NT_TABLE, TCreateOptions().Recursive(true));

        TBatchRequest batchRequest;
        auto oneRes = batchRequest.Remove("//testing/one");
        auto noTxRes = batchRequest.Remove("//testing/tx_one");
        auto txRes = batchRequest.WithTransaction(tx).Remove("//testing/tx_two");
        auto nonRecursiveRes = batchRequest.Remove("//testing/dir1");
        auto recursiveRes = batchRequest.Remove("//testing/dir2", TRemoveOptions().Recursive(true));
        auto unexistingRes = batchRequest.Remove("//testing/unexisting-path");
        auto forceRes = batchRequest.Remove("//testing/unexisting-path", TRemoveOptions().Force(true));

        client->ExecuteBatch(batchRequest);

        oneRes.GetValue(); // check no exception
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/one"), false);

        UNIT_ASSERT_EXCEPTION(noTxRes.GetValue(), TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(tx->Exists("//testing/tx_one"), true);

        txRes.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(tx->Exists("//testing/tx_two"), false);

        UNIT_ASSERT_EXCEPTION(nonRecursiveRes.GetValue(), TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(tx->Exists("//testing/dir1"), true);

        recursiveRes.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(tx->Exists("//testing/dir2"), false);

        UNIT_ASSERT_EXCEPTION(unexistingRes.GetValue(), TErrorResponse);

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
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();

        client->Set("//testing/simple", "simple value");
        tx->Set("//testing/tx_will_not_move", "tx_will_not_move value");
        tx->Set("//testing/tx_simple", "tx_simple value");
        client->Set("//testing/recursive_error", "recursive_error value");
        client->Set("//testing/recursive", "recursive value");
        client->Set("//testing/force_error", "force_error value");
        client->Set("//testing/moved_force_error", "moved_force_error value");
        client->Set("//testing/force", "force value");
        client->Set("//testing/moved_force", "moved_force value");

        TBatchRequest batchRequest;
        auto simpleRes = (batchRequest.*copyMoveOp)("//testing/simple", "//testing/moved_simple", TOptions());
        auto noTxRes = (batchRequest.*copyMoveOp)("//testing/tx_will_not_move", "//testing/moved_tx_will_not_move", TOptions());
        auto txSimpleRes = (batchRequest.WithTransaction(tx).*copyMoveOp)("//testing/tx_simple", "//testing/moved_tx_simple", TOptions());
        auto recursiveErrorRes = (batchRequest.*copyMoveOp)(
            "//testing/recursive_error",
            "//testing/recursive_error_dir/moved_recursive_error",
            TOptions());
        auto recursiveRes = (batchRequest.*copyMoveOp)(
            "//testing/recursive",
            "//testing/recursive_dir/moved_recursive",
            TOptions().Recursive(true));
        auto forceErrorRes = (batchRequest.*copyMoveOp)("//testing/force_error", "//testing/moved_force_error", TOptions());
        auto forceRes = (batchRequest.*copyMoveOp)("//testing/force", "//testing/moved_force", TOptions().Force(true));

        client->ExecuteBatch(batchRequest);

        checkCopyMove(client, "//testing/simple", "//testing/moved_simple", "simple value", simpleRes.GetValue());

        UNIT_ASSERT_EXCEPTION(noTxRes.GetValue(), TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(tx->Get("//testing/tx_will_not_move").AsString(), "tx_will_not_move value");

        checkCopyMove(tx, "//testing/tx_simple", "//testing/moved_tx_simple", "tx_simple value", txSimpleRes.GetValue());

        UNIT_ASSERT_EXCEPTION(recursiveErrorRes.GetValue(), TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(tx->Get("//testing/recursive_error").AsString(), "recursive_error value");

        checkCopyMove(
            client,
            "//testing/recursive",
            "//testing/recursive_dir/moved_recursive",
            "recursive value",
            recursiveRes.GetValue());

        UNIT_ASSERT_EXCEPTION(forceErrorRes.GetValue(), TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/force_error").AsString(), "force_error value");
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/moved_force_error").AsString(), "moved_force_error value");

        checkCopyMove(
            client,
            "//testing/force",
            "//testing/moved_force",
            "force value",
            forceRes.GetValue());
    }

    SIMPLE_UNIT_TEST(TestMove) {
        auto checkMoved = [] (
            IClientBasePtr client,
            const TString& sourcePath,
            const TString& destinationPath,
            const TString& expectedContent,
            const TNodeId& nodeId)
        {
            UNIT_ASSERT_VALUES_EQUAL(client->Exists(sourcePath), false);
            UNIT_ASSERT_VALUES_EQUAL(client->Get(destinationPath).AsString(), expectedContent);
            UNIT_ASSERT_VALUES_EQUAL(client->Get(destinationPath + "/@id").AsString(), GetGuidAsString(nodeId));
        };
        TestCopyMove<TMoveOptions>(&IBatchRequest::Move, checkMoved);
    }

    SIMPLE_UNIT_TEST(TestCopy) {
        const auto checkCopied = [] (
            IClientBasePtr client,
            const TString& sourcePath,
            const TString& destinationPath,
            const TString& expectedContent,
            const TNodeId& nodeId)
        {
            UNIT_ASSERT_VALUES_EQUAL(client->Get(sourcePath).AsString(), expectedContent);
            UNIT_ASSERT_VALUES_EQUAL(client->Get(destinationPath).AsString(), expectedContent);
            UNIT_ASSERT_VALUES_EQUAL(client->Get(destinationPath + "/@id").AsString(), GetGuidAsString(nodeId));
        };

        TestCopyMove<TCopyOptions>(&IBatchRequest::Copy, checkCopied);
    }

    SIMPLE_UNIT_TEST(TestLink) {
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();

        client->Set("//testing/simple", 1);
        client->Link("//testing/simple", "//testing/existing_link1");
        client->Link("//testing/simple", "//testing/existing_link2");
        tx->Set("//testing/tx_simple", 1);

        TBatchRequest batchRequest;
        auto simpleRes = batchRequest.Link("//testing/simple", "//testing/simple_link");
        auto txErrorRes = batchRequest.Link("//testing/tx_simple", "//testing/tx_simple_missing_link");
        auto txRes = batchRequest.WithTransaction(tx).Link("//testing/tx_simple", "//testing/tx_simple_link");
        auto attributesRes = batchRequest.Link("//testing/simple", "//testing/simple_link_with_attributes",
            TLinkOptions().Attributes(
                TNode()("attr_name", "attr_value")));
        auto noRecursiveRes = batchRequest.Link("//testing/simple", "//testing/missing_dir/simple_link");
        auto recursiveRes = batchRequest.Link("//testing/simple", "//testing/dir/simple_link",
            TLinkOptions().Recursive(true));
        auto noIgnoreExistingRes = batchRequest.Link("//testing/simple", "//testing/existing_link1");
        auto ignoreExistingRes = batchRequest.Link(
            "//testing/simple", "//testing/existing_link2",
            TLinkOptions().IgnoreExisting(true));

        client->ExecuteBatch(batchRequest);

        auto checkLink = [] (
            IClientBasePtr client,
            const TString& targetPath,
            const TString& linkPath,
            const TNodeId& nodeId)
        {
            UNIT_ASSERT_VALUES_EQUAL(client->Get(linkPath + "&/@target_path").AsString(), targetPath);
            UNIT_ASSERT_VALUES_EQUAL(client->Get(linkPath + "&/@id").AsString(), GetGuidAsString(nodeId));
        };

        checkLink(client, "//testing/simple", "//testing/simple_link", simpleRes.GetValue());

        UNIT_ASSERT_EXCEPTION(txErrorRes.GetValue(), TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/tx_simple_missing_link"), false);

        checkLink(tx, "//testing/tx_simple", "//testing/tx_simple_link", txRes.GetValue());

        checkLink(client, "//testing/simple", "//testing/simple_link_with_attributes", attributesRes.GetValue());

        UNIT_ASSERT_VALUES_EQUAL(
            client->Get("//testing/simple_link_with_attributes&/@attr_name").AsString(),
            "attr_value");

        UNIT_ASSERT_EXCEPTION(noRecursiveRes.GetValue(), TErrorResponse);
        UNIT_ASSERT_VALUES_EQUAL(client->Exists("//testing/missing_dir"), false);

        checkLink(client, "//testing/simple", "//testing/dir/simple_link", recursiveRes.GetValue());

        UNIT_ASSERT_EXCEPTION(noIgnoreExistingRes.GetValue(), TErrorResponse);
        ignoreExistingRes.GetValue(); // check it doesn't throw
    }

    SIMPLE_UNIT_TEST(TestYtPrefix) {
        TYtPrefixGuard guard("//testing/");
        auto client = CreateTestClient();
        auto tx = client->StartTransaction();

        client->Set("//testing/four", 4);
        client->Create("//testing/dir/child", NYT::ENodeType::NT_MAP, TCreateOptions().Recursive(true));

        TBatchRequest batchRequest;
        auto getRes = batchRequest.Get("four");
        auto setRes = batchRequest.Set("two", 5); // just for fun
        auto listRes = batchRequest.List("dir");
        auto lockRes = batchRequest.WithTransaction(tx).Lock("four", ELockMode::LM_SHARED);
        auto existsRes = batchRequest.Exists("four");

        client->ExecuteBatch(batchRequest);

        UNIT_ASSERT_VALUES_EQUAL(getRes.GetValue(), TNode(4));
        setRes.GetValue(); // check no exception here
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/two"), TNode(5));
        UNIT_ASSERT_VALUES_EQUAL(listRes.GetValue(), yvector<TNode>({"child"}));
        lockRes.GetValue(); // check no exception here
        UNIT_ASSERT_VALUES_EQUAL(existsRes.GetValue(), true);
    }

    SIMPLE_UNIT_TEST(TestRequestReset) {
        auto client = CreateTestClient();
        TBatchRequest batchRequest;
        auto getRes = batchRequest.Create("//testing/foo", ENodeType::NT_MAP);
        client->ExecuteBatch(batchRequest);

        getRes.GetValue(); // no exception

        UNIT_ASSERT_EXCEPTION(batchRequest.Get("//testing/foo"), yexception);
        UNIT_ASSERT_EXCEPTION(client->ExecuteBatch(batchRequest), yexception);
    }

    SIMPLE_UNIT_TEST(TestBatchPartMaxSize) {
        auto client = CreateTestClient();
        {
            TBatchRequest batchRequest;
            for (size_t i = 0; i < 100; ++i) {
                TStringBuilder path;
                path << "//testing/foo" << i;
                batchRequest.Set(path, i);
            }
            client->ExecuteBatch(batchRequest);
        }

        yvector<NThreading::TFuture<TNode>> results;
        TBatchRequest batchRequest;
        for (size_t i = 0; i < 100; ++i) {
            TStringBuilder path;
            path << "//testing/foo" << i;
            results.push_back(batchRequest.Get(path));
        }
        client->ExecuteBatch(batchRequest, TExecuteBatchOptions().Concurrency(5).BatchPartMaxSize(7));

        for (size_t i = 0; i < 100; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(results[i].GetValue(), TNode(i));
        }
    }

    SIMPLE_UNIT_TEST(TestBigRequest) {
        auto client = CreateTestClient();
        TBatchRequest batchRequest;
        const TString aaa(32 * 1024, 'a');
        const TString bbb(32 * 1024, 'b');
        const TString ccc(32 * 1024, 'c');
        const TString ddd(32 * 1024, 'd');
        auto resA = batchRequest.Set("//testing/aaa", aaa);
        auto resB = batchRequest.Set("//testing/bbb", bbb);
        auto resC = batchRequest.Set("//testing/ccc", ccc);
        auto resD = batchRequest.Set("//testing/ddd", ddd);
        client->ExecuteBatch(batchRequest);

        // Check no exceptions.
        resA.GetValue();
        resB.GetValue();
        resC.GetValue();
        resD.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/aaa"), TNode(aaa));
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/bbb"), TNode(bbb));
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/ccc"), TNode(ccc));
        UNIT_ASSERT_VALUES_EQUAL(client->Get("//testing/ddd"), TNode(ddd));
    }

    SIMPLE_UNIT_TEST(TestBigLoad) {
        auto client = CreateTestClient();
        TLowerRequestLimit lrl(client, 100);
        TConfig::Get()->RetryInterval = TDuration();
        TConfig::Get()->RateLimitExceededRetryInterval = TDuration();

        client->Set("//testing/node", 5);
        TBatchRequest batchRequest;
        yvector<NThreading::TFuture<TNode>> results;
        for (size_t i = 0; i != 500; ++i) {
            results.push_back(batchRequest.Get("//testing/node"));
        }
        client->ExecuteBatch(batchRequest, TExecuteBatchOptions().Concurrency(500));

        for (const auto& r : results) {
            r.GetValue();
        }
    }
}

////////////////////////////////////////////////////////////////////
