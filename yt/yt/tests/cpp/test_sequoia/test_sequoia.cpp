#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/sequoia_client/public.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const auto Logger = CppTestsLogger;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaTestBase
    : public TApiTestBase
{
public:
    void SetUp() override
    {
        TApiTestBase::SetUp();

        WaitFor(Client_->CreateNode("//sequoia", EObjectType::Rootstock))
            .ValueOrThrow();
    }

    void TearDown() override
    {
        auto baseTearDown = Finally([this] {
            TApiTestBase::TearDown();
        });

        AbortCypressTransactions();

        RemoveScion();
    }

    static NApi::ITransactionPtr StartCypressTransaction(const NApi::TTransactionStartOptions& options = {})
    {
        auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();

        YT_LOG_DEBUG("Cypress transaction started (TransactionId: %v)", tx->GetId());

        return tx;
    }

private:
    static void RemoveScion()
    {
        WaitFor(Client_->RemoveNode("//sequoia", NApi::TRemoveNodeOptions{
            .Recursive = true,
            .Force = true,
        }))
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSequoiaTest
    : public TSequoiaTestBase
{ };

std::string Prettify(const TYsonString& yson)
{
    return ConvertToYsonString(yson, EYsonFormat::Pretty).ToString();
}

std::string Prettify(TYsonStringBuf yson)
{
    return Prettify(TYsonString(yson));
}

#define YSON_EXPECT_EQ(lhs, rhs) EXPECT_EQ(Prettify(lhs), Prettify(rhs))
#define YSON_ASSERT_EQ(lhs, rhs) ASSERT_EQ(Prettify(lhs), Prettify(rhs))

TEST_F(TSequoiaTest, TestScionCreated)
{
    auto scionId = ConvertTo<TObjectId>(WaitFor(Client_->GetNode("//sequoia/@id"))
        .ValueOrThrow());

    ASSERT_EQ(TypeFromId(scionId), EObjectType::Scion);
}

TEST_F(TSequoiaTest, TestCreateMapNode)
{
    auto nodeId = WaitFor(Client_->CreateNode(
        "//sequoia/a/b",
        EObjectType::MapNode,
        NApi::TCreateNodeOptions{.Recursive = true,}))
        .ValueOrThrow();

    EXPECT_TRUE(IsSequoiaId(nodeId));
    EXPECT_EQ(TypeFromId(nodeId), EObjectType::SequoiaMapNode);

    auto rsp = WaitFor(Client_->GetNode("//sequoia/a"))
        .ValueOrThrow();

    YSON_EXPECT_EQ(TYsonStringBuf("{b = {}}"), rsp);
}

TEST_F(TSequoiaTest, TestRowLockConflict)
{
    constexpr static int ThreadCount = 5;
    constexpr static int RequestCount = 25;

    WaitFor(Client_->CreateNode("//sequoia/map", EObjectType::MapNode))
        .ValueOrThrow();

    auto requestFutures = std::vector<TFuture<void>>(RequestCount);

    auto barrierPromise = NewPromise<void>();

    auto threadPool = CreateThreadPool(ThreadCount, "ConcurrentNodeCreation");
    for (int requestIndex : std::views::iota(0, RequestCount)) {
        requestFutures[requestIndex] = BIND([
            barrierFuture = barrierPromise.ToFuture()
        ] {
            WaitForFast(barrierFuture)
                .ThrowOnError();

            WaitFor(Client_->CreateNode("//sequoia/map/child", EObjectType::MapNode))
                .ThrowOnError();
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run();
    }

    barrierPromise.Set();

    auto results = WaitFor(AllSet(requestFutures))
        .ValueOrThrow();

    int createdNodeCount = 0;
    int lockConflictCount = 0;
    int nodeExistsCount = 0;
    for (const auto& error : results) {
        if (error.IsOK()) {
            ++createdNodeCount;
        } else if (error.GetCode() == NSequoiaClient::EErrorCode::SequoiaRetriableError &&
            !error.InnerErrors().empty() &&
            error.InnerErrors().front().GetCode() == NTabletClient::EErrorCode::TransactionLockConflict)
        {
            ++lockConflictCount;
        } else if (error.FindMatching([] (const TError& error) {
            return error.GetMessage().contains("already exists");
        })) {
            ++nodeExistsCount;
        } else {
            // NB: In case of "socket closed" error all suite hangs. It's better
            // just crash it on unexcepted error.
            GTEST_FAIL() << Format("Unexpected error: %v", error).c_str();
        }
    }

    YT_LOG_DEBUG(
        "Concurrent node creations are finished (RequestCount: %v, CreatedNodeCount: %v, "
        "LockConflictCount: %v, AlreadyExistsErrorCount: %v)",
        RequestCount,
        createdNodeCount,
        lockConflictCount,
        nodeExistsCount);

    // TODO(kvk1920): implement per-request option to disable Sequoia retries
    // and rewrite this test in the following way: if Sequoia retries are
    // disabled we should observe lock conflicts here.

    EXPECT_EQ(createdNodeCount, 1);
    EXPECT_EQ(lockConflictCount, 0);
    EXPECT_EQ(createdNodeCount + lockConflictCount + nodeExistsCount, RequestCount);
}

TEST_F(TSequoiaTest, TestCypressTransactionSimple)
{
    auto topmostTx = StartCypressTransaction();
    auto nestedTx = StartCypressTransaction({.ParentId = topmostTx->GetId()});
    auto dependentTx = StartCypressTransaction({.PrerequisiteTransactionIds = {topmostTx->GetId()}});

    EXPECT_TRUE(IsSequoiaId(topmostTx->GetId()));
    EXPECT_TRUE(IsSequoiaId(nestedTx->GetId()));
    EXPECT_TRUE(IsSequoiaId(dependentTx->GetId()));

    EXPECT_TRUE(WaitFor(Client_->NodeExists(Format("//sys/transactions/%v", topmostTx->GetId()))).ValueOrThrow());
    EXPECT_TRUE(WaitFor(Client_->NodeExists(Format("//sys/transactions/%v", nestedTx->GetId()))).ValueOrThrow());
    EXPECT_TRUE(WaitFor(Client_->NodeExists(Format("//sys/transactions/%v", dependentTx->GetId()))).ValueOrThrow());

    YSON_EXPECT_EQ(WaitFor(Client_->GetNode(Format("#%v/@cypress_transaction", topmostTx->GetId())))
        .ValueOrThrow(),
        TYsonStringBuf("%true"));

    YSON_EXPECT_EQ(WaitFor(Client_->GetNode(Format("#%v/@cypress_transaction", nestedTx->GetId())))
        .ValueOrThrow(),
        TYsonStringBuf("%true"));

    YSON_EXPECT_EQ(WaitFor(Client_->GetNode(Format("#%v/@cypress_transaction", dependentTx->GetId())))
        .ValueOrThrow(),
        TYsonStringBuf("%true"));

    WaitFor(topmostTx->Abort())
        .ThrowOnError();

    EXPECT_FALSE(WaitFor(Client_->NodeExists(Format("//sys/transactions/%v", topmostTx->GetId()))).ValueOrThrow());
    EXPECT_FALSE(WaitFor(Client_->NodeExists(Format("//sys/transactions/%v", nestedTx->GetId()))).ValueOrThrow());
    EXPECT_FALSE(WaitFor(Client_->NodeExists(Format("//sys/transactions/%v", dependentTx->GetId()))).ValueOrThrow());

    YT_LOG_DEBUG("All 3 transactions are aborted");

    // All these transactions are expected to be aborted in TearDownTestCase().
    auto tx1 = StartCypressTransaction({.AutoAbort = false});
    auto tx2 = StartCypressTransaction({.ParentId = tx1->GetId(), .AutoAbort = false});
}

TEST_F(TSequoiaTest, TestConcurrentCommitTx)
{
    constexpr auto ChildCount = 4;
    constexpr auto LevelCount = 4;

    auto barrierPromise = NewPromise<void>();
    auto barrier = barrierPromise.ToFuture();
    auto threadPool = CreateThreadPool(4, "ConcurrentTxCommit");

    std::vector<ITransactionPtr> transactions{StartCypressTransaction()};
    std::vector<TFuture<void>> commitFutures;

    std::atomic<int> inFlightRequests = 0;
    std::atomic<int> finishedRequests = 0;

    auto scheduleCommit = [&] () {
        YT_VERIFY(transactions.size() == commitFutures.size() + 1);

        auto txIndex = transactions.size() - 1;

        auto commitFuture = BIND([barrier, txIndex, &transactions, &inFlightRequests, &finishedRequests] {
            WaitFor(barrier)
                .ThrowOnError();

            YT_LOG_DEBUG(
                "Request started (TransactionId: %v, InFlightRequestCount: %v)",
                transactions[txIndex]->GetId(),
                inFlightRequests.fetch_add(1, std::memory_order::relaxed) + 1);

            auto finally = Finally([&] {
                YT_LOG_DEBUG(
                    "Request finished (TransactionId: %v, InFlightRequestCount: %v, FinishedRequestCount: %v)",
                    transactions[txIndex]->GetId(),
                    inFlightRequests.fetch_sub(1, std::memory_order::relaxed) - 1,
                    finishedRequests.fetch_add(1, std::memory_order::relaxed) + 1);
            });

            WaitFor(transactions[txIndex]->Commit())
                .ThrowOnError();
        }).AsyncVia(threadPool->GetInvoker()).Run();

        commitFutures.push_back(std::move(commitFuture));
    };

    scheduleCommit();

    int lastLevelBegin = 0;
    int lastLevelEnd = 1;
    for (int _ : std::views::iota(1, LevelCount)) {
        auto nextLevelBegin = lastLevelEnd;
        for (int txIndex : std::views::iota(lastLevelBegin, lastLevelEnd)) {
            for (int _ : std::views::iota(0, ChildCount)) {
                transactions.emplace_back(StartCypressTransaction({.ParentId = transactions[txIndex]->GetId()}));
                scheduleCommit();
            }
        }
        lastLevelBegin = nextLevelBegin;
        lastLevelEnd = transactions.size();
    }

    barrierPromise.Set();

    WaitFor(AllSet(commitFutures))
        .ThrowOnError();

    // At least topmost tx should be committed.
    EXPECT_TRUE(commitFutures.front().TryGet()->IsOK());

    int committed = 0;
    int alreadyAborted = 0;

    for (const auto& commitFuture : commitFutures) {
        auto result = *commitFuture.TryGet();
        if (result.IsOK()) {
            ++committed;
        } else if (result.FindMatching([] (const TError& error) { return error.GetMessage().contains("No such transaction"); })) {
            ++alreadyAborted;
        } else {
            GTEST_FAIL() << Format("Unexpected error: %v", result);
        }
    }

    YT_LOG_DEBUG("All transactions finished (Committed: %v, AlreadyAborted: %v)", committed, alreadyAborted);
}

TEST_F(TSequoiaTest, TestParallelWriteActionsWithPrerequisiteTx)
{
    constexpr static int ThreadCount = 3;
    constexpr static int RequestCount = 20;

    WaitFor(Client_->CreateNode("//sequoia/tmp", EObjectType::MapNode))
        .ThrowOnError();
    WaitFor(Client_->SetNode(
            "//sys/cypress_proxies/@config/object_service/enable_fast_path_prerequisite_transaction_check",
            ConvertToYsonString(false)))
        .ThrowOnError();

    auto threadPool = CreateThreadPool(ThreadCount, "ConcurrentWriteActionsWithPrerequisiteTx");
    auto barrierPromise = NewPromise<void>();
    std::atomic<int> timestamp = 0;

    auto prerequisiteTx = StartCypressTransaction();
    NApi::TCreateNodeOptions options;
    options.PrerequisiteTransactionIds = std::vector<TTransactionId>{prerequisiteTx->GetId()};

    auto requestFutures = std::vector<TFuture<std::tuple<int, int, TError>>>(RequestCount);
    for (int requestIndex : std::views::iota(0, std::ssize(requestFutures))) {
        requestFutures[requestIndex] = BIND([
            &timestamp,
            requestIndex,
            options,
            barrierFuture = barrierPromise.ToFuture()
        ] () -> std::tuple<int, int, TError> {
            WaitForFast(barrierFuture)
                .ThrowOnError();
            auto startTimestamp = timestamp.fetch_add(1, std::memory_order::acquire);
            auto error = WaitFor(Client_->CreateNode(Format("//sequoia/tmp/node-%v", requestIndex), EObjectType::MapNode, options));
            auto endTimestamp = timestamp.fetch_add(1, std::memory_order::release);
            return {startTimestamp, endTimestamp, error};
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run();
    }

    requestFutures.push_back(
        BIND([
            &timestamp,
            barrierFuture = barrierPromise.ToFuture(),
            prerequisiteTx
        ] () -> std::tuple<int, int, TError> {
            WaitForFast(barrierFuture)
                .ThrowOnError();

            auto startTimestamp = timestamp.fetch_add(1, std::memory_order::acquire);
            auto error = WaitFor(prerequisiteTx->Commit());
            auto endTimestamp = timestamp.fetch_add(1, std::memory_order::release);
            YT_LOG_DEBUG(error, "Request executed (StartTimestamp: %v, EndTimestamp: %v)", startTimestamp, endTimestamp);
            return {startTimestamp, endTimestamp, error};
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run());

    barrierPromise.Set();

    auto results = WaitFor(AllSet(std::move(requestFutures)))
        .ValueOrThrow();
    auto commitResult = results.back();
    YT_VERIFY(commitResult.IsOK());
    auto [startCommitTimestamp, endCommitTimestamp, commitError] = commitResult.Value();
    YT_VERIFY(commitError.IsOK());

    YT_LOG_DEBUG(
        "Commit request (StartTimestamp: %v, EndTimestamp: %v)",
        startCommitTimestamp,
        endCommitTimestamp);

    auto createdNodeCount = 0;
    auto prerequisiteCheckFailedCount = 0;
    auto leaseIssueFailedCount = 0;
    for (int requestIndex : std::views::iota(0, RequestCount)) {
        auto result = results[requestIndex];
        YT_VERIFY(result.IsOK());
        auto [startTimestamp, endTimestamp, error] = result.Value();
        YT_LOG_DEBUG(
            "Create request (RequestIndex: %v, StartTimestamp: %v, EndTimestamp: %v)",
            requestIndex,
            startTimestamp,
            endTimestamp);

        if (error.IsOK()) {
            YT_VERIFY(startTimestamp < endCommitTimestamp);
            ++createdNodeCount;
        } else if (error.GetCode() == NObjectClient::EErrorCode::PrerequisiteCheckFailed) {
            YT_VERIFY(startCommitTimestamp < endTimestamp);
            ++prerequisiteCheckFailedCount;
        } else if (error.GetMessage().contains("Failed to issue leases for prerequisite transactions")) {
            ++leaseIssueFailedCount;
        } else {
            YT_LOG_FATAL(error, "Unexpected error");
        }
    }

    YT_LOG_DEBUG(
        "Creations with prerequisite transaction are finished "
        "(CreatedNodeCount: %v, PrerequisiteCheckFailedCount: %v, LeaseIssueFailedCount: %v, PrerequisiteTransactionId: %v)",
        createdNodeCount,
        prerequisiteCheckFailedCount,
        leaseIssueFailedCount,
        prerequisiteTx->GetId());

    EXPECT_FALSE(WaitFor(Client_->NodeExists(Format("//sys/transactions/%v", prerequisiteTx->GetId()))).ValueOrThrow());

    auto error = WaitFor(Client_->CreateNode("//sequoia/tmp/a", EObjectType::MapNode, options));
    YT_VERIFY(!error.IsOK());
    // Request should fail since it was executed after commit of the prerequisite tx happened.
    YT_VERIFY(error.GetCode() == NObjectClient::EErrorCode::PrerequisiteCheckFailed);

    WaitFor(Client_->SetNode(
        "//sys/cypress_proxies/@config/object_service/enable_fast_path_prerequisite_transactions_check",
        ConvertToYsonString(true)))
        .ThrowOnError();
}

TEST_F(TSequoiaTest, TestParallelReadActionsWithPrerequisiteTx)
{
    constexpr static int ThreadCount = 3;
    constexpr static int RequestCount = 20;

    WaitFor(Client_->CreateNode("//sequoia/tmp", EObjectType::MapNode))
        .ThrowOnError();
    WaitFor(Client_->SetNode(
            "//sys/cypress_proxies/@config/object_service/enable_fast_path_prerequisite_transaction_check",
            ConvertToYsonString(false)))
        .ThrowOnError();

    auto threadPool = CreateThreadPool(ThreadCount, "ConcurrentReadActionsWithPrerequisiteTx");
    auto barrierPromise = NewPromise<void>();
    std::atomic<int> timestamp = 0;

    // Create nodes that will be listed.
    {
        std::vector<TFuture<void>> futures;
        for (int requestIndex : std::views::iota(0, RequestCount)) {
            futures.push_back(Client_->CreateNode(Format("//sequoia/tmp/node-%v",requestIndex), EObjectType::MapNode).AsVoid());
        }
        WaitFor(AllSucceeded(std::move(futures)))
            .ThrowOnError();
    }

    auto prerequisiteTx = StartCypressTransaction();
    NApi::TListNodeOptions options;
    options.PrerequisiteTransactionIds = std::vector<TTransactionId>{prerequisiteTx->GetId()};

    auto requestFutures = std::vector<TFuture<std::tuple<int, int, TError>>>(RequestCount);
    for (int requestIndex : std::views::iota(0, std::ssize(requestFutures))) {
        requestFutures[requestIndex] = BIND([
            &timestamp,
            requestIndex,
            options,
            barrierFuture = barrierPromise.ToFuture()
        ] () -> std::tuple<int, int, TError> {
            WaitForFast(barrierFuture)
                .ThrowOnError();
            auto startTimestamp = timestamp.fetch_add(1, std::memory_order::acquire);
            auto error = WaitFor(Client_->ListNode(Format("//sequoia/tmp/node-%v", requestIndex), options));
            auto endTimestamp = timestamp.fetch_add(1, std::memory_order::release);
            return {startTimestamp, endTimestamp, error};
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run();
    }

    requestFutures.push_back(
        BIND([
            &timestamp,
            barrierFuture = barrierPromise.ToFuture(),
            prerequisiteTx
        ] () -> std::tuple<int, int, TError> {
            WaitForFast(barrierFuture)
                .ThrowOnError();

            auto startTimestamp = timestamp.fetch_add(1, std::memory_order::acquire);
            auto error = WaitFor(prerequisiteTx->Commit());
            auto endTimestamp = timestamp.fetch_add(1, std::memory_order::release);
            YT_LOG_DEBUG(error, "Request executed (StartTimestamp: %v, EndTimestamp: %v)", startTimestamp, endTimestamp);
            return {startTimestamp, endTimestamp, error};
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run());

    barrierPromise.Set();

    auto results = WaitFor(AllSet(std::move(requestFutures)))
        .ValueOrThrow();
    auto commitResult = results.back();
    YT_VERIFY(commitResult.IsOK());
    auto [startCommitTimestamp, endCommitTimestamp, commitError] = commitResult.Value();
    YT_VERIFY(commitError.IsOK());

    YT_LOG_DEBUG(
        "Commit request (StartTimestamp: %v, EndTimestamp: %v)",
        startCommitTimestamp,
        endCommitTimestamp);

    int listedNodeCount = 0, prerequisiteCheckFailedCount = 0;
    for (int requestIndex : std::views::iota(0, RequestCount)) {
        auto result = results[requestIndex];
        YT_VERIFY(result.IsOK());
        auto [startTimestamp, endTimestamp, error] = result.Value();
        YT_LOG_DEBUG(
            "Create request (RequestIndex: %v, StartTimestamp: %v, EndTimestamp: %v)",
            requestIndex,
            startTimestamp,
            endTimestamp);

        if (error.IsOK()) {
            YT_VERIFY(startTimestamp < endCommitTimestamp);
            ++listedNodeCount;
        } else if (error.GetCode() == NObjectClient::EErrorCode::PrerequisiteCheckFailed) {
            YT_VERIFY(startCommitTimestamp < endTimestamp);
            ++prerequisiteCheckFailedCount;
        } else {
            YT_LOG_FATAL(error, "Unexpected error");
        }
    }

    YT_LOG_DEBUG(
        "Lists with prerequisite transaction are finished "
        "(ListedNodeCount: %v, PrerequisiteCheckFailedCount: %v, PrerequisiteTransactionId: %v)",
        listedNodeCount,
        prerequisiteCheckFailedCount,
        prerequisiteTx->GetId());

    EXPECT_FALSE(WaitFor(Client_->NodeExists(Format("//sys/transactions/%v", prerequisiteTx->GetId()))).ValueOrThrow());

    auto error = WaitFor(Client_->ListNode("//sequoia/tmp", options));
    YT_VERIFY(!error.IsOK());
    // Request should fail since it was executed after commit of the prerequisite tx happened.
    YT_VERIFY(error.GetCode() == NObjectClient::EErrorCode::PrerequisiteCheckFailed);

    WaitFor(Client_->SetNode(
            "//sys/cypress_proxies/@config/object_service/enable_fast_path_prerequisite_transactions_check",
            ConvertToYsonString(true)))
        .ThrowOnError();
}

TPrerequisiteRevisionConfigPtr GetNodeRevision(const TYPath& path, const IClientPtr& client)
{
    auto ysonRevision = WaitFor(client->GetNode(Format("%v/@revision", path))).ValueOrThrow();
    auto nodeRevision = ConvertToNode(ysonRevision);

    auto prerequisiteRevision = New<TPrerequisiteRevisionConfig>();
    Deserialize(prerequisiteRevision->Revision, nodeRevision);
    prerequisiteRevision->Path = path;
    return prerequisiteRevision;
}

TEST_F(TSequoiaTest, TestManyCopyReqsWithPrerequisiteRevisionAndOneSet)
{
    constexpr static int ThreadCount = 3;
    constexpr static int RequestCount = 20;

    WaitFor(Client_->CreateNode("//sequoia/tmp", EObjectType::MapNode))
        .ThrowOnError();
    WaitFor(Client_->SetNode("//sequoia/tmp/table", ConvertToYsonString("value")))
        .ThrowOnError();

    auto threadPool = CreateThreadPool(ThreadCount, "ManyCopyReqsAndOneSet");
    auto barrierPromise = NewPromise<void>();
    std::atomic<int> timestamp = 0;

    TCopyNodeOptions options;
    options.PrerequisiteRevisions = {
        GetNodeRevision("//sequoia/tmp/table", Client_),
    };

    auto requestFutures = std::vector<TFuture<std::tuple<int, int, TError>>>(RequestCount);
    for (int requestIndex : std::views::iota(0, std::ssize(requestFutures))) {
        requestFutures[requestIndex] = BIND([
            &timestamp,
            options,
            requestIndex,
            barrierFuture = barrierPromise.ToFuture()
        ] () -> std::tuple<int, int, TError> {
            WaitForFast(barrierFuture)
                .ThrowOnError();

            auto startTimestamp = timestamp.fetch_add(1, std::memory_order::acquire);
            auto error = WaitFor(Client_->CopyNode("//sequoia/tmp/table", Format("//sequoia/tmp/table-%v", requestIndex), options));
            auto endTimestamp = timestamp.fetch_add(1, std::memory_order::release);
            return {startTimestamp, endTimestamp, error};
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run();
    }

    requestFutures.push_back(
        BIND([
            &timestamp,
            barrierFuture = barrierPromise.ToFuture()
        ] () -> std::tuple<int, int, TError> {
            WaitForFast(barrierFuture)
                .ThrowOnError();

            auto startTimestamp = timestamp.fetch_add(1, std::memory_order::acquire);
            auto error = WaitFor(Client_->SetNode("//sequoia/tmp/table", ConvertToYsonString("another value")));
            auto endTimestamp = timestamp.fetch_add(1, std::memory_order::release);
            return {startTimestamp, endTimestamp, error};
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run());

    barrierPromise.Set();

    auto results = WaitFor(AllSet(std::move(requestFutures)))
        .ValueOrThrow();
    auto setResult = results.back();
    YT_VERIFY(setResult.IsOK());
    auto [startSetTimestamp, endSetTimestamp, setError] = setResult.Value();
    YT_VERIFY(setError.IsOK());

    int copiedNodeCount = 0, prerequisiteCheckFailedCount = 0;
    for (int requestIndex : std::views::iota(0, RequestCount)) {
        auto result = results[requestIndex];
        YT_VERIFY(result.IsOK());
        auto [startTimestamp, endTimestamp, error] = result.Value();
        YT_LOG_DEBUG(
            "Copy request (RequestIndex: %v, StartTimestamp: %v, EndTimestamp: %v)",
            requestIndex,
            startTimestamp,
            endTimestamp);

        if (result.IsOK()) {
            YT_VERIFY(startTimestamp < endSetTimestamp);
            ++copiedNodeCount;
        } else if (result.GetCode() == NObjectClient::EErrorCode::PrerequisiteCheckFailed) {
            YT_VERIFY(startSetTimestamp < endTimestamp);
            ++prerequisiteCheckFailedCount;
        } else {
            YT_LOG_FATAL(result, "Unexpected error");
        }
    }

    YT_LOG_DEBUG(
        "Copyings with prerequisite revision are finished "
        "(CopiedNodeCount: %v, PrerequisiteCheckFailedCount: %v)",
        copiedNodeCount,
        prerequisiteCheckFailedCount);
}

TEST_F(TSequoiaTest, TestManySetReqsWithPrerequisiteRevision)
{
    constexpr static int ThreadCount = 3;
    constexpr static int RequestCount = 10;

    WaitFor(Client_->CreateNode("//sequoia/tmp", EObjectType::MapNode))
        .ThrowOnError();
    WaitFor(Client_->SetNode("//sequoia/tmp/node", ConvertToYsonString("value")))
        .ThrowOnError();

    auto threadPool = CreateThreadPool(ThreadCount, "ManySetReqs");
    auto barrierPromise = NewPromise<void>();
    std::atomic<int> timestamp = 0;

    auto requestFutures = std::vector<TFuture<std::tuple<int, int, TError>>>(RequestCount);
    for (int requestIndex : std::views::iota(0, std::ssize(requestFutures))) {
        requestFutures[requestIndex] = BIND([
            &timestamp,
            requestIndex,
            barrierFuture = barrierPromise.ToFuture()
        ] () -> std::tuple<int, int, TError> {
            WaitForFast(barrierFuture)
                .ThrowOnError();

            auto revisionConfig = New<TPrerequisiteRevisionConfig>();

            auto startTimestamp = timestamp.fetch_add(1, std::memory_order::acquire);
            TGetNodeOptions getOptions;
            getOptions.Attributes = TAttributeFilter({"revision"});
            auto nodeResult = ConvertToNode(WaitFor(Client_->GetNode("//sequoia/tmp/node", getOptions)).ValueOrThrow());
            auto nodeValue = nodeResult->GetValue<std::string>();

            TSetNodeOptions setOptions;
            revisionConfig->Revision = nodeResult->Attributes().Get<NHydra::TRevision>("revision");
            revisionConfig->Path = "//sequoia/tmp/node";
            setOptions.PrerequisiteRevisions = {revisionConfig};

            auto newValue = Format("%v%v", nodeValue, requestIndex);
            auto error = WaitFor(Client_->SetNode("//sequoia/tmp/node", ConvertToYsonString(newValue), setOptions));
            auto endTimestamp = timestamp.fetch_add(1, std::memory_order::release);
            return {startTimestamp, endTimestamp, error};
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run();
    }

    barrierPromise.Set();

    auto results = WaitFor(AllSet(std::move(requestFutures)))
        .ValueOrThrow();

    int setNodeCount = 0, prerequisiteCheckFailedCount = 0;
    for (int requestIndex : std::views::iota(0, RequestCount)) {
        auto result = results[requestIndex];
        YT_VERIFY(result.IsOK());
        auto [startTimestamp, endTimestamp, error] = result.Value();
        YT_LOG_DEBUG(
            "Set request (RequestIndex: %v, StartTimestamp: %v, EndTimestamp: %v)",
            requestIndex,
            startTimestamp,
            endTimestamp);

        if (result.IsOK()) {
            ++setNodeCount;
        } else if (result.GetCode() == NObjectClient::EErrorCode::PrerequisiteCheckFailed) {
            auto intersectingRequestIndex = -1;
            for (int index : std::views::iota(0, RequestCount)) {
                auto maybeIntersectingResult = results[index];
                YT_VERIFY(maybeIntersectingResult.IsOK());
                auto [maybeIntersectingStartTimestamp, maybeIntersectingEndTimestamp, maybeIntersectingError] = maybeIntersectingResult.Value();
                if (maybeIntersectingError.IsOK() && maybeIntersectingStartTimestamp < endTimestamp) {
                    intersectingRequestIndex = index;
                }
            }
            YT_VERIFY(intersectingRequestIndex >= 0 && intersectingRequestIndex < RequestCount);
            ++prerequisiteCheckFailedCount;
        } else {
            YT_LOG_FATAL(result, "Unexpected error");
        }
    }

    YT_LOG_DEBUG(
        "Sets with prerequisite revision are finished "
        "(SetNodeCount: %v, PrerequisiteCheckFailedCount: %v)",
        setNodeCount,
        prerequisiteCheckFailedCount);
}

TEST_F(TSequoiaTest, TestTransactionAbortConflict)
{
    std::vector<NApi::ITransactionPtr> transactions;
    std::vector<TTransactionId> transactionIds;
    std::vector<NCypressClient::TNodeId> createdNodes;

    NApi::ITransactionPtr currentTransaction;
    std::string currentPath = "//sequoia";
    for (int i = 0; i < 20; ++i) {
        currentPath += Format("/level%v", i);
        NApi::TTransactionStartOptions startTransactionOptions;
        if (currentTransaction) {
            startTransactionOptions.ParentId = currentTransaction->GetId();
        }
        currentTransaction = StartCypressTransaction(startTransactionOptions);
        transactions.push_back(currentTransaction);
        transactionIds.push_back(currentTransaction->GetId());
        TCreateNodeOptions createNodeOptions;
        createNodeOptions.TransactionId = currentTransaction->GetId();
        createdNodes.push_back(
            WaitFor(Client_->CreateNode(currentPath.c_str(), EObjectType::MapNode, createNodeOptions))
                .ValueOrThrow());
    }

    auto threadPool = CreateThreadPool(5, "ConcurrentAbortTx");
    auto barrierPromise = NewPromise<void>();
    std::vector<TFuture<void>> resultFutures;
    for (auto tx : transactions) {
        resultFutures.push_back(
            BIND([barrierFuture = barrierPromise.ToFuture(), transaction = tx] () {
                WaitFor(barrierFuture)
                    .ThrowOnError();
                return transaction->Abort();
            })
                .AsyncVia(threadPool->GetInvoker())
                .Run());
    }

    barrierPromise.Set();

    WaitFor(AllSucceeded(resultFutures))
        .ThrowOnError();

    for (auto transactionId : transactionIds) {
        EXPECT_FALSE(WaitFor(Client_->NodeExists(FromObjectId(transactionId)))
            .ValueOrThrow());
    }

    WaitFor(Client_->GCCollect())
        .ThrowOnError();

    for (auto nodeId : createdNodes) {
        EXPECT_FALSE(WaitFor(Client_->NodeExists(FromObjectId(nodeId)))
            .ValueOrThrow());
    }
}

TEST_F(TSequoiaTest, TestTransactionAbortNoStarvation)
{
    std::vector<NApi::ITransactionPtr> transactions;
    std::vector<TTransactionId> transactionIds;

    const auto tmpNodePath = "//sequoia/tmp";
    WaitFor(Client_->CreateNode(tmpNodePath, EObjectType::MapNode))
        .ThrowOnError();

    auto topmostTransaction = StartCypressTransaction();
    transactions.push_back(topmostTransaction);
    for (int i = 0; i < 10; ++i) {
        auto parentTransaction = StartCypressTransaction({.ParentId = topmostTransaction->GetId()});
        transactions.push_back(parentTransaction);
        transactionIds.push_back(parentTransaction->GetId());

        for (int j = 0; j < 10; ++j) {
            auto childTransaction = StartCypressTransaction({.ParentId = parentTransaction->GetId()});
            transactions.push_back(childTransaction);
            transactionIds.push_back(childTransaction->GetId());
        }

        auto dependentTransaction = StartCypressTransaction({.PrerequisiteTransactionIds = {topmostTransaction->GetId()}});
        transactions.push_back(dependentTransaction);
        transactionIds.push_back(dependentTransaction->GetId());
        for (int j = 0; j < 10; ++j) {
            auto dependentChildTransaction = StartCypressTransaction({.ParentId = dependentTransaction->GetId()});
            transactions.push_back(dependentChildTransaction);
            transactionIds.push_back(dependentChildTransaction->GetId());
        }
    }

    std::vector<TFuture<void>> setNodeRequests;
    for (int i = 0; i < std::ssize(transactions); ++i) {
        auto setResult = Client_->SetNode(Format("%v/%v", tmpNodePath, i), ConvertToYsonString("some_value"));
        setNodeRequests.push_back(setResult);
    }

    WaitFor(AllSucceeded(std::move(setNodeRequests)))
        .ThrowOnError();

    auto threadPool = CreateThreadPool(25, "ConcurrentSet");
    auto barrierPromise = NewPromise<void>();
    std::vector<TFuture<void>> resultFutures;
    for (int i = 0; i < std::ssize(transactions); ++i) {
        auto transaction = transactions[i];
        resultFutures.push_back(
            BIND([barrierFuture = barrierPromise.ToFuture(), tmpNodePath, i, transaction] () {
                WaitFor(barrierFuture)
                    .ThrowOnError();

                while(true) {
                    TSetNodeOptions options;
                    options.TransactionId = transaction->GetId();
                    auto result = WaitFor(Client_->SetNode(
                        Format("%v/%v", tmpNodePath, i),
                        ConvertToYsonString("some_new_value"),
                        options));

                    if (!result.IsOK()) {
                        EXPECT_TRUE(result.FindMatching([] (const TError& error) { return error.GetMessage().contains("No such transaction"); }).has_value());
                        return;
                    }
                }
            })
                .AsyncVia(threadPool->GetInvoker())
                .Run());
    }

    YT_LOG_DEBUG("Started setting node values");
    barrierPromise.Set();

    YT_LOG_DEBUG("Aborting topmost transaction (TransactionId: %v)",
        topmostTransaction->GetId());
    WaitFor(topmostTransaction->Abort())
        .ThrowOnError();
    YT_LOG_DEBUG("Topmost transaction aborted");

    WaitFor(AllSet(resultFutures))
        .ThrowOnError();
}

TEST_F(TSequoiaTest, TestResponseKeeper)
{
    constexpr int ThreadCount = 4;
    constexpr int RequestCount = 20;

    WaitFor(Client_->SetNode("//sequoia/@some_user_attr", ConvertToYsonString(ConvertToNode(123))))
        .ThrowOnError();

    auto threadPool = CreateThreadPool(ThreadCount, "ConcurrentResponseKeeperRequests");
    auto barrierPromise = NewPromise<void>();

    auto mutationId = NRpc::TMutationId::Create();

    auto client = DynamicPointerCast<NApi::NNative::IClient>(Client_);
    auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);

    std::vector<TFuture<void>> responses;

    auto registerRequest = [&] (auto batchReq) {
        auto req = NCypressClient::TCypressYPathProxy::Remove("//sequoia/@some_user_attr");
        SetMutationId(req, mutationId, /*retry*/ true);

        batchReq->AddRequest(req);

        responses.push_back(BIND([batchReq = std::move(batchReq), barrier = barrierPromise.ToFuture()] {
            WaitFor(barrier)
                .ThrowOnError();
            auto batchRsp = WaitFor(batchReq->Invoke())
                .ValueOrThrow();
            auto rsp = batchRsp->template GetResponse<NCypressClient::TCypressYPathProxy::TRspRemove>(0);
            YT_LOG_DEBUG(TError(rsp), "Response finished");
            rsp.ThrowOnError();
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run());
    };

    for (int i : std::views::iota(0, RequestCount)) {
        if (i < RequestCount / 2) {
            // NB: sometimes we want to observe SequoiaRetriableError.
            registerRequest(proxy.ExecuteBatchNoBackoffRetries());
        } else {
            registerRequest(proxy.ExecuteBatch());
        }
    }

    barrierPromise.Set();

    WaitFor(AllSet(responses))
        .ThrowOnError();

    int sequoiaRetriableErrors = 0;
    int okRequests = 0;
    for (int i : std::views::iota(0, RequestCount)) {
        const auto& error = responses[i].GetOrCrash();
        if (error.IsOK()) {
            ++okRequests;
        } else if (error.GetNonTrivialCode() == NSequoiaClient::EErrorCode::SequoiaRetriableError && i < RequestCount / 2) {
            ++sequoiaRetriableErrors;
        } else if (error.GetNonTrivialCode() == NSequoiaClient::EErrorCode::SequoiaRetriableError) {
            YT_LOG_FATAL(error, "Unexpected retriable error when retries were enabled");
        } else {
            YT_LOG_FATAL(error, "Unexpected error");
        }
    }

    YT_LOG_DEBUG("Requests finished (OkRequests: %v, SequoiaRetriableErrors: %v)",
        okRequests,
        sequoiaRetriableErrors);

    // Every request with Sequoia retries should succeed thanks to PRK.
    EXPECT_GE(okRequests, RequestCount / 2);
}

TEST_F(TSequoiaTest, TestNodeReplacementAtomicity)
{
    constexpr auto IterationCount = 20;

    auto threadPool = CreateThreadPool(2, "ConcurrentNodeReplacement");
    auto invoker = threadPool->GetInvoker();

    for (int iteration : std::views::iota(0, IterationCount)) {
        YT_LOG_DEBUG("Starting iteration %v", iteration);

        WaitFor(Client_->CreateNode("//sequoia/a", EObjectType::MapNode))
            .ThrowOnError();

        YT_LOG_DEBUG("Node \"//sequoia/a\" created");

        auto barrier = NewPromise<void>();
        auto conncurrentReplaceFuture = BIND([client = Client_, barrier = barrier.ToFuture()] () {
            WaitFor(barrier)
                .ThrowOnError();

            YT_LOG_DEBUG("Concurrent worker started");

            WaitFor(Client_->CreateNode("//sequoia/b", EObjectType::MapNode))
                .ThrowOnError();

            YT_LOG_DEBUG("Node \"//sequoia/b\" created");

            TMoveNodeOptions moveOptions = {};
            moveOptions.Force = true;
            WaitFor(Client_->MoveNode("//sequoia/b", "//sequoia/a", moveOptions))
                .ThrowOnError();

            YT_LOG_DEBUG("\"//sequoia/b\" replaced with \"//sequoia/a\"");
        })
            .AsyncVia(invoker)
            .Run();

        auto now = TInstant::Now();
        YT_LOG_DEBUG("(Old creation time: %v)", now);

        TDelayedExecutor::Submit(BIND([&] { barrier.Set(); }), TDuration::MicroSeconds(50));

        constexpr auto WaitIterationLimit = 1000;

        for (int _ : std::views::iota(0, WaitIterationLimit)) {
            auto creationTimeString = WaitFor(Client_->GetNode("//sequoia/a/@creation_time"))
                .ValueOrThrow();

            auto creationTime = ConvertTo<TInstant>(creationTimeString);
            YT_LOG_DEBUG("Creation time fetched (CurrentCreationTime: %v)", creationTime);

            if (creationTime >= now) {
                break;
            }

            Sleep(TDuration::MicroSeconds(100));
        }

        WaitFor(conncurrentReplaceFuture)
            .ThrowOnError();

        WaitFor(Client_->RemoveNode("//sequoia/*"))
            .ThrowOnError();
    }
}

TEST_F(TSequoiaTest, TestLatency)
{
    WaitFor(Client_->CreateNode("//sequoia/latency", EObjectType::MapNode)).ThrowOnError();
    auto finally = Finally([] {
        Y_UNUSED(WaitFor(Client_->RemoveNode("//sequoia/latency", {.Recursive = true, .Force = true})));
    });

    constexpr auto AttemptCount = 40;
    auto threadPool = CreateThreadPool(8, "Worker");

    std::vector<TFuture<TDuration>> futures;
    futures.reserve(AttemptCount);
    for (auto attempt : std::views::iota(0, AttemptCount)) {
        futures.emplace_back(BIND([client = Client_, attempt] {
            NProfiling::TWallTimer timer;

            ITransactionPtr tx;
           {
               NTracing::TTraceContextGuard guard(NTracing::TTraceContext::NewRoot(""));
               tx = WaitFor(client->StartTransaction(ETransactionType::Master))
                    .ValueOrThrow();
           }

            auto path = Format("//sequoia/latency/%v", attempt);
            {
                NTracing::TTraceContextGuard guard(NTracing::TTraceContext::NewRoot(""));
                WaitFor(tx->CreateNode(path, EObjectType::MapNode))
                    .ThrowOnError();
            }

            {
                NTracing::TTraceContextGuard guard(NTracing::TTraceContext::NewRoot(""));
                WaitFor(tx->RemoveNode(path, {}))
                    .ThrowOnError();
            }

            {
                NTracing::TTraceContextGuard guard(NTracing::TTraceContext::NewRoot(""));
                WaitFor(tx->Commit())
                    .ThrowOnError();
            }

            return timer.GetElapsedTime();
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run());
    }

    auto total = TDuration::Zero();
    for (auto& f : futures) {
        total += WaitForFast(f)
            .ValueOrThrow();
    }

    YT_LOG_DEBUG("Mean time per test: %vms", total.MilliSeconds() / AttemptCount);
}

TEST_F(TSequoiaTest, TestSequoiaPrelockDisable)
{
    using namespace NCypressClient;

    auto parentNodeId = WaitFor(Client_->CreateNode("//sequoia/prelock_disable", EObjectType::MapNode))
        .ValueOrThrow();
    TNodeId targetNodeId = {};
    do {
        WaitFor(Client_->RemoveNode("//sequoia/prelock_disable/*")).ThrowOnError();
        targetNodeId = WaitFor(Client_->CreateNode("//sequoia/prelock_disable/m", EObjectType::MapNode))
            .ValueOrThrow();
    } while (CellTagFromId(parentNodeId) == CellTagFromId(targetNodeId));

    auto startTx = [&] (TStringBuf title, TTransactionId parentId = {}) {
        TTransactionStartOptions options;
        options.ReplicateToMasterCellTags = {CellTagFromId(parentNodeId), CellTagFromId(targetNodeId)};
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("title", title);
        options.ParentId = parentId;
        return WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();
    };

    auto tx1 = startTx("Parent");
    auto tx2 = startTx("Nested", tx1->GetId());

    auto setArtificialCommitDelay = [&] (TDuration delay) {
        TSetNodeOptions options;
        options.SuppressStronglyOrderedTransactionBarrier = true;
        options.SuppressTransactionCoordinatorSync = true;
        return Client_->SetNode(
            "//sys/@config/transaction_manager/testing/artificial_participant_commit_delay",
            ConvertToYsonString(delay),
            options)
            .AsVoid();
    };

    auto restore = Finally([&] {
        YT_UNUSED_FUTURE(setArtificialCommitDelay(TDuration::Zero()));

        YT_UNUSED_FUTURE(Client_->SetNode("//sys/@config/sequoia_manager/enable_prelock_tracker", ConvertToYsonString(true)));
    });

    auto tryLock = [&] () ->TError {
        auto nativeConnection = DynamicPointerCast<NNative::IConnection>(Connection_);
        auto channel = nativeConnection
            ->GetMasterCellDirectory()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CellTagFromId(targetNodeId));

        auto proxy = TObjectServiceProxy::FromDirectMasterChannel(channel);
        auto req = TCypressYPathProxy::Lock(FromObjectId(targetNodeId));
        SetTransactionId(req, tx1->GetId());
        req->set_mode(ToProto(ELockMode::Exclusive));

        auto timestamp = WaitFor(nativeConnection->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();
        req->set_timestamp(timestamp);
        SetAllowResolveFromSequoiaObject(&req->Header(), true);

        auto batchReq = proxy.ExecuteBatch();
        batchReq->AddRequest(req);
        batchReq->SetSuppressStronglyOrderedTransactionBarrier(true);
        batchReq->SetSuppressTransactionCoordinatorSync(true);
        batchReq->SetSuppressUpstreamSync(true);

        return WaitFor(batchReq->Invoke())
            .ValueOrThrow()
            ->GetResponse<TCypressYPathProxy::TRspLock>(0);
    };

    // NB: TDelayedExecutor::WaitForDuration can be used in fibers only.
    auto threadPool = CreateThreadPool(1, "TestSequoiaPrelockDisable");
    WaitFor(BIND([&] {
        WaitFor(setArtificialCommitDelay(TDuration::MilliSeconds(2000))).ThrowOnError();
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(250));

        TRemoveNodeOptions options;
        options.TransactionId = tx2->GetId();
        TFuture<void> removalFuture;
        {
            NTracing::TTraceContextGuard guard(NTracing::GetOrCreateTraceContext("Remove"));
            removalFuture = Client_->RemoveNode(FromObjectId(targetNodeId), options);
        }
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(250));

        {
            NTracing::TTraceContextGuard guard(NTracing::GetOrCreateTraceContext("Lock1"));
            auto lockError1 = tryLock();
            // NB: actually, it's OK to acquire lock under parent transaction.
            // But current implementation is a bit suboptimal here and has
            // false-positive conflict detection.
            EXPECT_EQ(lockError1.GetCode(), NSequoiaClient::EErrorCode::SequoiaRetriableError);
        }

        {
            TSetNodeOptions options;
            options.SuppressStronglyOrderedTransactionBarrier = true;
            options.SuppressTransactionCoordinatorSync = true;
            WaitFor(
                Client_->SetNode(
                    "//sys/@config/sequoia_manager/enable_prelock_tracker",
                    ConvertToYsonString(false),
                    options))
                .ThrowOnError();
        }

        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(250));

        {
            NTracing::TTraceContextGuard guard(NTracing::GetOrCreateTraceContext("Lock2"));
            tryLock().ThrowOnError();
        }

        WaitFor(removalFuture).ThrowOnError();
    })
        .AsyncVia(threadPool->GetInvoker())
        .Run())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestSequoiaPrelocksTransaction,
    (Self)
    (Parent)
    (Ancestor)
    (Nested)
    (Concurrent)
);

struct TSequoiaTestPrelocksParams
{
    ETestSequoiaPrelocksTransaction Transaction;
    std::optional<std::string> ExpectedErrorMessage;
    NCypressClient::ELockMode Mode;
    std::optional<std::string> Attribute;
    std::optional<std::string> Child;
};

class TSequoiaTestPrelocks
    : public TSequoiaTestBase
    , public testing::WithParamInterface<TSequoiaTestPrelocksParams>
{ };

std::vector<TSequoiaTestPrelocksParams> GenerateTestSequoiaPrelocksParams()
{
    using namespace NCypressClient;

    std::vector<TSequoiaTestPrelocksParams> cases;

    const std::tuple<ELockMode, std::optional<std::string>, std::optional<std::string>> LockKinds[] = {
        {ELockMode::Exclusive, std::nullopt, std::nullopt},
        {ELockMode::Shared, std::nullopt, std::nullopt},
        {ELockMode::Shared, "some_atr", std::nullopt},
        {ELockMode::Shared, std::nullopt, "some_child"},
    };

    for (auto tx : TEnumTraits<ETestSequoiaPrelocksTransaction>::GetDomainValues()) {
        for (auto [mode, attribute, child] : LockKinds) {
            std::optional<std::string> expectedErrorMessage;
            if (tx != ETestSequoiaPrelocksTransaction::Self) {
                if (attribute) {
                    expectedErrorMessage = "since this attribute is being locked by transaction";
                } else if (child) {
                    expectedErrorMessage = "since this child is being locked by transaction";
                } else {
                    expectedErrorMessage = "\"exclusive\" lock is being taken";
                }
            }

            cases.push_back({
                .Transaction = tx,
                .ExpectedErrorMessage = expectedErrorMessage,
                .Mode = mode,
                .Attribute = attribute,
                .Child = child,
            });
        }
    }

    // Snapshot locks are validated differently.
    cases.push_back({ETestSequoiaPrelocksTransaction::Self, "or its descendant", ELockMode::Snapshot});
    cases.push_back({ETestSequoiaPrelocksTransaction::Parent, "or its descendant", ELockMode::Snapshot});
    cases.push_back({ETestSequoiaPrelocksTransaction::Ancestor, "or its descendant", ELockMode::Snapshot});
    cases.push_back({ETestSequoiaPrelocksTransaction::Nested, std::nullopt, ELockMode::Snapshot});
    cases.push_back({ETestSequoiaPrelocksTransaction::Concurrent, std::nullopt, ELockMode::Snapshot});

    return cases;
}

TEST_P(TSequoiaTestPrelocks, Test)
{
    using namespace NCypressClient;

    auto parentNodeId = WaitFor(Client_->CreateNode("//sequoia/race_2pc_execute", EObjectType::MapNode))
        .ValueOrThrow();

    TNodeId targetNodeId = {};
    do {
        WaitFor(Client_->RemoveNode("//sequoia/race_2pc_execute/*")).ThrowOnError();
        targetNodeId = WaitFor(Client_->CreateNode("//sequoia/race_2pc_execute/m", EObjectType::MapNode)).ValueOrThrow();
    } while (CellTagFromId(targetNodeId) == CellTagFromId(parentNodeId));

    YT_LOG_DEBUG("Test nodes created (ParentId: %v, NodeId: %v)", parentNodeId, targetNodeId);

    TEnumIndexedArray<ETestSequoiaPrelocksTransaction, ITransactionPtr> tx;
    auto startTx = [&] (ETestSequoiaPrelocksTransaction thisTx, const ITransactionPtr& parent) {
        TTransactionStartOptions options;
        options.ReplicateToMasterCellTags = {CellTagFromId(parentNodeId), CellTagFromId(targetNodeId)};
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("title", ToString(thisTx));
        if (parent) {
            options.ParentId = parent->GetId();
        }
        tx[thisTx] = WaitFor(Client_->StartTransaction(ETransactionType::Master, options)).ValueOrThrow();
    };

    startTx(ETestSequoiaPrelocksTransaction::Concurrent, nullptr);
    startTx(ETestSequoiaPrelocksTransaction::Ancestor, nullptr);
    startTx(ETestSequoiaPrelocksTransaction::Parent, tx[ETestSequoiaPrelocksTransaction::Ancestor]);
    startTx(ETestSequoiaPrelocksTransaction::Self, tx[ETestSequoiaPrelocksTransaction::Parent]);
    startTx(ETestSequoiaPrelocksTransaction::Nested, tx[ETestSequoiaPrelocksTransaction::Self]);

    auto resetArtificialCommitDelay = [&] {
        TSetNodeOptions options;
        options.SuppressTransactionCoordinatorSync = true;
        options.SuppressStronglyOrderedTransactionBarrier = true;
        YT_UNUSED_FUTURE(Client_->SetNode(
            "//sys/@config/transaction_manager/testing/artificial_participant_commit_delay",
            ConvertToYsonString(0),
            options));
    };

    auto finally = Finally(resetArtificialCommitDelay);

    auto removalStarted = NewPromise<void>();

    auto threadPool = CreateThreadPool(2, "TestPrelocks");

    auto removalFinished = BIND([&] {
        NTracing::TTraceContextGuard guard(NTracing::GetOrCreateTraceContext("LockNode"));

        YT_LOG_DEBUG("Planning removal");

        WaitFor(Client_->SetNode(
            "//sys/@config/transaction_manager/testing/artificial_participant_commit_delay",
            ConvertToYsonString(1000)))
            .ThrowOnError();

        // Wait for config to be applied to all cells.
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(250));

        TRemoveNodeOptions options;
        options.TransactionId = tx[ETestSequoiaPrelocksTransaction::Self]->GetId();
        auto future = Client_->RemoveNode(FromObjectId(targetNodeId), options);
        removalStarted.Set();
        return future;
    })
        .AsyncVia(threadPool->GetInvoker())
        .Run();

    auto lockAttemptFinished = BIND([&] {
        NTracing::TTraceContextGuard guard(NTracing::GetOrCreateTraceContext("LockNode"));

        YT_LOG_DEBUG("Planning locking");

        auto nativeConnection = DynamicPointerCast<NNative::IConnection>(Connection_);
        auto channel = nativeConnection
            ->GetMasterCellDirectory()
            ->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CellTagFromId(targetNodeId));

        auto proxy = TObjectServiceProxy::FromDirectMasterChannel(channel);
        auto req = TCypressYPathProxy::Lock(FromObjectId(targetNodeId));
        SetTransactionId(req, tx[GetParam().Transaction]->GetId());
        req->set_mode(ToProto(GetParam().Mode));

        // Waitable flag doesn't matter here since any conflict with prelock has to
        // be retried.
        req->set_waitable(true);

        if (GetParam().Child) {
            req->set_child_key(*GetParam().Child);
        }
        if (GetParam().Attribute) {
            req->set_attribute_key(*GetParam().Attribute);
        }

        auto timestamp = WaitFor(nativeConnection->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();
        req->set_timestamp(timestamp);
        SetAllowResolveFromSequoiaObject(&req->Header(), true);

        auto batchReq = proxy.ExecuteBatch();
        batchReq->AddRequest(req);
        batchReq->SetSuppressStronglyOrderedTransactionBarrier(true);
        batchReq->SetSuppressTransactionCoordinatorSync(true);
        batchReq->SetSuppressUpstreamSync(true);

        WaitFor(removalStarted.ToFuture())
            .ThrowOnError();

        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

        auto batchRspOrError = WaitFor(batchReq->Invoke());

        resetArtificialCommitDelay();

        auto batchRsp = batchRspOrError.ValueOrThrow();
        batchRsp->GetResponse<TCypressYPathProxy::TRspLock>(0)
            .ThrowOnError();
    })
        .AsyncVia(threadPool->GetInvoker())
        .Run();

    WaitFor(removalFinished).ThrowOnError();


    auto error = WaitFor(lockAttemptFinished);
    if (!GetParam().ExpectedErrorMessage) {
        error.ThrowOnError();
    } else {
        EXPECT_EQ(error.GetCode(), NSequoiaClient::EErrorCode::SequoiaRetriableError);
        EXPECT_TRUE(error.GetMessage().contains(*GetParam().ExpectedErrorMessage));
    }
}

INSTANTIATE_TEST_SUITE_P(
    ,
    TSequoiaTestPrelocks,
    ::testing::ValuesIn(GenerateTestSequoiaPrelocksParams()),
    [] (const ::testing::TestParamInfo<TSequoiaTestPrelocks::ParamType>& info) {
        TStringBuilder builder;
        builder.AppendFormat("%v_%v", info.param.Transaction, info.param.Mode);
        if (info.param.Mode == NCypressClient::ELockMode::Shared) {
            if (info.param.Child) {
                builder.AppendString("_child");
            }
            if (info.param.Attribute) {
                builder.AppendString("_attribute");
            }
        }
        return std::string(builder.Flush());
    });

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
