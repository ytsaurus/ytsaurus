#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/sequoia_client/public.h>

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

class TSequoiaTest
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

TString Prettify(const TYsonString& yson)
{
    return ConvertToYsonString(yson, EYsonFormat::Pretty).ToString();
}

TString Prettify(TStringBuf yson)
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

    YSON_EXPECT_EQ("{b = {}}", rsp);
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

TEST_F(TSequoiaTest, CypressTransactionSimple)
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
        "%true");

    YSON_EXPECT_EQ(WaitFor(Client_->GetNode(Format("#%v/@cypress_transaction", nestedTx->GetId())))
        .ValueOrThrow(),
        "%true");

    YSON_EXPECT_EQ(WaitFor(Client_->GetNode(Format("#%v/@cypress_transaction", dependentTx->GetId())))
        .ValueOrThrow(),
        "%true");

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

TEST_F(TSequoiaTest, ConcurrentCommitTx)
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

TEST_F(TSequoiaTest, TestParallelActionsWithPrerequisiteTx)
{
    constexpr static int ThreadCount = 3;
    constexpr static int RequestCount = 20;

    WaitFor(Client_->CreateNode("//sequoia/tmp", EObjectType::MapNode))
        .ThrowOnError();
    WaitFor(Client_->SetNode(
            "//sys/cypress_proxies/@config/object_service/enable_fast_path_prerequisite_transaction_check",
            ConvertToYsonString(false)))
        .ThrowOnError();

    auto threadPool = CreateThreadPool(ThreadCount, "ConcurrentActionsWithPrerequisiteTx");
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

    int createdNodeCount = 0, prerequisiteCheckFailedCount = 0;
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
        } else {
            YT_LOG_FATAL(error, "Unexpected error");
        }
    }

    YT_LOG_DEBUG(
        "Creations with prerequisite transaction are finished "
        "(CreatedNodeCount: %v, PrerequisiteCheckFailedCount: %v, PrerequisiteTransactionId: %v)",
        createdNodeCount,
        prerequisiteCheckFailedCount,
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

TEST_F(TSequoiaTest, TestResponseKeeper)
{
    constexpr int ThreadCount = 4;
    constexpr int RequestCount = 20;

    WaitFor(Client_->SetNode("//sequoia/@unexisting_attr", ConvertToYsonString(ConvertToNode(123))))
        .ThrowOnError();

    auto threadPool = CreateThreadPool(ThreadCount, "ConcurrentResponseKeeperRequests");
    auto barrierPromise = NewPromise<void>();

    auto mutationId = NRpc::TMutationId::Create();

    auto client = DynamicPointerCast<NApi::NNative::IClient>(Client_);
    auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);

    std::vector<TFuture<void>> responses;

    auto registerRequest = [&] (auto batchReq) {
        auto req = NCypressClient::TCypressYPathProxy::Remove("//sequoia/@unexisting_attr");
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

    int okWithRetries = 0;
    int okWithoutRetries = 0;
    int sequoiaRetriableErrors = 0;
    for (int i : std::views::iota(0, RequestCount)) {
        const auto& error = responses[i].Get();
        if (error.IsOK()) {
            ++(i < RequestCount / 2 ? okWithoutRetries : okWithRetries);
        } else if (error.GetCode() == NSequoiaClient::EErrorCode::SequoiaRetriableError && i < RequestCount / 2) {
            ++sequoiaRetriableErrors;
        } else if (error.GetCode() == NSequoiaClient::EErrorCode::SequoiaRetriableError) {
            YT_LOG_FATAL(error, "Unexpected retriable error when retries were enabled");
        } else {
            YT_LOG_FATAL(error, "Unexpected error");
        }
    }

    YT_LOG_DEBUG("Requests finished (OkWithRetries: %v, OkWithoutRetries: %v, SequoiaRetriableErrors: %v)",
        okWithRetries,
        okWithoutRetries,
        sequoiaRetriableErrors);

    EXPECT_LE(okWithoutRetries, 1);
    EXPECT_GE(okWithRetries, RequestCount / 2);
}

TEST_F(TSequoiaTest, TestNodeReplacementAtomicity)
{
    constexpr auto IterationCount = 20;

    auto threadPool = CreateThreadPool(2, "ConcurrentNodereplacement");
    auto invoker = threadPool->GetInvoker();

    for (int iteration : std::views::iota(0, IterationCount)) {
        YT_LOG_DEBUG("Starting iteration %v", iteration);

        WaitFor(Client_->CreateNode("//sequoia/a", EObjectType::MapNode))
            .ThrowOnError();

        YT_LOG_DEBUG("Node \"//sequoia/a\" craeted");

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
