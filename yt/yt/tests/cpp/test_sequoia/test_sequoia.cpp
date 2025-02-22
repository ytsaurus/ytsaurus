#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

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

    static void AbortCypressTransactions()
    {
        YT_LOG_DEBUG("Aborting Cypress transactions");

        auto transactions = ConvertToNode(WaitFor(Client_->ListNode("//sys/transactions", NApi::TListNodeOptions{
            .Attributes = {"cypress_transaction"},
        }))
            .ValueOrThrow())
            ->AsList();

        std::vector<TFuture<void>> abortFutures;
        for (const auto& txNode : transactions->GetChildren()) {
            auto tx = txNode->AsString();
            if (tx->Attributes().Get("cypress_transaction", false)) {
                abortFutures.push_back(Client_
                    ->AttachTransaction(ConvertTo<TTransactionId>(tx->GetValue()))
                    ->Abort(NApi::TTransactionAbortOptions{.Force = true}));
            }
        }

        WaitFor(AllSet(abortFutures).AsVoid())
            .ThrowOnError();

        for (int transactionIndex : std::views::iota(0, std::ssize(abortFutures))) {
            auto value = abortFutures[transactionIndex].TryGet();
            YT_VERIFY(value.has_value());

            auto transactionId = transactions->FindChild(transactionIndex)->GetValue<std::string>();

            if (value->IsOK()) {
                YT_LOG_DEBUG(
                    "Cypress transaction aborted (TransactionId: %v)",
                    transactionId);
            } else {
                YT_LOG_DEBUG(
                    "Failed to abort Cypress transaction (TransactionId: %v, Error: %v)",
                    transactionId,
                    value->GetMessage());
            }
        }

        YT_LOG_DEBUG("All Cypress transactions aborted (TransactionCount: %v)", abortFutures.size());
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
    constexpr static int ThreadCount = 3;
    constexpr static int RequestCount = 15;

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
        } else if (error.FindMatching(NTabletClient::EErrorCode::TransactionLockConflict)) {
            ++lockConflictCount;
        } else if (error.FindMatching([] (const TError& error) {
            return error.GetMessage().contains("already exists");
        })) {
            ++nodeExistsCount;
        } else {
            // NB: In case of "socket closed" error all suite hangs. It's better
            // just crash it on unexcepted error.
            YT_LOG_FATAL(error, "Unexpected error");
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
    constexpr auto ChildCount = 3;
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
                inFlightRequests);

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
            YT_LOG_FATAL(result, "Unexpected error");
        }
    }

    YT_LOG_DEBUG("All transactions finished (Committed: %v, AlreadyAborted: %v)", committed, alreadyAborted);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
