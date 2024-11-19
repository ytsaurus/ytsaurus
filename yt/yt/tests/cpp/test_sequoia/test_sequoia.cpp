#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NCppTests {
namespace {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger Logger("TestSequoia");

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

        Cerr << Format("Cypress transaction started (TransactionId: %v)", tx->GetId()) << Endl;

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
        Cerr << "Aborting Cypress transactions" << Endl;

        auto transactions = ConvertToNode(WaitFor(Client_->ListNode("//sys/transactions", NApi::TListNodeOptions{
            .Attributes = std::vector<TString>{"cypress_transaction"},
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

            value->ThrowOnError();

            Cerr
                << Format(
                    "Cypress transaction aborted (TransactionId: %v)",
                    transactions->FindChild(transactionIndex)->GetValue<std::string>())
                << Endl;
        }

        Cerr
            << Format("All Cypress transactions aborted (TransactionCount: %v)", abortFutures.size())
            << Endl;
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
            return error.GetMessage().Contains("already exists");
        })) {
            ++nodeExistsCount;
        } else {
            // NB: in case of "socket closed" error all suite hangs. It's better
            // just crash it on unexcepted error.
            YT_LOG_FATAL(error, "Unexpected error");
        }
    }

    Cerr << Format(
        "Concurrent node creations are finished (RequestCount: %v, CreatedNodeCount: %v, "
        "LockConflictCount: %v, AlreadyExistsErrorCount: %v)",
        RequestCount,
        createdNodeCount,
        lockConflictCount,
        nodeExistsCount)
        << Endl;

    // NB: we cannot require |createdNodeCount| to be 1 here since all Sequoia
    // tx can conflict with each other.

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

    Cerr << "All 3 transactions are aborted" << Endl;

    // All these transactions are expected to be aborted in TearDownTestCase().
    auto tx1 = StartCypressTransaction({.AutoAbort = false});
    auto tx2 = StartCypressTransaction({.ParentId = tx1->GetId(), .AutoAbort = false});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
