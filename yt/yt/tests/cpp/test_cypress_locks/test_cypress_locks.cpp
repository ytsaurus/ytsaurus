#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <util/random/random.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const TYPath NodePath = "//node_to_lock";

////////////////////////////////////////////////////////////////////////////////

const auto Logger = CppTestsLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressLocksTestBase
    : public TApiTestBase
{
public:
    void SetUp() override
    {
        TApiTestBase::SetUp();

        WaitFor(Client_->CreateNode(NodePath, EObjectType::MapNode))
            .ThrowOnError();
    }

    void TearDown() override
    {
        auto baseTearDown = Finally([this] {
            TApiTestBase::TearDown();
        });

        AbortCypressTransactions();

        WaitFor(Client_->RemoveNode(NodePath, NApi::TRemoveNodeOptions{
            .Force = true,
        }))
            .ThrowOnError();
    }

    static NApi::ITransactionPtr StartCypressTransaction(const NApi::TTransactionStartOptions& options = {})
    {
        auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();

        YT_LOG_DEBUG("Cypress transaction started (TransactionId: %v)", tx->GetId());

        return tx;
    }

    TFuture<void> ExecuteNRequests(
        IInvokerPtr invoker,
        auto requestCreationCallback,
        int n)
    {
        return BIND([=] {
            auto client = DynamicPointerCast<NApi::NNative::IClient>(Client_);
            auto proxy = CreateObjectServiceWriteProxy(client);

            auto batchReq = proxy.ExecuteBatch();
            for (auto _ : std::views::iota(0, n)) {
                auto newRequest = requestCreationCallback();
                batchReq->AddRequest(std::move(newRequest));
            }

            WaitFor(batchReq->Invoke())
                .ThrowOnError();
        })
            .AsyncVia(invoker)
            .Run();
    }
};

std::string MakeRandomString(size_t stringSize)
{
    std::string randomString;
    randomString.reserve(stringSize);
    for (size_t i = 0; i < stringSize; i++) {
        randomString += ('a' + rand() % 25);
    }
    return randomString;
}

////////////////////////////////////////////////////////////////////////////////

struct TRandomizeChildKeys{};

class TCypressLocksTest
    : public TCypressLocksTestBase
    , public ::testing::WithParamInterface<std::tuple<
        /*lockMode*/ ELockMode,
        /*childKey*/ std::variant<TRandomizeChildKeys, std::string>,
        /*workingTxCount*/ int
    >>
{};


TEST_P(TCypressLocksTest, TestLockConflictResolution)
{
    constexpr static int ThreadCount = 8;
    constexpr static int LocksPerThreadCount = 10000;

    // Setup a blocking exclusive lock, so waitable locks have to wait.
    auto blockingTx = StartCypressTransaction();
    WaitFor(blockingTx->LockNode(NodePath, ELockMode::Exclusive))
        .ThrowOnError();

    auto [waitbleLockMode, childKey, workingTxCount] = GetParam();
    std::vector<ITransactionPtr> workingTxs;
    for (auto _ : std::views::iota(0, workingTxCount)) {
        workingTxs.push_back(StartCypressTransaction());
    }

    YT_LOG_DEBUG("Transactions started (BlockingTxId: %v, WorkingTxIds: [%v])",
        blockingTx->GetId(),
        MakeFormattableView(
            workingTxs,
            [] (auto* builder, const auto& transaction) {
                builder->AppendFormat("%v", transaction->GetId());
    }));

    // Setup a lot of waitable locks.
    auto createRequest = [workingTxs = workingTxs, waitbleLockMode = waitbleLockMode, childKey = childKey] {
        auto req = TCypressYPathProxy::Lock(NodePath);
        auto workingTxIndex = RandomNumber<ui32>(workingTxs.size());
        SetTransactionId(req, workingTxs[workingTxIndex]->GetId());

        req->set_mode(ToProto(waitbleLockMode));
        req->set_waitable(true);

        Visit(childKey,
            [&] (TRandomizeChildKeys) {
                req->set_child_key(MakeRandomString(10));
            },
            [&] (const std::string& key) {
                if (!key.empty()) {
                    req->set_child_key(key);
                }
            });

        return req;
    };

    auto requestsReadyFutures = std::vector<TFuture<void>>(ThreadCount);
    auto threadPool = CreateThreadPool(ThreadCount, "LockersThreadPool");
    for (auto threadIndex : std::views::iota(0, ThreadCount)) {
        requestsReadyFutures[threadIndex] = ExecuteNRequests(
            threadPool->GetInvoker(),
            createRequest,
            LocksPerThreadCount);
    }

    WaitFor(AllSucceeded(requestsReadyFutures))
        .ThrowOnError();

    // Sanity check.
    auto lockCount = ConvertTo<int>(WaitFor(Client_->GetNode(NodePath + "/@lock_count"))
        .ValueOrThrow());
    ASSERT_EQ(lockCount, LocksPerThreadCount * ThreadCount + 1);

    auto start = GetInstant();

    // Remove the obstucting lock, all waitable locks will be taken within one mutation.
    WaitFor(blockingTx->Abort())
        .ThrowOnError();

    // Ensure that locks were taken and that mutation has been applied. Mostly a sanity check.
    Y_UNUSED(WaitFor(Client_->SetNode(NodePath + "/@some_attr", ConvertToYsonString("some_value"))));

    auto transactionAbortDuration = GetInstant() - start;
    YT_LOG_DEBUG("Recorded transaction abort duration (AbortDuration: %vms)",
        transactionAbortDuration);
    // 5 seconds is _very_ conservative.
    ASSERT_LT(transactionAbortDuration, TDuration::Seconds(5));

    lockCount = ConvertTo<int>(WaitFor(Client_->GetNode(NodePath + "/@lock_count"))
        .ValueOrThrow());
    ASSERT_EQ(lockCount, LocksPerThreadCount * ThreadCount);
}

using TestParamsType = std::tuple<ELockMode, std::variant<TRandomizeChildKeys, std::string>, int>;

INSTANTIATE_TEST_SUITE_P(
    TCypressLocksTest,
    TCypressLocksTest,
    ::testing::Values(
        TestParamsType(ELockMode::Exclusive, "", 1),
        TestParamsType(ELockMode::Shared, "", 1),
        TestParamsType(ELockMode::Shared, "some_key", 1),
        TestParamsType(ELockMode::Shared, TRandomizeChildKeys{}, 1),
        TestParamsType(ELockMode::Shared, "some_key", 10)
    ));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
