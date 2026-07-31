#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/transaction_client.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace {

using namespace NApi;
using namespace NCppTests;
using namespace NConcurrency;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

// Networking is disabled irreversibly and process-wide in the middle of the test, so
// this test lives in its own single-test binary and skips all network-dependent teardown.
class TTransactionExpirationModeTest
    : public TApiTestBase
{
protected:
    void TearDown() override
    { }

    static void TearDownTestCase()
    {
        Client_.Reset();
        Connection_.Reset();
    }
};

TEST_F(TTransactionExpirationModeTest, PessimisticMasterTransactionSelfFences)
{
    TTransactionStartOptions options{
        .Timeout = TDuration::Seconds(3),
        .PingPeriod = TDuration::MilliSeconds(200),
        .MasterExpirationMode = EMasterTransactionExpirationMode::Pessimistic,
    };
    auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
        .ValueOrThrow();

    std::atomic<bool> aborted = false;
    transaction->SubscribeAborted(BIND([&] (const TError& /*error*/) {
        aborted.store(true);
    }));

    // Sever this process from the master; the transaction can no longer be pinged, while
    // its server-side counterpart lives on until it too expires.
    NBus::NTcp::TDispatcher::Get()->DisableNetworking();

    // A pessimistic transaction fences itself once its ping lease elapses, even though it
    // cannot reach the master to learn its fate.
    // NB: fencing is currently gated behind the periodic ping's retry-exhaustion — the next lease
    // check is scheduled only after the ping future resolves — so it can take well beyond the 3 s
    // timeout, hence the generous 20 s poll. A follow-up PR will decouple the lease check from ping
    // completion so a pessimistic transaction fences within about one ping period past its timeout.
    WaitForPredicate(
        [&] { return aborted.load(); },
        /*iterationCount*/ 100,
        /*period*/ TDuration::MilliSeconds(200));
    EXPECT_TRUE(aborted.load());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
