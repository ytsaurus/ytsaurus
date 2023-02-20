#include <mapreduce/yt/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/client/client.h>
#include <mapreduce/yt/client/yt_poller.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

class TPollableItem
    : public IYtPollerItem
{
public:
    TPollableItem(std::atomic<int>& counter)
        : Counter_(counter)
    { }

    void PrepareRequest(NRawClient::TRawBatchRequest*) override
    { }

    EStatus OnRequestExecuted() override
    {
        return PollContinue;
    }

    void OnItemDiscarded() override
    {
        Counter_.fetch_add(1);
    }
private:
    std::atomic<int>& Counter_;
};

Y_UNIT_TEST_SUITE(Shutdown)
{
    Y_UNIT_TEST(DiscardItem)
    {
        auto testClient = CreateTestClient();
        auto client = TClientPtr(static_cast<TClient*>(testClient.Get()));

        std::atomic<int> discardCounter = 0;

        client->GetYtPoller().Watch(new TPollableItem(discardCounter));

        client->Shutdown();
        UNIT_ASSERT(discardCounter == 1);

        client->Shutdown();
        UNIT_ASSERT(discardCounter == 1);
    }

    Y_UNIT_TEST(CallAfterShutdown)
    {
        auto client = CreateTestClient();
        UNIT_ASSERT_NO_EXCEPTION(client->WhoAmI());

        client->Shutdown();
        UNIT_ASSERT_EXCEPTION(client->WhoAmI(), TApiUsageError);
    }

    Y_UNIT_TEST(FutureCancellation)
    {
        TTestFixture fixture;
        auto workingDir = fixture.GetWorkingDir();
        TYPath path = workingDir + "/node";

        auto client1 = fixture.GetClient();
        client1->Set(path, 1);
        auto tx1 = client1->StartTransaction();
        tx1->Lock(path, ELockMode::LM_EXCLUSIVE);

        ::NThreading::TFuture<void> applied;
        {
            auto client2 = fixture.CreateClient();

            auto tx2 = client2->StartTransaction();
            auto lock2 = tx2->Lock(path, ELockMode::LM_EXCLUSIVE, TLockOptions().Waitable(true));

            auto future = lock2->GetAcquiredFuture();

            applied = future.Apply([client2=client2] (auto&& future) {
                UNIT_ASSERT(future.HasException());
                future.TryRethrow();
                UNIT_FAIL("Unreachable");
            });

            client2->Shutdown();
        }

        UNIT_ASSERT(applied.HasException());
        tx1->Abort();

        try {
            applied.TryRethrow();
        } catch (const std::exception& exc) {
            UNIT_ASSERT_STRING_CONTAINS_C(exc.what(), "cancelled", "Operation expected to be cancelled");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
