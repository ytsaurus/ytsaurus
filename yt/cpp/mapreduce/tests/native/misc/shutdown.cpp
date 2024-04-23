#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/client/client.h>
#include <yt/cpp/mapreduce/client/yt_poller.h>

#include <library/cpp/testing/gtest/gtest.h>

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

TEST(Shutdown, DiscardItem)
{
    auto testClient = CreateTestClient();
    auto client = TClientPtr(static_cast<TClient*>(testClient.Get()));

    std::atomic<int> discardCounter = 0;

    client->GetYtPoller().Watch(new TPollableItem(discardCounter));

    client->Shutdown();
    EXPECT_TRUE(discardCounter == 1);

    client->Shutdown();
    EXPECT_TRUE(discardCounter == 1);
}

TEST(Shutdown, CallAfterShutdown)
{
    auto client = CreateTestClient();
    EXPECT_NO_THROW(client->WhoAmI());

    client->Shutdown();
    EXPECT_THROW(client->WhoAmI(), TApiUsageError);
}

TEST(Shutdown, FutureCancellation)
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
            EXPECT_TRUE(future.HasException());
            future.TryRethrow();
            FAIL() << "Unreachable";
        });

        client2->Shutdown();
    }

    EXPECT_TRUE(applied.HasException());
    tx1->Abort();

    try {
        applied.TryRethrow();
    } catch (const std::exception& exc) {
        // Operation expected to be cancelled
        EXPECT_TRUE(TString(exc.what()).Contains("cancelled"));
    }
}

////////////////////////////////////////////////////////////////////////////////
