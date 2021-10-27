#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/tests/cpp/api_test_base.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>
#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace {

using namespace NCppTests;
using namespace NCypressElection;
using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCypressElectionManagerTest
    : public TApiTestBase
{
public:
    TCypressElectionManagerTest()
    {
        Config_->LockPath = GetLockPath();
        Config_->TransactionPingPeriod = TDuration::MilliSeconds(100);
        Config_->LockAcquisitionPeriod = TDuration::MilliSeconds(100);
    }

protected:
    TCypressElectionManagerConfigPtr Config_ = New<TCypressElectionManagerConfig>();

    TActionQueuePtr ActionQueue_ = New<TActionQueue>();

    std::atomic<int> StartCount_ = 0;
    std::atomic<int> EndCount_ = 0;

    void SetUpCounts()
    {
        StartCount_ = 0;
        EndCount_ = 0;
    }

    void OnLeadingStart()
    {
        ++StartCount_;
    }

    void OnLeadingEnd()
    {
        ++EndCount_;
    }

    ICypressElectionManagerPtr CreateElectionManager(
        const TString& name,
        TActionQueuePtr actionQueue = nullptr,
        TCypressElectionManagerConfigPtr config = nullptr)
    {
        auto options = New<TCypressElectionManagerOptions>();
        options->Name = name;
        if (!config) {
            config = Config_;
        }
        if (!actionQueue) {
            actionQueue = ActionQueue_;
        }
        auto electionManager = CreateCypressElectionManager(
            Client_,
            actionQueue->GetInvoker(),
            config,
            options);
        electionManager->SubscribeLeadingEnded(BIND([&] {
            ++EndCount_;
        }));
        electionManager->SubscribeLeadingStarted(BIND([&] {
            ++StartCount_;
        }));
        return electionManager;
    }

    void SetUp() override
    {
        WaitFor(Client_->CreateNode(GetLockPath(), EObjectType::MapNode))
            .ThrowOnError();
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetLockPath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iteartionCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
    }

    static TString GetLockPath()
    {
        return "//lock";
    }

    static bool IsActive(TTransactionId transactionId)
    {
        auto isActive = WaitFor(Client_->GetNode("#" + ToString(transactionId) + "/@state"));
        return isActive.IsOK() && ConvertTo<TString>(isActive.Value()) == "active";
    }
};

TEST_F(TCypressElectionManagerTest, TestElectionManager)
{
    auto electionManager = CreateElectionManager("electionManager");
    electionManager->Start();
    WaitForPredicate([&] () {
        return IsActive(electionManager->GetPrerequistiveTransactionId());
    });
    auto transaction1 = electionManager->GetPrerequistiveTransactionId();
    EXPECT_TRUE(IsActive(transaction1));

    EXPECT_EQ(StartCount_, 1);
    EXPECT_EQ(EndCount_, 0);

    electionManager->StopLeading();
    WaitForPredicate([&] () {
        return !IsActive(transaction1);
    });
    EXPECT_FALSE(IsActive(transaction1));
    WaitForPredicate([&] () {
        return IsActive(electionManager->GetPrerequistiveTransactionId());
    });
    auto transaction2 = electionManager->GetPrerequistiveTransactionId();
    EXPECT_TRUE(IsActive(transaction2));

    EXPECT_EQ(StartCount_, 2);
    EXPECT_EQ(EndCount_, 1);

    electionManager->Stop();
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));

    EXPECT_EQ(StartCount_, 2);
    EXPECT_EQ(EndCount_, 2);
}

TEST_F(TCypressElectionManagerTest, TestSeveralElectionManagers)
{
    std::vector<ICypressElectionManagerPtr> electionManagers;
    std::vector<TActionQueuePtr> queues;
    for (int i = 0; i < 3; ++i) {
        queues.push_back(New<TActionQueue>());
        electionManagers.push_back(CreateElectionManager(Format("electionManager%v", i), queues.back()));
        electionManagers.back()->Start();
    }

    auto countLeaders = [&] {
        int count = 0;
        for (int i = 0; i < 3; ++i) {
            count += IsActive(electionManagers[i]->GetPrerequistiveTransactionId());
        }
        return count;
    };

    auto findLeader = [&] () -> ICypressElectionManagerPtr {
        for (int i = 0; i < 3; ++i) {
            if (IsActive(electionManagers[i]->GetPrerequistiveTransactionId())) {
                return electionManagers[i];
            }
        }
        YT_ASSERT(false);
        return nullptr;
    };

    Sleep(TDuration::MilliSeconds(200));
    WaitForPredicate([&] () {
        return countLeaders() == 1;
    });

    EXPECT_EQ(StartCount_, 1);
    EXPECT_EQ(EndCount_, 0);

    EXPECT_EQ(countLeaders(), 1);
    auto leader = findLeader();
    auto transaction1 = leader->GetPrerequistiveTransactionId();

    for (int i = 0; i < 3; ++i) {
        if (!IsActive(electionManagers[i]->GetPrerequistiveTransactionId())) {
            electionManagers[i]->StopLeading();
        }
    }

    Sleep(TDuration::MilliSeconds(200));

    EXPECT_EQ(countLeaders(), 1);

    EXPECT_EQ(StartCount_, 1);
    EXPECT_EQ(EndCount_, 0);

    leader->StopLeading();
    WaitForPredicate([&] () {
        return !IsActive(transaction1);
    });
    Sleep(TDuration::MilliSeconds(200));
    WaitForPredicate([&] () {
        return countLeaders() == 1;
    });
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));

    EXPECT_EQ(StartCount_, 2);
    EXPECT_EQ(EndCount_, 1);

    leader = findLeader();
    auto transaction2 = leader->GetPrerequistiveTransactionId();

    leader->Stop();
    WaitForPredicate([&] () {
        return !IsActive(transaction2);
    });
    Sleep(TDuration::MilliSeconds(200));
    WaitForPredicate([&] () {
        return countLeaders() == 1;
    });
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));

    EXPECT_EQ(StartCount_, 3);
    EXPECT_EQ(EndCount_, 2);

    leader->Start();
    Sleep(TDuration::MilliSeconds(200));
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));

    EXPECT_EQ(StartCount_, 3);
    EXPECT_EQ(EndCount_, 2);

    leader = findLeader();
    auto transaction3 = leader->GetPrerequistiveTransactionId();

    ICypressElectionManagerPtr stopedManager;

    for (int i = 0; i < 3; ++i) {
        if (!IsActive(electionManagers[i]->GetPrerequistiveTransactionId())) {
            stopedManager = electionManagers[i];
            stopedManager->Stop();
            break;
        }
    }

    Sleep(TDuration::MilliSeconds(200));
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));
    EXPECT_TRUE(IsActive(transaction3));

    EXPECT_EQ(StartCount_, 3);
    EXPECT_EQ(EndCount_, 2);

    leader->StopLeading();
    WaitForPredicate([&] () {
        return !IsActive(transaction3);
    });
    Sleep(TDuration::MilliSeconds(200));
    WaitForPredicate([&] () {
        return countLeaders() == 1;
    });
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));
    EXPECT_FALSE(IsActive(transaction3));
    EXPECT_EQ(stopedManager->GetPrerequistiveTransactionId(), NullTransactionId);

    EXPECT_EQ(StartCount_, 4);
    EXPECT_EQ(EndCount_, 3);

    stopedManager->Start();
    Sleep(TDuration::MilliSeconds(200));
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));
    EXPECT_FALSE(IsActive(transaction3));

    EXPECT_EQ(StartCount_, 4);
    EXPECT_EQ(EndCount_, 3);

    for (int i = 0; i < 3; ++i) {
        electionManagers[i]->Stop();
    }

    EXPECT_EQ(StartCount_, 4);
    EXPECT_EQ(EndCount_, 4);
}

TEST_F(TCypressElectionManagerTest, TestAbortTransaction)
{
    for (auto abort : {false, true}) {
        SetUpCounts();
        auto electionManager = CreateElectionManager("electionManager");
        electionManager->Start();
        WaitForPredicate([&] () {
            return IsActive(electionManager->GetPrerequistiveTransactionId());
        });
        auto transactionId = electionManager->GetPrerequistiveTransactionId();
        EXPECT_TRUE(IsActive(transactionId));

        EXPECT_EQ(StartCount_, 1);
        EXPECT_EQ(EndCount_, 0);

        auto transaction = Client_->AttachTransaction(transactionId);
        if (abort) {
            WaitFor(transaction->Abort()).ThrowOnError();
        } else {
            WaitFor(transaction->Commit()).ThrowOnError();
        }
        EXPECT_FALSE(IsActive(transactionId));

        WaitForPredicate([&] () {
            return IsActive(electionManager->GetPrerequistiveTransactionId());
        });

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 1);

        EXPECT_TRUE(IsActive(electionManager->GetPrerequistiveTransactionId()));

        electionManager->Stop();

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 2);
    }
}

TEST_F(TCypressElectionManagerTest, TestAbortTransactionAndChangeLeader)
{
    for (auto abort : {false, true}) {
        SetUpCounts();
        auto actionQueue1 = New<TActionQueue>();
        auto config = CloneYsonSerializable(Config_);
        config->TransactionPingPeriod = TDuration::Seconds(10);
        auto electionManager1 = CreateElectionManager("electionManager1", actionQueue1, config);
        electionManager1->Start();
        WaitForPredicate([&] () {
            return IsActive(electionManager1->GetPrerequistiveTransactionId());
        });
        auto transactionId = electionManager1->GetPrerequistiveTransactionId();
        EXPECT_TRUE(IsActive(transactionId));

        EXPECT_EQ(StartCount_, 1);
        EXPECT_EQ(EndCount_, 0);

        auto electionManager2 = CreateElectionManager("electionManager2");
        electionManager2->Start();

        auto transaction = Client_->AttachTransaction(transactionId);
        if (abort) {
            WaitFor(transaction->Abort()).ThrowOnError();
        } else {
            WaitFor(transaction->Commit()).ThrowOnError();
        }
        EXPECT_FALSE(IsActive(transactionId));
        EXPECT_EQ(electionManager1->GetPrerequistiveTransactionId(), transactionId);

        WaitForPredicate([&] () {
            return IsActive(electionManager2->GetPrerequistiveTransactionId());
        });
        EXPECT_TRUE(IsActive(electionManager2->GetPrerequistiveTransactionId()));
        EXPECT_EQ(electionManager1->GetPrerequistiveTransactionId(), transactionId);

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 0);

        electionManager1->StopLeading();

        WaitForPredicate([&] () {
            return electionManager1->GetPrerequistiveTransactionId() == NullTransactionId;
        });

        EXPECT_TRUE(IsActive(electionManager2->GetPrerequistiveTransactionId()));
        EXPECT_EQ(electionManager1->GetPrerequistiveTransactionId(), NullTransactionId);

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 1);

        electionManager2->Stop();
        electionManager1->Stop();

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 2);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
