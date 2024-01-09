#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

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
        Config_->TransactionTimeout = TDuration::Minutes(1);
        Config_->TransactionPingPeriod = TDuration::MilliSeconds(100);
        Config_->LockAcquisitionPeriod = TDuration::MilliSeconds(100);
        Config_->LeaderCacheUpdatePeriod = TDuration::MilliSeconds(100);
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
        TCypressElectionManagerConfigPtr config = nullptr,
        IAttributeDictionaryPtr transactionAttributes = nullptr)
    {
        auto options = New<TCypressElectionManagerOptions>();
        options->GroupName = name;
        if (!config) {
            config = Config_;
        }
        if (!actionQueue) {
            actionQueue = ActionQueue_;
        }
        if (transactionAttributes) {
            options->TransactionAttributes = transactionAttributes;
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

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetLockPath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iterationCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
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
    WaitForPredicate([&] {
        return IsActive(electionManager->GetPrerequisiteTransactionId());
    });
    auto transaction1 = electionManager->GetPrerequisiteTransactionId();
    EXPECT_TRUE(IsActive(transaction1));

    EXPECT_EQ(StartCount_, 1);
    EXPECT_EQ(EndCount_, 0);

    YT_UNUSED_FUTURE(electionManager->StopLeading());
    WaitForPredicate([&] {
        return !IsActive(transaction1);
    });
    EXPECT_FALSE(IsActive(transaction1));
    WaitForPredicate([&] {
        return IsActive(electionManager->GetPrerequisiteTransactionId());
    });
    auto transaction2 = electionManager->GetPrerequisiteTransactionId();
    EXPECT_TRUE(IsActive(transaction2));

    EXPECT_EQ(StartCount_, 2);
    EXPECT_EQ(EndCount_, 1);

    WaitFor(electionManager->Stop())
        .ThrowOnError();
    EXPECT_FALSE(IsActive(transaction1));
    WaitForPredicate([&] {
        return !IsActive(transaction2);
    });

    EXPECT_EQ(StartCount_, 2);
    EXPECT_EQ(EndCount_, 2);
}

TEST_F(TCypressElectionManagerTest, TestSeveralElectionManagers)
{
    std::vector<ICypressElectionManagerPtr> electionManagers;
    std::vector<TActionQueuePtr> queues;
    for (int i = 0; i < 3; ++i) {
        queues.push_back(New<TActionQueue>());
        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("test", i);
        electionManagers.push_back(CreateElectionManager(
            Format("electionManager%v", i),
            queues.back(),
            /*config*/ nullptr,
            transactionAttributes));
        electionManagers.back()->Start();
    }

    auto countLeaders = [&] {
        int count = 0;
        for (int i = 0; i < 3; ++i) {
            count += IsActive(electionManagers[i]->GetPrerequisiteTransactionId());
        }
        return count;
    };

    auto findLeaderIndex = [&] {
        for (int i = 0; i < 3; ++i) {
            if (IsActive(electionManagers[i]->GetPrerequisiteTransactionId())) {
                return i;
            }
        }
        YT_ASSERT(false);
        return -1;
    };

    auto findLeader = [&] {
        return electionManagers[findLeaderIndex()];
    };

    auto getSyncedTransactionAttributeCaches = [&] () -> IAttributeDictionaryPtr {
        std::vector<IAttributeDictionaryPtr> attributes;
        for (const auto& electionManager : electionManagers) {
            if (electionManager->IsActive()) {
                attributes.push_back(electionManager->GetCachedLeaderTransactionAttributes());
            }
        }
        YT_ASSERT(!attributes.empty());
        if (std::find(attributes.begin(), attributes.end(), nullptr) != attributes.end()) {
            return nullptr;
        }
        for (int i = 1; i < std::ssize(attributes); ++i) {
            if (*attributes[i] != *attributes[0]) {
                return nullptr;
            }
        }
        return attributes[0];
    };

    auto checkTransactionAttributeCachesEqualToLeader = [&] {
        auto leaderIndex = findLeaderIndex();
        auto syncedCachedAttributes = getSyncedTransactionAttributeCaches();
        return syncedCachedAttributes && syncedCachedAttributes->Get<int>("test") == leaderIndex;
    };

    Sleep(TDuration::MilliSeconds(200));
    WaitForPredicate([&] {
        return countLeaders() == 1;
    });
    WaitForPredicate(checkTransactionAttributeCachesEqualToLeader);

    EXPECT_EQ(StartCount_, 1);
    EXPECT_EQ(EndCount_, 0);

    EXPECT_EQ(countLeaders(), 1);
    auto leader = findLeader();
    auto transaction1 = leader->GetPrerequisiteTransactionId();

    for (int i = 0; i < 3; ++i) {
        if (!IsActive(electionManagers[i]->GetPrerequisiteTransactionId())) {
            YT_UNUSED_FUTURE(electionManagers[i]->StopLeading());
        }
    }

    Sleep(TDuration::MilliSeconds(200));

    EXPECT_EQ(countLeaders(), 1);

    EXPECT_EQ(StartCount_, 1);
    EXPECT_EQ(EndCount_, 0);

    YT_UNUSED_FUTURE(leader->StopLeading());
    WaitForPredicate([&] {
        return !IsActive(transaction1);
    });
    Sleep(TDuration::MilliSeconds(200));
    WaitForPredicate([&] {
        return countLeaders() == 1;
    });
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));

    EXPECT_EQ(StartCount_, 2);
    EXPECT_EQ(EndCount_, 1);

    WaitForPredicate(checkTransactionAttributeCachesEqualToLeader);

    leader = findLeader();
    auto transaction2 = leader->GetPrerequisiteTransactionId();

    WaitFor(leader->Stop())
        .ThrowOnError();
    WaitForPredicate([&] {
        return !IsActive(transaction2);
    });
    Sleep(TDuration::MilliSeconds(200));
    WaitForPredicate([&] {
        return countLeaders() == 1;
    });
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));

    EXPECT_EQ(StartCount_, 3);
    EXPECT_EQ(EndCount_, 2);

    WaitForPredicate(checkTransactionAttributeCachesEqualToLeader);

    leader->Start();
    Sleep(TDuration::MilliSeconds(200));
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));

    EXPECT_EQ(StartCount_, 3);
    EXPECT_EQ(EndCount_, 2);

    WaitForPredicate(checkTransactionAttributeCachesEqualToLeader);

    leader = findLeader();
    auto transaction3 = leader->GetPrerequisiteTransactionId();

    ICypressElectionManagerPtr stoppedManager;

    for (int i = 0; i < 3; ++i) {
        if (!IsActive(electionManagers[i]->GetPrerequisiteTransactionId())) {
            stoppedManager = electionManagers[i];
            WaitFor(stoppedManager->Stop())
                .ThrowOnError();
            break;
        }
    }

    Sleep(TDuration::MilliSeconds(200));
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));
    EXPECT_TRUE(IsActive(transaction3));
    EXPECT_TRUE(checkTransactionAttributeCachesEqualToLeader());

    EXPECT_EQ(StartCount_, 3);
    EXPECT_EQ(EndCount_, 2);

    YT_UNUSED_FUTURE(leader->StopLeading());
    WaitForPredicate([&] {
        return !IsActive(transaction3);
    });
    Sleep(TDuration::MilliSeconds(200));
    WaitForPredicate([&] {
        return countLeaders() == 1;
    });
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));
    EXPECT_FALSE(IsActive(transaction3));
    EXPECT_EQ(stoppedManager->GetPrerequisiteTransactionId(), NullTransactionId);

    EXPECT_EQ(StartCount_, 4);
    EXPECT_EQ(EndCount_, 3);

    WaitForPredicate(checkTransactionAttributeCachesEqualToLeader);

    stoppedManager->Start();
    Sleep(TDuration::MilliSeconds(200));
    EXPECT_EQ(countLeaders(), 1);
    EXPECT_FALSE(IsActive(transaction1));
    EXPECT_FALSE(IsActive(transaction2));
    EXPECT_FALSE(IsActive(transaction3));

    EXPECT_EQ(StartCount_, 4);
    EXPECT_EQ(EndCount_, 3);

    WaitForPredicate(checkTransactionAttributeCachesEqualToLeader);

    for (int i = 0; i < 3; ++i) {
        WaitFor(electionManagers[i]->Stop())
            .ThrowOnError();
    }

    EXPECT_EQ(StartCount_, 4);
    EXPECT_EQ(EndCount_, 4);
}

TEST_F(TCypressElectionManagerTest, TestRollingAttributeRemoval)
{
    std::vector<ICypressElectionManagerPtr> electionManagers;
    std::vector<TActionQueuePtr> queues;
    for (int i = 0; i < 3; ++i) {
        queues.push_back(New<TActionQueue>());
        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("test", i);
        if (i == 0) {
            transactionAttributes->Set("extra_attribute", i);
        }
        electionManagers.push_back(CreateElectionManager(
            Format("electionManager%v", i),
            queues.back(),
            /*config*/ nullptr,
            transactionAttributes));
        electionManagers.back()->Start();
    }

    auto findLeaderIndex = [&] {
        for (int i = 0; i < 3; ++i) {
            if (IsActive(electionManagers[i]->GetPrerequisiteTransactionId())) {
                return i;
            }
        }
        return -1;
    };

    WaitFor(electionManagers[0]->Stop())
        .ThrowOnError();

    Sleep(TDuration::MilliSeconds(200));
    WaitForPredicate([&] {
        return findLeaderIndex() != -1;
    });
    // The leader is now definitely not 0.
    auto leaderIndex = findLeaderIndex();

    electionManagers[0]->Start();
    WaitForPredicate([&] {
        // We will be trying to fetch extra_attribute from the leader's transaction, it should not fail.
        auto attributes = electionManagers[0]->GetCachedLeaderTransactionAttributes();
        return attributes && attributes->Get<int>("test") == leaderIndex;
    });
}

TEST_F(TCypressElectionManagerTest, TestAbortTransaction)
{
    for (auto abort : {false, true}) {
        SetUpCounts();
        auto electionManager = CreateElectionManager("electionManager");
        electionManager->Start();
        WaitForPredicate([&] () {
            return IsActive(electionManager->GetPrerequisiteTransactionId());
        });
        auto transactionId = electionManager->GetPrerequisiteTransactionId();
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

        WaitForPredicate([&] {
            return IsActive(electionManager->GetPrerequisiteTransactionId());
        });

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 1);

        EXPECT_TRUE(IsActive(electionManager->GetPrerequisiteTransactionId()));

        WaitFor(electionManager->Stop())
            .ThrowOnError();

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 2);
    }
}

TEST_F(TCypressElectionManagerTest, TestAbortTransactionAndChangeLeader)
{
    for (auto abort : {false, true}) {
        SetUpCounts();
        auto actionQueue1 = New<TActionQueue>();
        auto config = CloneYsonStruct(Config_);
        config->TransactionPingPeriod = TDuration::Seconds(10);
        auto electionManager1 = CreateElectionManager("electionManager1", actionQueue1, config);
        electionManager1->Start();
        WaitForPredicate([&] {
            return IsActive(electionManager1->GetPrerequisiteTransactionId());
        });
        auto transactionId = electionManager1->GetPrerequisiteTransactionId();
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
        EXPECT_EQ(electionManager1->GetPrerequisiteTransactionId(), transactionId);

        WaitForPredicate([&] {
            return IsActive(electionManager2->GetPrerequisiteTransactionId());
        });
        EXPECT_TRUE(IsActive(electionManager2->GetPrerequisiteTransactionId()));
        EXPECT_EQ(electionManager1->GetPrerequisiteTransactionId(), transactionId);

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 0);

        WaitFor(electionManager1->StopLeading())
            .ThrowOnError();

        WaitForPredicate([&] {
            return electionManager1->GetPrerequisiteTransactionId() == NullTransactionId;
        });

        EXPECT_TRUE(IsActive(electionManager2->GetPrerequisiteTransactionId()));
        EXPECT_EQ(electionManager1->GetPrerequisiteTransactionId(), NullTransactionId);

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 1);

        WaitFor(electionManager2->Stop())
            .ThrowOnError();
        WaitFor(electionManager1->Stop())
            .ThrowOnError();

        EXPECT_EQ(StartCount_, 2);
        EXPECT_EQ(EndCount_, 2);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
