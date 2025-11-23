#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/guid.h>

#include <yt/yt/server/master/chunk_server/chunk_requisition.h>

namespace NYT::NChunkServer {
namespace {

using namespace NObjectClient;

using NSecurityServer::TAccount;

////////////////////////////////////////////////////////////////////////////////

class TChunkRequisitionTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        std::vector<TGuid> guids;
        guids.reserve(6);
        for (auto i = 0; i < 6; ++i) {
            guids.push_back(TGuid::Create());
        }
        std::sort(guids.begin(), guids.end());

        Account1Id_ = guids[0];
        Account2Id_ = guids[1];
        Account3Id_ = guids[2];
        Account4Id_ = guids[3];
        DeadAccountId_ = guids[4];
        InactiveAccountId_ = guids[5];

        auto createAccount = [] (TGuid accountId, bool doRef = true) -> std::unique_ptr<TAccount> {
            auto result = TPoolAllocator::New<TAccount>(accountId);
            if (doRef) {
                result->RefObject();
            }
            return result;
        };

        Account1_ = createAccount(Account1Id_);
        Account2_ = createAccount(Account2Id_);
        Account3_ = createAccount(Account3Id_);
        Account4_ = createAccount(Account4Id_);
        DeadAccount_ = createAccount(DeadAccountId_, /* doRef */ false);
        InactiveAccount_ = createAccount(InactiveAccountId_);
        InactiveAccount_->SetLifeStage(EObjectLifeStage::RemovalStarted);
    }

    void TearDown() override
    {
        Account1_.reset();
        Account2_.reset();
        Account3_.reset();
        Account4_.reset();
    }

protected:
    TGuid Account1Id_;
    std::unique_ptr<TAccount> Account1_;
    TGuid Account2Id_;
    std::unique_ptr<TAccount> Account2_;
    TGuid Account3Id_;
    std::unique_ptr<TAccount> Account3_;
    TGuid Account4Id_;
    std::unique_ptr<TAccount> Account4_;
    TGuid DeadAccountId_;
    std::unique_ptr<TAccount> DeadAccount_;
    TGuid InactiveAccountId_;
    std::unique_ptr<TAccount> InactiveAccount_;
};

TEST_F(TChunkRequisitionTest, Aggregate)
{
    TChunkRequisition requisition1;
    ASSERT_FALSE(requisition1.GetVital());
    ASSERT_EQ(requisition1.GetAllEntryCount(), 0);
    TChunkRequisition requisition2(Account1_.get(), 0, TReplicationPolicy(3, false), true);
    requisition2.SetVital(true);
    requisition1 |= requisition2;
    ASSERT_TRUE(requisition1.GetVital());
    ASSERT_EQ(requisition1.GetAllEntryCount(), 1);
    ASSERT_EQ(*requisition1.AllEntries().begin(), TRequisitionEntry(Account1_.get(), 0, TReplicationPolicy(3, false), true));

    requisition1 |= TChunkRequisition(Account2_.get(), 1, TReplicationPolicy(2, true), false);
    // These two entries should merge into one.
    requisition1 |= TChunkRequisition(Account1_.get(), 2, TReplicationPolicy(3, true), true);
    requisition1 |= TChunkRequisition(Account1_.get(), 2, TReplicationPolicy(3, false), true);
    ASSERT_EQ(requisition1.GetAllEntryCount(), 3);

    requisition2 |= TChunkRequisition(Account3_.get(), 5, TReplicationPolicy(4, false), false);
    requisition2 |= TChunkRequisition(Account3_.get(), 5, TReplicationPolicy(4, false), true);
    requisition2 |= TChunkRequisition(Account3_.get(), 4, TReplicationPolicy(2, false), false);
    requisition2 |= TChunkRequisition(Account4_.get(), 3, TReplicationPolicy(1, true), true);
    requisition2 |= TChunkRequisition(DeadAccount_.get(), 2, TReplicationPolicy(1, true), true);
    requisition2 |= TChunkRequisition(InactiveAccount_.get(), 6, TReplicationPolicy(1, true), true);
    ASSERT_EQ(requisition2.GetAllEntryCount(), 7);

    requisition1 |= requisition2;
    ASSERT_TRUE(requisition1.GetVital());
    ASSERT_EQ(requisition1.GetAllEntryCount(), 9);

    auto it = requisition1.AllEntries().begin();
    ASSERT_EQ(*it, TRequisitionEntry(Account1_.get(), 0, TReplicationPolicy(3, false), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(Account1_.get(), 2, TReplicationPolicy(3, false), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(Account2_.get(), 1, TReplicationPolicy(2, true), false));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(Account3_.get(), 4, TReplicationPolicy(2, false), false));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(Account3_.get(), 5, TReplicationPolicy(4, false), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(Account3_.get(), 5, TReplicationPolicy(4, false), false));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(Account4_.get(), 3, TReplicationPolicy(1, true), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(DeadAccount_.get(), 2, TReplicationPolicy(1, true), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(InactiveAccount_.get(), 6, TReplicationPolicy(1, true), true));
    ++it;
    ASSERT_EQ(it, requisition1.AllEntries().end());

    requisition2 |= requisition1;
    ASSERT_EQ(requisition1, requisition2);
}

TEST_F(TChunkRequisitionTest, SelfAggregate)
{
    TChunkRequisition requisition(Account1_.get(), 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(Account3_.get(), 5, TReplicationPolicy(4, false), false);
    auto requisitionCopy = requisition;
    requisition |= static_cast<TChunkRequisition&>(requisition);
    ASSERT_EQ(requisition, requisitionCopy);
}

TEST_F(TChunkRequisitionTest, SelfAggregate_DeadAccount)
{
    TChunkRequisition requisition(Account1_.get(), 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(DeadAccount_.get(), 5, TReplicationPolicy(4, false), false);
    auto requisitionCopy = requisition;
    requisition |= static_cast<TChunkRequisition&>(requisition);
    ASSERT_EQ(requisition, requisitionCopy);
}

TEST_F(TChunkRequisitionTest, SelfAggregate_InactiveAccount)
{
    TChunkRequisition requisition(Account1_.get(), 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(InactiveAccount_.get(), 5, TReplicationPolicy(4, false), false);
    auto requisitionCopy = requisition;
    requisition |= static_cast<TChunkRequisition&>(requisition);
    ASSERT_EQ(requisition, requisitionCopy);
}

TEST_F(TChunkRequisitionTest, AggregateWithEmpty)
{
    TChunkRequisition requisition(Account1_.get(), 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(Account3_.get(), 5, TReplicationPolicy(4, false), false);
    auto requisitionCopy = requisition;

    TChunkRequisition emptyRequisition;
    ASSERT_EQ(emptyRequisition.GetAllEntryCount(), 0);

    requisition |= emptyRequisition;
    ASSERT_EQ(requisition, requisitionCopy);
}

TEST_F(TChunkRequisitionTest, AggregateWithReplication)
{
    TChunkRequisition requisition(Account4_.get(), 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(Account1_.get(), 5, TReplicationPolicy(4, false), false);
    ASSERT_FALSE(requisition.GetVital());

    TChunkReplication replication;
    replication.Set(4, TReplicationPolicy(8, false));
    replication.Set(6, TReplicationPolicy(7, true));

    requisition.AggregateWith(replication, Account2_.get(), true);

    ASSERT_FALSE(requisition.GetVital());
    ASSERT_EQ(requisition.GetAllEntryCount(), 4);
    auto it = requisition.AllEntries().begin();
    ASSERT_EQ(*it, TRequisitionEntry(Account1_.get(), 5, TReplicationPolicy(4, false), false));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(Account2_.get(), 4, TReplicationPolicy(8, false), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(Account2_.get(), 6, TReplicationPolicy(7, true), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(Account4_.get(), 0, TReplicationPolicy(3, false), true));
    ++it;
    ASSERT_EQ(it, requisition.AllEntries().end());
}

TEST_F(TChunkRequisitionTest, RequisitionReplicationEquivalency)
{
    TChunkRequisition requisition1(Account4_.get(), 0, TReplicationPolicy(3, false), true);
    requisition1 |= TChunkRequisition(Account1_.get(), 5, TReplicationPolicy(4, false), true);
    requisition1.SetVital(true);

    TChunkRequisition requisition2(Account2_.get(), 1, TReplicationPolicy(5, true), true);
    requisition2 |= TChunkRequisition(Account3_.get(), 0, TReplicationPolicy(1, false), true);

    auto replication1 = requisition1.ToReplication();
    auto replication2 = requisition2.ToReplication();

    auto aggregatedReplication = replication1;
    for (const auto& entry : replication2) {
        aggregatedReplication.Aggregate(entry.GetMediumIndex(), entry.Policy());
    }

    ASSERT_EQ((requisition1 |= requisition2).ToReplication(), aggregatedReplication);
}

TEST_F(TChunkRequisitionTest, ActiveEntryFiltering)
{
    TChunkRequisition requisition(DeadAccount_.get(), 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(Account1_.get(), 1, TReplicationPolicy(4, false), false);
    requisition |= TChunkRequisition(DeadAccount_.get(), 2, TReplicationPolicy(5, false), false);
    requisition |= TChunkRequisition(Account2_.get(), 3, TReplicationPolicy(6, false), false);
    requisition |= TChunkRequisition(Account3_.get(), 4, TReplicationPolicy(7, false), false);
    requisition |= TChunkRequisition(InactiveAccount_.get(), 5, TReplicationPolicy(8, false), false);

    TChunkRequisition expectedEffectiveRequisition;
    expectedEffectiveRequisition |= TChunkRequisition(Account1_.get(), 1, TReplicationPolicy(4, false), false);
    expectedEffectiveRequisition |= TChunkRequisition(Account2_.get(), 3, TReplicationPolicy(6, false), false);
    expectedEffectiveRequisition |= TChunkRequisition(Account3_.get(), 4, TReplicationPolicy(7, false), false);

    ASSERT_EQ(requisition.CountActiveEntries(), 3);
    ASSERT_EQ(requisition.CountActiveEntries(), expectedEffectiveRequisition.CountActiveEntries());

    ASSERT_TRUE(std::ranges::equal(requisition.ActiveEntries(), expectedEffectiveRequisition.AllEntries()));

    requisition |= TChunkRequisition(Account4_.get(), 6, TReplicationPolicy(9, false), false);
    expectedEffectiveRequisition |= TChunkRequisition(Account4_.get(), 6, TReplicationPolicy(9, false), false);

    ASSERT_TRUE(std::ranges::equal(requisition.ActiveEntries(), expectedEffectiveRequisition.AllEntries()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
