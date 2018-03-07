#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/guid.h>

#include <yt/server/chunk_server/chunk_requisition.h>

namespace NYT {
namespace NChunkServer {
namespace {

using NSecurityServer::TAccount;

////////////////////////////////////////////////////////////////////////////////

TGuid account1Id;
std::unique_ptr<TAccount> account1;
TGuid account2Id;
std::unique_ptr<TAccount> account2;
TGuid account3Id;
std::unique_ptr<TAccount> account3;
TGuid account4Id;
std::unique_ptr<TAccount> account4;

void InitAccountIds()
{
    if (account1Id) {
        return;
    }

    std::vector<TGuid> guids;
    guids.reserve(4);
    guids.push_back(TGuid::Create());
    guids.push_back(TGuid::Create());
    guids.push_back(TGuid::Create());
    guids.push_back(TGuid::Create());
    std::sort(guids.begin(), guids.end());

    account1Id = guids[0];
    account2Id = guids[1];
    account3Id = guids[2];
    account4Id = guids[3];

    account1 = std::make_unique<TAccount>(account1Id);
    account2 = std::make_unique<TAccount>(account2Id);
    account3 = std::make_unique<TAccount>(account3Id);
    account4 = std::make_unique<TAccount>(account4Id);
}

TEST(TChunkRequisitionTest, Combine)
{
    InitAccountIds();

    TChunkRequisition requisition1;
    ASSERT_FALSE(requisition1.GetVital());
    ASSERT_EQ(requisition1.GetEntryCount(), 0);
    TChunkRequisition requisition2(account1.get(), 0, TReplicationPolicy(3, false), true);
    requisition2.SetVital(true);
    requisition1 |= requisition2;
    ASSERT_TRUE(requisition1.GetVital());
    ASSERT_EQ(requisition1.GetEntryCount(), 1);
    ASSERT_EQ(*requisition1.begin(), TRequisitionEntry(account1.get(), 0, TReplicationPolicy(3, false), true));

    requisition1 |= TChunkRequisition(account2.get(), 1, TReplicationPolicy(2, true), false);
    // These two entries should merge into one.
    requisition1 |= TChunkRequisition(account1.get(), 2, TReplicationPolicy(3, true), true);
    requisition1 |= TChunkRequisition(account1.get(), 2, TReplicationPolicy(3, false), true);
    ASSERT_EQ(requisition1.GetEntryCount(), 3);

    requisition2 |= TChunkRequisition(account3.get(), 5, TReplicationPolicy(4, false), false);
    requisition2 |= TChunkRequisition(account3.get(), 5, TReplicationPolicy(4, false), true);
    requisition2 |= TChunkRequisition(account3.get(), 4, TReplicationPolicy(2, false), false);
    requisition2 |= TChunkRequisition(account4.get(), 3, TReplicationPolicy(1, true), true);
    ASSERT_EQ(requisition2.GetEntryCount(), 5);

    requisition1 |= requisition2;
    ASSERT_TRUE(requisition1.GetVital());
    ASSERT_EQ(requisition1.GetEntryCount(), 7);

    auto it = requisition1.begin();
    ASSERT_EQ(*it, TRequisitionEntry(account1.get(), 0, TReplicationPolicy(3, false), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(account1.get(), 2, TReplicationPolicy(3, false), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(account2.get(), 1, TReplicationPolicy(2, true), false));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(account3.get(), 4, TReplicationPolicy(2, false), false));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(account3.get(), 5, TReplicationPolicy(4, false), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(account3.get(), 5, TReplicationPolicy(4, false), false));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(account4.get(), 3, TReplicationPolicy(1, true), true));
    ++it;
    ASSERT_EQ(it, requisition1.end());

    requisition2 |= requisition1;
    ASSERT_EQ(requisition1, requisition2);
}

TEST(TChunkRequisitionTest, SelfCombine)
{
    InitAccountIds();

    TChunkRequisition requisition(account1.get(), 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(account3.get(), 5, TReplicationPolicy(4, false), false);
    auto requisitionCopy = requisition;
    requisition |= requisition;
    ASSERT_EQ(requisition, requisitionCopy);
}

TEST(TChunkRequisitionTest, CombineWithEmpty)
{
    InitAccountIds();

    TChunkRequisition requisition(account1.get(), 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(account3.get(), 5, TReplicationPolicy(4, false), false);
    auto requisitionCopy = requisition;

    TChunkRequisition emptyRequisition;
    ASSERT_EQ(emptyRequisition.GetEntryCount(), 0);

    requisition |= emptyRequisition;
    ASSERT_EQ(requisition, requisitionCopy);
}

TEST(TChunkRequisitionTest, CombineWithReplication)
{
    InitAccountIds();

    TChunkRequisition requisition(account4.get(), 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(account1.get(), 5, TReplicationPolicy(4, false), false);
    ASSERT_FALSE(requisition.GetVital());

    TChunkReplication replication;
    replication[4].SetReplicationFactor(8);
    replication[6].SetReplicationFactor(7);
    replication[6].SetDataPartsOnly(true);

    requisition.CombineWith(replication, account2.get(), true);

    ASSERT_FALSE(requisition.GetVital());
    ASSERT_EQ(requisition.GetEntryCount(), 4);
    auto it = requisition.begin();
    ASSERT_EQ(*it, TRequisitionEntry(account1.get(), 5, TReplicationPolicy(4, false), false));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(account2.get(), 4, TReplicationPolicy(8, false), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(account2.get(), 6, TReplicationPolicy(7, true), true));
    ++it;
    ASSERT_EQ(*it, TRequisitionEntry(account4.get(), 0, TReplicationPolicy(3, false), true));
    ++it;
    ASSERT_EQ(it, requisition.end());
}

TEST(TChunkRequisitionTest, RequisitionReplicationEquivalency)
{
    InitAccountIds();

    TChunkRequisition requisition1(account4.get(), 0, TReplicationPolicy(3, false), true);
    requisition1 |= TChunkRequisition(account1.get(), 5, TReplicationPolicy(4, false), true);
    requisition1.SetVital(true);

    TChunkRequisition requisition2(account2.get(), 1, TReplicationPolicy(5, true), true);
    requisition2 |= TChunkRequisition(account3.get(), 0, TReplicationPolicy(1, false), true);

    auto replication1 = requisition1.ToReplication();
    auto replication2 = requisition2.ToReplication();

    ASSERT_EQ((requisition1 |= requisition2).ToReplication(), replication1 |= replication2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkServer
} // namespace NYT
