#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/guid.h>

#include <yt/server/chunk_server/chunk_requisition.h>

namespace NYT {
namespace {

using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

static TGuid account1ID;
static TGuid account2ID;
static TGuid account3ID;
static TGuid account4ID;

void InitAccountIDs()
{
    if (account1ID) {
        return;
    }

    std::vector<TGuid> guids;
    guids.reserve(4);
    guids.push_back(TGuid::Create());
    guids.push_back(TGuid::Create());
    guids.push_back(TGuid::Create());
    guids.push_back(TGuid::Create());
    std::sort(guids.begin(), guids.end());

    account1ID = guids[0];
    account2ID = guids[1];
    account3ID = guids[2];
    account4ID = guids[3];
}

TEST(TChunkRequisitionTest, Combine)
{
    InitAccountIDs();

    TChunkRequisition requisition1;
    ASSERT_FALSE(requisition1.GetVital());
    ASSERT_EQ(requisition1.EntryCount(), 0);
    TChunkRequisition requisition2(account1ID, 0, TReplicationPolicy(3, false), true);
    requisition2.SetVital(true);
    requisition1 |= requisition2;
    ASSERT_TRUE(requisition1.GetVital());
    ASSERT_EQ(requisition1.EntryCount(), 1);
    ASSERT_EQ(*requisition1.begin(), TRequisitionEntry(account1ID, 0, TReplicationPolicy(3, false), true));

    requisition1 |= TChunkRequisition(account2ID, 1, TReplicationPolicy(2, true), false);
    // These two entries should merge into one.
    requisition1 |= TChunkRequisition(account1ID, 2, TReplicationPolicy(3, true), true);
    requisition1 |= TChunkRequisition(account1ID, 2, TReplicationPolicy(3, false), true);
    ASSERT_EQ(requisition1.EntryCount(), 3);

    requisition2 |= TChunkRequisition(account3ID, 5, TReplicationPolicy(4, false), false);
    requisition2 |= TChunkRequisition(account3ID, 5, TReplicationPolicy(4, false), true);
    requisition2 |= TChunkRequisition(account3ID, 4, TReplicationPolicy(2, false), false);
    requisition2 |= TChunkRequisition(account4ID, 3, TReplicationPolicy(1, true), true);
    ASSERT_EQ(requisition2.EntryCount(), 5);

    requisition1 |= requisition2;
    ASSERT_TRUE(requisition1.GetVital());
    ASSERT_EQ(requisition1.EntryCount(), 7);

    auto it = requisition1.begin();
    ASSERT_EQ(*it++, TRequisitionEntry(account1ID, 0, TReplicationPolicy(3, false), true));
    ASSERT_EQ(*it++, TRequisitionEntry(account1ID, 2, TReplicationPolicy(3, false), true));
    ASSERT_EQ(*it++, TRequisitionEntry(account2ID, 1, TReplicationPolicy(2, true), false));
    ASSERT_EQ(*it++, TRequisitionEntry(account3ID, 4, TReplicationPolicy(2, false), false));
    ASSERT_EQ(*it++, TRequisitionEntry(account3ID, 5, TReplicationPolicy(4, false), true));
    ASSERT_EQ(*it++, TRequisitionEntry(account3ID, 5, TReplicationPolicy(4, false), false));
    ASSERT_EQ(*it++, TRequisitionEntry(account4ID, 3, TReplicationPolicy(1, true), true));
    ASSERT_EQ(it, requisition1.end());

    requisition2 |= requisition1;
    ASSERT_EQ(requisition1, requisition2);
}

TEST(TChunkRequisitionTest, SelfCombine)
{
    InitAccountIDs();

    TChunkRequisition requisition(account1ID, 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(account3ID, 5, TReplicationPolicy(4, false), false);
    auto requisitionCopy = requisition;
    requisition |= requisition;
    ASSERT_EQ(requisition, requisitionCopy);
}

TEST(TChunkRequisitionTest, CombineWithEmpty)
{
    InitAccountIDs();

    TChunkRequisition requisition(account1ID, 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(account3ID, 5, TReplicationPolicy(4, false), false);
    auto requisitionCopy = requisition;

    TChunkRequisition emptyRequisition;
    ASSERT_EQ(emptyRequisition.EntryCount(), 0);

    requisition |= emptyRequisition;
    ASSERT_EQ(requisition, requisitionCopy);
}

TEST(TChunkRequisitionTest, CombineWithReplication)
{
    InitAccountIDs();

    TChunkRequisition requisition(account4ID, 0, TReplicationPolicy(3, false), true);
    requisition |= TChunkRequisition(account1ID, 5, TReplicationPolicy(4, false), false);
    ASSERT_FALSE(requisition.GetVital());

    TChunkReplication replication;
    replication[4].SetReplicationFactor(8);
    replication[6].SetReplicationFactor(7);
    replication[6].SetDataPartsOnly(true);

    requisition.CombineWith(replication, account2ID, true);

    ASSERT_FALSE(requisition.GetVital());
    ASSERT_EQ(requisition.EntryCount(), 4);
    auto it = requisition.begin();
    ASSERT_EQ(*it++, TRequisitionEntry(account1ID, 5, TReplicationPolicy(4, false), false));
    ASSERT_EQ(*it++, TRequisitionEntry(account2ID, 4, TReplicationPolicy(8, false), true));
    ASSERT_EQ(*it++, TRequisitionEntry(account2ID, 6, TReplicationPolicy(7, true), true));
    ASSERT_EQ(*it++, TRequisitionEntry(account4ID, 0, TReplicationPolicy(3, false), true));
    ASSERT_EQ(it, requisition.end());
}

TEST(TChunkRequisitionTest, RequisitionReplicationEquivalency)
{
    InitAccountIDs();

    TChunkRequisition requisition1(account4ID, 0, TReplicationPolicy(3, false), true);
    requisition1 |= TChunkRequisition(account1ID, 5, TReplicationPolicy(4, false), true);
    requisition1.SetVital(true);

    TChunkRequisition requisition2(account2ID, 1, TReplicationPolicy(5, true), true);
    requisition2 |= TChunkRequisition(account3ID, 0, TReplicationPolicy(1, false), true);

    auto replication1 = requisition1.ToReplication();
    auto replication2 = requisition2.ToReplication();

    ASSERT_EQ((requisition1 |= requisition2).ToReplication(), replication1 |= replication2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
