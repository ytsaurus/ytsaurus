#include <gtest/gtest.h>

#include <yt/yt/server/node/chaos_node/replication_card.h>
#include <yt/yt/server/node/chaos_node/replication_card_batcher.h>
#include <yt/yt/server/node/chaos_node/replication_card_collocation.h>

#include <yt/yt/client/chaos_client/helpers.h>

#include <yt/yt/client/tablet_client/config.h>

namespace NYT::NChaosNode {
namespace {

using namespace NChaosClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

class TReplcationCardBatcherTest
    : public ::testing::Test
{
public:
    static TReplicationCard* AddReplicationCard(
        EReplicationCardState state,
        TEntityMap<TReplicationCard>* replicationCardMap)
    {
        auto replicationCardId = MakeReplicationCardId(TObjectId::Create());
        auto replicationCardHolder = std::make_unique<TReplicationCard>(replicationCardId);
        replicationCardHolder->SetState(state);
        return replicationCardMap->Insert(replicationCardId, std::move(replicationCardHolder));
    }

    static void AddReplicationCards(
        int count,
        EReplicationCardState state,
        TEntityMap<TReplicationCard>* replicationCardMap)
    {
        for (int i = 0; i < count; ++i) {
            AddReplicationCard(state, replicationCardMap);
        }
    }

    static TReplicationCardCollocation* AddReplicationCardCollocation(
        int count,
        TEntityMap<TReplicationCardCollocation>* collocationMap,
        TEntityMap<TReplicationCard>* replicationCardMap)
    {
        auto collocationId = MakeReplicationCardCollocationId(TObjectId::Create());
        auto collocationHolder = std::make_unique<TReplicationCardCollocation>(collocationId);
        auto* collocation = collocationMap->Insert(collocationId, std::move(collocationHolder));
        collocation->SetSize(count);

        for (int i = 0; i < count; ++i) {
            auto* replicationCard = AddReplicationCard(EReplicationCardState::Normal, replicationCardMap);
            collocation->ReplicationCards().insert(replicationCard);
            replicationCard->SetCollocation(collocation);
        }

        return collocation;
    }
};

TEST_F(TReplcationCardBatcherTest, NoCollocations)
{
    TEntityMap<TReplicationCard> replicationCardMap;
    AddReplicationCards(10, EReplicationCardState::Normal, &replicationCardMap);
    AddReplicationCards(10, EReplicationCardState::RevokingShortcutsForMigration, &replicationCardMap);

    auto batch = BuildReadyToMigrateReplicationCardBatch(replicationCardMap, 5);
    EXPECT_EQ(batch.size(), 5ull);
    for (const auto& cardId : batch) {
        EXPECT_EQ(replicationCardMap.Get(cardId)->GetState(), EReplicationCardState::Normal);
    }

    batch = BuildReadyToMigrateReplicationCardBatch(replicationCardMap, 15);
    EXPECT_EQ(batch.size(), 10ull);
    for (const auto& cardId : batch) {
        EXPECT_EQ(replicationCardMap.Get(cardId)->GetState(), EReplicationCardState::Normal);
    }
}

TEST_F(TReplcationCardBatcherTest, SmallCollocations)
{
    TEntityMap<TReplicationCard> replicationCardMap;
    TEntityMap<TReplicationCardCollocation> collocationMap;
    auto* collocation = AddReplicationCardCollocation(10, &collocationMap, &replicationCardMap);
    auto* collocation2 = AddReplicationCardCollocation(10, &collocationMap, &replicationCardMap);


    auto batch = BuildReadyToMigrateReplicationCardBatch(replicationCardMap, 5);
    EXPECT_EQ(batch.size(), 10ull);

    auto* card = *collocation->ReplicationCards().begin();
    card->SetState(EReplicationCardState::RevokingShortcutsForMigration);
    batch = BuildReadyToMigrateReplicationCardBatch(replicationCardMap, 15);
    EXPECT_EQ(batch.size(), 10ull);

    collocation2->SetState(EReplicationCardCollocationState::Emigrating);
    batch = BuildReadyToMigrateReplicationCardBatch(replicationCardMap, 15);
    EXPECT_TRUE(batch.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDataNode
