#include "replication_card_batcher.h"

#include "replication_card.h"
#include "replication_card_collocation.h"

#include <yt/yt/server/lib/hydra/entity_map.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

std::vector<TReplicationCardId> BuildReadyToMigrateReplicationCardBatch(
    const NHydra::TEntityMap<TReplicationCard>& replicationCardMap,
    int targetBatchSize)
{
    std::vector<TReplicationCardId> cardIdsToMigrate;
    THashSet<TReplicationCardCollocation*> checkedCollocations;
    for (const auto& [id, replicationCard] : replicationCardMap) {
        if (!replicationCard->IsReadyToMigrate()) {
            continue;
        }

        if (auto* collocation = replicationCard->GetCollocation(); !collocation) {
            cardIdsToMigrate.push_back(id);
        } else {
            if (checkedCollocations.insert(collocation).second) {
                bool isCollocationReady = !collocation->IsMigrating() && AllOf(collocation->ReplicationCards(),
                    [](const auto* replicationCard) {
                        return replicationCard->IsReadyToMigrate();
                    });

                if (isCollocationReady) {
                    for (auto* replicationCard : collocation->ReplicationCards()) {
                        cardIdsToMigrate.push_back(replicationCard->GetId());
                    }
                }
            }
        }

        if (std::ssize(cardIdsToMigrate) >= targetBatchSize) {
            break;
        }
    }

    return cardIdsToMigrate;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
