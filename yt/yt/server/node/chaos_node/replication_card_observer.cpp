#include "replication_card_observer.h"

#include "chaos_manager.h"
#include "chaos_slot.h"
#include "private.h"
#include "public.h"
#include "replication_card.h"

#include <yt/yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/hydra_common/hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/mutation.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosNode {

using namespace NObjectClient;
using namespace NConcurrency;
using namespace NChaosClient;
using namespace NTabletClient;
using namespace NTransactionClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChaosNodeLogger;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpiredReplicaHistory* protoExpiredHistory, const TExpiredReplicaHistory& expiredHistory)
{
    ToProto(protoExpiredHistory->mutable_replica_id(), expiredHistory.ReplicaId);
    protoExpiredHistory->set_retain_timestamp(expiredHistory.RetainTimestamp);
}

void FromProto(TExpiredReplicaHistory* expiredHistory, const NProto::TExpiredReplicaHistory& protoExpiredHistory)
{
    FromProto(&expiredHistory->ReplicaId, protoExpiredHistory.replica_id());
    expiredHistory->RetainTimestamp = protoExpiredHistory.retain_timestamp();
}

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardObserver
    : public IReplicationCardObserver
{
public:
    TReplicationCardObserver(
        TReplicationCardObserverConfigPtr config,
        IChaosSlotPtr slot)
        : Config_(std::move(config))
        , Slot_(std::move(slot))
        , ObserveExecutor_(New<TPeriodicExecutor>(
            Slot_->GetAutomatonInvoker(),
            BIND(&TReplicationCardObserver::Observe, MakeWeak(this)),
            Config_->ObservationPeriod))
    { }

    void Start() override
    {
        ObserveExecutor_->Start();
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(ObserveExecutor_->Stop());
    }

private:
    const TReplicationCardObserverConfigPtr Config_;
    const IChaosSlotPtr Slot_;

    NConcurrency::TPeriodicExecutorPtr ObserveExecutor_;

    void Observe()
    {
        try {
            GuardedObserve();
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Error while replication card observation");
        }
    }

    void GuardedObserve()
    {
        const auto& chaosManager = Slot_->GetChaosManager();
        auto replicationCardIds = GetKeys(chaosManager->ReplicationCards());
        i64 cardsPerRound = Config_->ReplicationCardCountPerRound;
        i64 roundCount = (std::ssize(replicationCardIds) + cardsPerRound - 1) / cardsPerRound;

        YT_LOG_DEBUG("Starting replication card observer iteration (ReplicationCardCount: %v, Rounds: %v)",
            std::ssize(replicationCardIds),
            roundCount);

        for (i64 roundIndex = 0; roundIndex < roundCount; ++roundIndex) {
            std::vector<TExpiredReplicaHistory> expiredHistories;

            for (i64 index = roundIndex * cardsPerRound;
                index < (roundIndex + 1)* cardsPerRound && index < std::ssize(replicationCardIds);
                ++index)
            {
                ObserveReplicationCard(replicationCardIds[index], &expiredHistories);
            }

            if (!expiredHistories.empty() > 0) {
                NProto::TReqRemoveExpiredReplicaHistory request;
                ToProto(request.mutable_expired_replica_histories(), expiredHistories);

                const auto& hydraManager = Slot_->GetHydraManager();
                auto mutation = CreateMutation(hydraManager, request);
                YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
            }

            YT_LOG_DEBUG("Replication card observer finished round (RoundIndex: %v, WillYield: %v)",
                roundIndex,
                roundIndex < roundCount - 1);

            if (roundIndex < roundCount - 1) {
                Yield();
            }
        }

        YT_LOG_DEBUG("Finished replication card observer iteration");
    }

    void ObserveReplicationCard(TReplicationCardId replicationCardId, std::vector<TExpiredReplicaHistory> *expiredHistories)
    {
        const auto& chaosManager = Slot_->GetChaosManager();
        auto* replicationCard = chaosManager->FindReplicationCard(replicationCardId);
        if (!replicationCard) {
            return;
        }

        auto checkExpiredHistory = [expiredHistories] (TReplicaId replicaId, const TReplicaInfo& replica, TTimestamp timestamp) {
            if(std::ssize(replica.History) > 1 && replica.History[1].Timestamp <= timestamp) {
                expiredHistories->push_back({replicaId, timestamp});
            }
        };

        auto minTimestamp = MaxTimestamp;
        for (const auto& [replicaId, replica] : replicationCard->Replicas()) {
            auto replicaMinTimestamp = GetReplicationProgressMinTimestamp(replica.ReplicationProgress);
            minTimestamp = std::min(minTimestamp, replicaMinTimestamp);

            if (replica.ContentType == ETableReplicaContentType::Data) {
                checkExpiredHistory(replicaId, replica, replicaMinTimestamp);
            }
        }

        for (const auto& [replicaId, replica] : replicationCard->Replicas()) {
            if (replica.ContentType == ETableReplicaContentType::Queue) {
                checkExpiredHistory(replicaId, replica, minTimestamp);
            }
        }
    }
};

IReplicationCardObserverPtr CreateReplicationCardObserver(
    TReplicationCardObserverConfigPtr config,
    IChaosSlotPtr slot)
{
    return New<TReplicationCardObserver>(std::move(config), std::move(slot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
