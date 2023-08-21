#include "foreign_migrated_replication_card_remover.h"

#include "automaton.h"
#include "bootstrap.h"
#include "chaos_manager.h"
#include "chaos_slot.h"
#include "private.h"
#include "public.h"
#include "replication_card.h"

#include <yt/yt/server/lib/chaos_node/config.h>
#include <yt/yt/server/node/chaos_node/chaos_manager.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/public.h>
#include <yt/yt/ytlib/chaos_client/replication_card_channel_factory.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/rpc/retrying_channel.h>

namespace NYT::NChaosNode {

using namespace NChaosClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChaosNodeLogger;

static constexpr int MigratedReplicatedCardTtlRemoveBatchSize = 128;

////////////////////////////////////////////////////////////////////////////////

class TForeignMigratedReplicationCardRemover
    : public IForeignMigratedReplicationCardRemover
{
public:
    TForeignMigratedReplicationCardRemover(
        TForeignMigratedReplicationCardRemoverConfigPtr config,
        IChaosSlotPtr slot,
        ISimpleHydraManagerPtr hydraManager)
        : Slot_(std::move(slot))
        , HydraManager_(std::move(hydraManager))
        , RemoverExecutor_(New<TPeriodicExecutor>(
            Slot_->GetAutomatonInvoker(NChaosNode::EAutomatonThreadQueue::MigrationDepartment),
            BIND(&TForeignMigratedReplicationCardRemover::PeriodicMigratedReplicationCardsRemoval, MakeWeak(this)),
            config->RemovePeriod))
        , ReplicationCardKeepAlivePeriod_(config->ReplicationCardKeepAlivePeriod)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);
    }

    void Start() override
    {
        RemoverExecutor_->Start();
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(RemoverExecutor_->Stop());
    }

private:
    const IChaosSlotPtr Slot_;
    const ISimpleHydraManagerPtr HydraManager_;

    NConcurrency::TPeriodicExecutorPtr RemoverExecutor_;
    TDuration ReplicationCardKeepAlivePeriod_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    bool IsDomesticReplicationCard(TReplicationCardId replicationCardId)
    {
        return CellTagFromId(replicationCardId) == CellTagFromId(Slot_->GetCellId());
    }

    void PeriodicMigratedReplicationCardsRemoval()
    {
        const auto& chaosManager = Slot_->GetChaosManager();
        NChaosNode::NProto::TReqRemoveMigratedReplicationCards req;
        int batchSize = 0;
        for (const auto& [id, replicationCard] : chaosManager->ReplicationCards()) {
            if (IsDomesticReplicationCard(id)) {
                continue;
            }

            if (!replicationCard->IsMigrated()) {
                continue;
            }

            if (Now() - TimestampToInstant(replicationCard->GetCurrentTimestamp()).second <= ReplicationCardKeepAlivePeriod_) {
                continue;
            }

            if (batchSize++ >= MigratedReplicatedCardTtlRemoveBatchSize) {
                break;
            }

            auto* protoMigratedCard = req.add_migrated_cards();

            ToProto(protoMigratedCard->mutable_replication_card_id(), id);
            protoMigratedCard->set_migration_timestamp(replicationCard->GetCurrentTimestamp());
        }

        if (req.migrated_cards_size() == 0) {
            return;
        }

        auto mutation = CreateMutation(HydraManager_, req);
        YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger));
    }
};

IForeignMigratedReplicationCardRemoverPtr CreateForeignMigratedReplicationCardRemover(
    TForeignMigratedReplicationCardRemoverConfigPtr config,
    IChaosSlotPtr slot,
    ISimpleHydraManagerPtr hydraManager)
{
    return New<TForeignMigratedReplicationCardRemover>(
        std::move(config),
        std::move(slot),
        std::move(hydraManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
