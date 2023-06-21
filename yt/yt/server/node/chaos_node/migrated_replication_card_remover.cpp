#include "migrated_replication_card_remover.h"

#include "automaton.h"
#include "bootstrap.h"
#include "chaos_manager.h"
#include "chaos_slot.h"
#include "private.h"
#include "public.h"


#include <yt/yt/server/lib/chaos_node/config.h>

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
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChaosNodeLogger;

static constexpr int MigratedReplicatedCardRemoveBatchSize = 128;

////////////////////////////////////////////////////////////////////////////////

class TMigratedReplicationCardRemover
    : public IMigratedReplicationCardRemover
{
public:
    TMigratedReplicationCardRemover(
        TMigratedReplicationCardRemoverConfigPtr config,
        IChaosSlotPtr slot,
        IBootstrap* bootstrap)
        : Slot_(slot)
        , Bootstrap_(bootstrap)
        , RemoverExecutor_(New<TPeriodicExecutor>(
            slot->GetAutomatonInvoker(NChaosNode::EAutomatonThreadQueue::MigrationDepartment),
            BIND(&TMigratedReplicationCardRemover::PeriodicMigratedReplicationCardsRemoval, MakeWeak(this)),
            config->RemovePeriod))
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
    IBootstrap* const Bootstrap_;

    NConcurrency::TPeriodicExecutorPtr RemoverExecutor_;

    THashSet<TReplicationCardId> MigratedReplicationCardsToRemove_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void Save(TSaveContext& context) const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;
        Save(context, MigratedReplicationCardsToRemove_);
    }

    void Load(TLoadContext& context) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        Load(context, MigratedReplicationCardsToRemove_);
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        MigratedReplicationCardsToRemove_.clear();
    }

    void EnqueueRemoval(TReplicationCardId migratedReplicationCardId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        MigratedReplicationCardsToRemove_.insert(migratedReplicationCardId);
    }

    void ConfirmRemoval(TReplicationCardId migratedReplicationCardId) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        MigratedReplicationCardsToRemove_.erase(migratedReplicationCardId);
    }

    void PeriodicMigratedReplicationCardsRemoval()
    {
        std::vector<TReplicationCardId> batch;
        batch.reserve(MigratedReplicatedCardRemoveBatchSize);
        {
            int batchSize = 0;
            for (auto replicationCardId : MigratedReplicationCardsToRemove_) {
                if (batchSize++ >= MigratedReplicatedCardRemoveBatchSize) {
                    break;
                }

                batch.push_back(replicationCardId);
            }
        }

        for (auto replicationCardId : batch) {
            auto channel = Bootstrap_->GetClusterConnection()->GetChaosChannelByCardId(replicationCardId);
            auto proxy = TChaosNodeServiceProxy(std::move(channel));

            auto req = proxy.RemoveReplicationCard();
            SetMutationId(req, GenerateMutationId(), false);
            ToProto(req->mutable_replication_card_id(), replicationCardId);

            req->Invoke()
                .Apply(
                    BIND([=] (const TChaosNodeServiceProxy::TErrorOrRspRemoveReplicationCardPtr& rspOrError) {
                        if (!rspOrError.IsOK()) {
                            YT_LOG_WARNING(rspOrError, "Failed to remove replication card (ReplicationCardId: %v)",
                                replicationCardId);
                        }
                    }));
        }
    }
};

IMigratedReplicationCardRemoverPtr CreateMigratedReplicationCardRemover(
    TMigratedReplicationCardRemoverConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap)
{
    return New<TMigratedReplicationCardRemover>(
        std::move(config),
        std::move(slot),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
