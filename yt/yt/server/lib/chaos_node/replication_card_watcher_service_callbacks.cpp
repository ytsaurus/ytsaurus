#include "replication_card_watcher_service_callbacks.h"

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NChaosNode {

using namespace NYT::NChaosClient;
using namespace NYT::NElection;
using namespace NYT::NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TReplicationCardWatcherCallbacks
    : public IReplicationCardWatcherCallbacks
{
public:
    explicit TReplicationCardWatcherCallbacks(
        TCtxReplicationCardWatcherPtr context)
        : Context_(std::move(context))
    { }

    void OnReplicationCardChanged(
        const TReplicationCardPtr& replicationCard,
        TTimestamp timestamp) override
    {
        auto* response = Context_->Response().mutable_replication_card_changed();
        response->set_replication_card_cache_timestamp(timestamp);
        ToProto(
            response->mutable_replication_card(),
            *replicationCard,
            MinimalFetchOptions);
        Context_->Reply();
    }

    void OnReplicationCardMigrated(TCellId destination) override
    {
        auto& response = Context_->Response();
        auto* migrateToCellId = response.mutable_replication_card_migrated()->mutable_migrate_to_cell_id();
        ToProto(migrateToCellId, destination);
        Context_->Reply();
    }

    void OnReplicationCardDeleted() override
    {
        auto& response = Context_->Response();
        response.mutable_replication_card_deleted();
        Context_->Reply();
    }

    void OnInstanceIsNotLeader() override
    {
        auto& response = Context_->Response();
        response.mutable_instance_is_not_leader();
        Context_->Reply();
    }

    void OnNothingChanged() override
    {
        auto& response = Context_->Response();
        response.mutable_replication_card_not_changed();
        Context_->Reply();
    }

    void OnUnknownReplicationCard() override
    {
        auto& response = Context_->Response();
        response.mutable_unknown_replication_card();
        Context_->Reply();
    }

private:
    const TCtxReplicationCardWatcherPtr Context_;
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardWatcherCallbacks);

} // namespace

NChaosClient::IReplicationCardWatcherCallbacksPtr CreateReplicationCardWatcherCallbacks(
    TCtxReplicationCardWatcherPtr context)
{
    return New<TReplicationCardWatcherCallbacks>(std::move(context));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
