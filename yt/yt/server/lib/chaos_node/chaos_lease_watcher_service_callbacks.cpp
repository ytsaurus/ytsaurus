#include "chaos_lease_watcher_service_callbacks.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NChaosNode {

using namespace NChaosClient;
using namespace NObjectClient;
using namespace NTransactionClient;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TChaosLeaseWatcherCallbacks
    : public IChaosLeaseWatcherCallbacks
{
public:
    explicit TChaosLeaseWatcherCallbacks(TCtxChaosLeaseWatcherPtr context)
        : Context_(std::move(context))
    { }

    void OnObjectChanged(
        const TChaosLeasePtr& chaosLease,
        TTimestamp timestamp) override
    {
        auto* response = Context_->Response().mutable_chaos_lease_changed();
        response->set_chaos_lease_cache_timestamp(ToProto(timestamp));
        response->set_timeout(ToProto(chaosLease->Timeout));
        ToProto(response->mutable_coordinator_cell_ids(), chaosLease->CoordinatorCellIds);
        Context_->Reply();
    }

    void OnObjectMigrated(TCellId destination) override
    {
        ToProto(
            Context_->Response().mutable_chaos_lease_migrated()->mutable_migrate_to_cell_id(),
            destination);
        Context_->Reply();
    }

    void OnObjectDeleted() override
    {
        Context_->Response().mutable_chaos_lease_deleted();
        Context_->Reply();
    }

    void OnInstanceIsNotLeader() override
    {
        Context_->Response().mutable_instance_is_not_leader();
        Context_->Reply();
    }

    void OnNothingChanged() override
    {
        Context_->Response().mutable_chaos_lease_not_changed();
        Context_->Reply();
    }

    void OnUnknownObject() override
    {
        Context_->Response().mutable_unknown_chaos_lease();
        Context_->Reply();
    }

private:
    const TCtxChaosLeaseWatcherPtr Context_;
};

DEFINE_REFCOUNTED_TYPE(TChaosLeaseWatcherCallbacks)

} // namespace

IChaosLeaseWatcherCallbacksPtr CreateChaosLeaseWatcherCallbacks(
    TCtxChaosLeaseWatcherPtr context)
{
    return New<TChaosLeaseWatcherCallbacks>(std::move(context));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
