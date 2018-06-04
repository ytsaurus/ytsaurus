#include "table_replica_proxy.h"
#include "table_replica.h"
#include "tablet_manager.h"

#include <yt/server/object_server/object_detail.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/chunk_server/chunk_list.h>

#include <yt/server/table_server/replicated_table_node.h>

#include <yt/server/cell_master/bootstrap.h>

#include <yt/ytlib/transaction_client/timestamp_provider.h>

#include <yt/ytlib/tablet_client/table_replica_ypath.pb.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTabletServer {

using namespace NYTree;
using namespace NYson;
using namespace NTabletClient;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TTableReplicaProxy
    : public TNonversionedObjectProxyBase<TTableReplica>
{
public:
    TTableReplicaProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTableReplica* replica)
        : TBase(bootstrap, metadata, replica)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTableReplica> TBase;

    virtual void ValidateRemoval() override
    {
        auto* replica = GetThisImpl();
        auto* table = replica->GetTable();
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->LockNode(table, nullptr, TLockRequest(ELockMode::Exclusive));
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        attributes->push_back(EInternedAttributeKey::ClusterName);
        attributes->push_back(EInternedAttributeKey::ReplicaPath);
        attributes->push_back(EInternedAttributeKey::TablePath);
        attributes->push_back(EInternedAttributeKey::StartReplicationTimestamp);
        attributes->push_back(EInternedAttributeKey::State);
        attributes->push_back(EInternedAttributeKey::Mode);
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Tablets)
            .SetOpaque(true));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicationLagTime)
            .SetOpaque(true));

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        auto* replica = GetThisImpl();
        auto* table = replica->GetTable();
        const auto& timestampProvider = Bootstrap_->GetTimestampProvider();

        switch (key) {
            case EInternedAttributeKey::ClusterName:
                BuildYsonFluently(consumer)
                    .Value(replica->GetClusterName());
                return true;

            case EInternedAttributeKey::ReplicaPath:
                BuildYsonFluently(consumer)
                    .Value(replica->GetReplicaPath());
                return true;

            case EInternedAttributeKey::StartReplicationTimestamp:
                BuildYsonFluently(consumer)
                    .Value(replica->GetStartReplicationTimestamp());
                return true;

            case EInternedAttributeKey::TablePath: {
                const auto& cypressManager = Bootstrap_->GetCypressManager();
                BuildYsonFluently(consumer)
                    .Value(cypressManager->GetNodePath(replica->GetTable(), nullptr));
                return true;
            }

            case EInternedAttributeKey::State:
                BuildYsonFluently(consumer)
                    .Value(replica->GetState());
                return true;

            case EInternedAttributeKey::Mode:
                BuildYsonFluently(consumer)
                    .Value(replica->GetMode());
                return true;

            case EInternedAttributeKey::Tablets:
                BuildYsonFluently(consumer)
                    .DoListFor(table->Tablets(), [=] (TFluentList fluent, TTablet* tablet) {
                        const auto* chunkList = tablet->GetChunkList();
                        const auto* replicaInfo = tablet->GetReplicaInfo(replica);
                        fluent
                            .Item().BeginMap()
                                .Item("tablet_id").Value(tablet->GetId())
                                .Item("state").Value(replicaInfo->GetState())
                                .Item("current_replication_row_index").Value(replicaInfo->GetCurrentReplicationRowIndex())
                                .Item("current_replication_timestamp").Value(replicaInfo->GetCurrentReplicationTimestamp())
                                .Item("replication_lag_time").Value(tablet->ComputeReplicationLagTime(
                                    timestampProvider->GetLatestTimestamp(), *replicaInfo))
                                .DoIf(!replicaInfo->Error().IsOK(), [&] (TFluentMap fluent) {
                                    fluent.Item("replication_error").Value(replicaInfo->Error());
                                })
                                .Item("trimmed_row_count").Value(tablet->GetTrimmedRowCount())
                                .Item("flushed_row_count").Value(chunkList->Statistics().LogicalRowCount)
                            .EndMap();
                    });
                return true;

            case EInternedAttributeKey::ReplicationLagTime:
                BuildYsonFluently(consumer)
                    .Value(replica->ComputeReplicationLagTime(timestampProvider->GetLatestTimestamp()));
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Alter);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NTabletClient::NProto, Alter)
    {
        Y_UNUSED(response);

        DeclareMutating();

        auto enabled = request->has_enabled() ? MakeNullable(request->enabled()) : Null;
        auto mode = request->has_mode() ? MakeNullable(ETableReplicaMode(request->mode())) : Null;

        context->SetRequestInfo("Enabled: %v, Mode: %v",
            enabled,
            mode);

        auto* replica = GetThisImpl();

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        if (enabled) {
            tabletManager->SetTableReplicaEnabled(replica, *enabled);
        }
        if (mode) {
            tabletManager->SetTableReplicaMode(replica, *mode);
        }

        context->Reply();
    }
};

IObjectProxyPtr CreateTableReplicaProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTableReplica* replica)
{
    return New<TTableReplicaProxy>(bootstrap, metadata, replica);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

