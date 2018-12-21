#include "tablet_type_handler.h"
#include "table_replica_proxy.h"
#include "table_replica.h"
#include "tablet_manager.h"

#include <yt/server/object_server/type_handler_detail.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/table_server/replicated_table_node.h>

#include <yt/client/object_client/helpers.h>

namespace NYT::NTabletServer {

using namespace NHydra;
using namespace NYTree;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCypressServer;
using namespace NTableServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TTableReplicaTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTableReplica>
{
public:
    TTableReplicaTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TTableReplica>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
        , Bootstrap_(bootstrap)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::TableReplica;
    }

    virtual ETypeFlags GetFlags() const override
    {
        return ETypeFlags::Creatable;
    }

    virtual TObjectBase* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto tablePath = attributes->GetAndRemove<TString>("table_path");
        auto clusterName = attributes->GetAndRemove<TString>("cluster_name");
        auto replicaPath = attributes->GetAndRemove<TString>("replica_path");
        auto startReplicationTimestamp = attributes->GetAndRemove<NTransactionClient::TTimestamp>("start_replication_timestamp", NTransactionClient::MinTimestamp);
        auto startReplicationRowIndexes = attributes->FindAndRemove<std::vector<i64>>("start_replication_row_indexes");
        auto mode = attributes->GetAndRemove<ETableReplicaMode>("mode", ETableReplicaMode::Async);
        auto preserveTimestamps = attributes->GetAndRemove<bool>("preserve_timestamps", true);
        auto atomicity = attributes->GetAndRemove<NTransactionClient::EAtomicity>("atomicity", NTransactionClient::EAtomicity::Full);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* trunkNode = cypressManager->ResolvePathToTrunkNode(tablePath);
        if (trunkNode->GetType() != EObjectType::ReplicatedTable) {
            THROW_ERROR_EXCEPTION("%v is not a replicated table",
                tablePath);
        }
        auto* table = trunkNode->As<TReplicatedTableNode>();

        if (startReplicationRowIndexes && startReplicationRowIndexes->size() != table->Tablets().size()) {
            THROW_ERROR_EXCEPTION("Invalid size of \"start_replication_row_indexes\": expected zero or %v, got %v",
                table->Tablets().size(),
                startReplicationRowIndexes->size());
        }

        cypressManager->LockNode(table, nullptr, ELockMode::Exclusive);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        return tabletManager->CreateTableReplica(
            table,
            clusterName,
            replicaPath,
            mode,
            preserveTimestamps,
            atomicity,
            startReplicationTimestamp,
            startReplicationRowIndexes);
    }

private:
    TBootstrap* const Bootstrap_;

    virtual TString DoGetName(const TTableReplica* replica) override
    {
        return Format("table replica %v", replica->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TTableReplica* replica, TTransaction* /*transaction*/) override
    {
        return CreateTableReplicaProxy(Bootstrap_, &Metadata_, replica);
    }

    virtual void DoZombifyObject(TTableReplica* replica) override
    {
        TObjectTypeHandlerWithMapBase::DoDestroyObject(replica);
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->DestroyTableReplica(replica);
    }
};

IObjectTypeHandlerPtr CreateTableReplicaTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TTableReplica>* map)
{
    return New<TTableReplicaTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
