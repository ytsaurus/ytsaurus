#include "sys_node_proxy.h"
#include "private.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/cypress_server/node_proxy_detail.h>

#include <yt/server/hydra/hydra_manager.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config_manager.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>

namespace NYT {
namespace NObjectServer {

using namespace NYson;
using namespace NYTree;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TSysNodeProxy
    : public TMapNodeProxy
{
public:
    TSysNodeProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TMapNode* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

private:
    typedef TMapNodeProxy TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back("cell_tag");
        descriptors->push_back("primary_cell_tag");
        descriptors->push_back("cell_id");
        descriptors->push_back("primary_cell_id");
        descriptors->push_back("current_commit_revision");
        descriptors->push_back("chunk_replicator_enabled");
        descriptors->push_back("registered_master_cell_tags");
        descriptors->push_back(TAttributeDescriptor("config")
            .SetWritable(true)
            .SetOpaque(true));
    }

    virtual bool GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer) override
    {
        if (key == "cell_tag") {
            BuildYsonFluently(consumer)
                .Value(Bootstrap_->GetCellTag());
            return true;
        }

        if (key == "primary_cell_tag") {
            BuildYsonFluently(consumer)
                .Value(Bootstrap_->GetPrimaryCellTag());
            return true;
        }

        if (key == "cell_id") {
            BuildYsonFluently(consumer)
                .Value(Bootstrap_->GetCellId());
            return true;
        }

        if (key == "primary_cell_id") {
            BuildYsonFluently(consumer)
                .Value(Bootstrap_->GetPrimaryCellId());
            return true;
        }

        if (key == "current_commit_revision") {
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            BuildYsonFluently(consumer)
                .Value(hydraManager->GetAutomatonVersion().ToRevision());
            return true;
        }

        if (key == "chunk_replicator_enabled") {
            RequireLeader();
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            BuildYsonFluently(consumer)
                .Value(chunkManager->IsReplicatorEnabled());
            return true;
        }

        if (key == "registered_master_cell_tags") {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            BuildYsonFluently(consumer)
                .Value(multicellManager->GetRegisteredMasterCellTags());
            return true;
        }

        if (key == "config") {
            const auto& configManager = Bootstrap_->GetConfigManager();
            BuildYsonFluently(consumer)
                .Value(configManager->GetConfig());
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const TString& key, const TYsonString& value) override
    {
        if (key == "config") {
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
            const auto& configManager = Bootstrap_->GetConfigManager();
            configManager->SetConfig(ConvertTo<TDynamicClusterConfigPtr>(value));
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateSysNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TMapNode* trunkNode)
{
    return New<TSysNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
