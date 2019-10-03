#include "sys_node_proxy.h"
#include "private.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/server/lib/hydra/hydra_manager.h>

#include <yt/server/master/chunk_server/chunk_manager.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/multicell_manager.h>

namespace NYT::NObjectServer {

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

        descriptors->push_back(EInternedAttributeKey::CellTag);
        descriptors->push_back(EInternedAttributeKey::PrimaryCellTag);
        descriptors->push_back(EInternedAttributeKey::CellId);
        descriptors->push_back(EInternedAttributeKey::PrimaryCellId);
        descriptors->push_back(EInternedAttributeKey::CurrentCommitRevision);
        descriptors->push_back(EInternedAttributeKey::ChunkReplicatorEnabled);
        descriptors->push_back(EInternedAttributeKey::ChunkRefreshEnabled);
        descriptors->push_back(EInternedAttributeKey::ChunkRequisitionUpdateEnabled);
        descriptors->push_back(EInternedAttributeKey::RegisteredMasterCellTags);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Config)
            .SetWritable(true)
            .SetOpaque(true));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& configManager = Bootstrap_->GetConfigManager();

        switch (key) {
            case EInternedAttributeKey::CellTag:
                BuildYsonFluently(consumer)
                    .Value(multicellManager->GetCellTag());
                return true;

            case EInternedAttributeKey::PrimaryCellTag:
                BuildYsonFluently(consumer)
                    .Value(multicellManager->GetPrimaryCellTag());
                return true;

            case EInternedAttributeKey::CellId:
                BuildYsonFluently(consumer)
                    .Value(multicellManager->GetCellId());
                return true;

            case EInternedAttributeKey::PrimaryCellId:
                BuildYsonFluently(consumer)
                    .Value(multicellManager->GetPrimaryCellId());
                return true;

            case EInternedAttributeKey::CurrentCommitRevision:
                BuildYsonFluently(consumer)
                    .Value(hydraManager->GetAutomatonVersion().ToRevision());
                return true;

            case EInternedAttributeKey::ChunkReplicatorEnabled:
                RequireLeader();
                BuildYsonFluently(consumer)
                    .Value(chunkManager->IsChunkReplicatorEnabled());
                return true;

            case EInternedAttributeKey::ChunkRefreshEnabled:
                RequireLeader();
                BuildYsonFluently(consumer)
                    .Value(chunkManager->IsChunkRefreshEnabled());
                return true;

            case EInternedAttributeKey::ChunkRequisitionUpdateEnabled:
                RequireLeader();
                BuildYsonFluently(consumer)
                    .Value(chunkManager->IsChunkRequisitionUpdateEnabled());
                return true;

            case EInternedAttributeKey::ChunkSealerEnabled:
                RequireLeader();
                BuildYsonFluently(consumer)
                    .Value(chunkManager->IsChunkSealerEnabled());
                return true;

            case EInternedAttributeKey::RegisteredMasterCellTags:
                BuildYsonFluently(consumer)
                    .Value(multicellManager->GetRegisteredMasterCellTags());
                return true;

            case EInternedAttributeKey::Config:
                BuildYsonFluently(consumer)
                    .Value(configManager->GetConfig());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        switch (key) {
            case EInternedAttributeKey::Config: {
                ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
                const auto& configManager = Bootstrap_->GetConfigManager();
                configManager->SetConfig(ConvertTo<TDynamicClusterConfigPtr>(value));
                return true;
            }

            default:
                break;
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

} // namespace NYT::NObjectServer
