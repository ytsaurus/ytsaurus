#include "sys_node_proxy.h"
#include "private.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/cypress_server/node_proxy_detail.h>

#include <yt/server/hydra/hydra_manager.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/object_server/interned_attributes.h>

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

        descriptors->push_back(EInternedAttributeKey::CellTag);
        descriptors->push_back(EInternedAttributeKey::PrimaryCellTag);
        descriptors->push_back(EInternedAttributeKey::CellId);
        descriptors->push_back(EInternedAttributeKey::PrimaryCellId);
        descriptors->push_back(EInternedAttributeKey::CurrentCommitRevision);
        descriptors->push_back(EInternedAttributeKey::ChunkReplicatorEnabled);
        descriptors->push_back(EInternedAttributeKey::RegisteredMasterCellTags);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Config)
            .SetWritable(true)
            .SetOpaque(true));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        switch (key) {
            case EInternedAttributeKey::CellTag:
                BuildYsonFluently(consumer)
                    .Value(Bootstrap_->GetCellTag());
                return true;

            case EInternedAttributeKey::PrimaryCellTag:
                BuildYsonFluently(consumer)
                    .Value(Bootstrap_->GetPrimaryCellTag());
                return true;

            case EInternedAttributeKey::CellId:
                BuildYsonFluently(consumer)
                    .Value(Bootstrap_->GetCellId());
                return true;

            case EInternedAttributeKey::PrimaryCellId:
                BuildYsonFluently(consumer)
                    .Value(Bootstrap_->GetPrimaryCellId());
                return true;

            case EInternedAttributeKey::CurrentCommitRevision: {
                const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
                BuildYsonFluently(consumer)
                    .Value(hydraManager->GetAutomatonVersion().ToRevision());
                return true;
            }

            case EInternedAttributeKey::ChunkReplicatorEnabled: {
                RequireLeader();
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                BuildYsonFluently(consumer)
                    .Value(chunkManager->IsReplicatorEnabled());
                return true;
            }

            case EInternedAttributeKey::RegisteredMasterCellTags: {
                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                BuildYsonFluently(consumer)
                    .Value(multicellManager->GetRegisteredMasterCellTags());
                return true;
            }

            case EInternedAttributeKey::Config: {
                const auto& configManager = Bootstrap_->GetConfigManager();
                BuildYsonFluently(consumer)
                    .Value(configManager->GetConfig());
                return true;
            }

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

} // namespace NObjectServer
} // namespace NYT
