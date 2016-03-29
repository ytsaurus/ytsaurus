#include "sys_node.h"
#include "private.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/cypress_server/node_detail.h>
#include <yt/server/cypress_server/node_proxy_detail.h>

#include <yt/server/hydra/hydra_manager.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

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
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        TMapNode* trunkNode)
        : TBase(
            typeHandler,
            bootstrap,
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
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        RequireLeader();

        auto hydraFacade = Bootstrap_->GetHydraFacade();

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
            auto hydraManager = hydraFacade->GetHydraManager();
            BuildYsonFluently(consumer)
                .Value(hydraManager->GetAutomatonVersion().ToRevision());
            return true;
        }

        if (key == "chunk_replicator_enabled") {
            auto chunkManager = Bootstrap_->GetChunkManager();
            BuildYsonFluently(consumer)
                .Value(chunkManager->IsReplicatorEnabled());
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual void ValidateCustomAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& /*oldValue*/,
        const TNullable<TYsonString>& newValue) override
    {
        if (key == "disable_chunk_replicator" && newValue) {
            ConvertTo<bool>(*newValue);
            return;
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSysNodeTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TSysNodeTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::SysNode;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        NTransactionServer::TTransaction* transaction) override
    {
        return New<TSysNodeProxy>(
            this,
            Bootstrap_,
            transaction,
            trunkNode);
    }

};

INodeTypeHandlerPtr CreateSysNodeTypeHandler(TBootstrap* bootstrap)
{
    return New<TSysNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
