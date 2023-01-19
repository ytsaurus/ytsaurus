#include "scion_type_handler.h"

#include "grafting_manager.h"
#include "node_detail.h"
#include "scion_node.h"
#include "scion_proxy.h"

#include <yt/yt/ytlib/sequoia_client/resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSequoiaClient;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class TScionTypeHandler
    : public TMapNodeTypeHandlerImpl<TScionNode>
{
public:
    using TMapNodeTypeHandlerImpl::TMapNodeTypeHandlerImpl;

    EObjectType GetObjectType() const override
    {
        return EObjectType::Scion;
    }

    ETypeFlags GetFlags() const override
    {
        return ETypeFlags::Removable;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TScionNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateScionProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    void DoDestroy(TScionNode* node) override
    {
        if (node->IsTrunk()) {
            const auto& graftingManager = Bootstrap_->GetGraftingManager();
            graftingManager->OnScionDestroyed(node);
        }

        TMapNodeTypeHandlerImpl::DoDestroy(node);
    }

    void DoDestroySequoiaObject(
        TScionNode* node,
        const NSequoiaClient::ISequoiaTransactionPtr& transaction) noexcept override
    {
        // TODO: Rewrite after removal implementation.
        if (node->IsTrunk()) {
            NRecords::TResolveNodeKey key{
                .Path = node->GetPath(),
            };
            transaction->DeleteRow(key);
        }
    }

    void DoBeginCopy(
        TScionNode* /*node*/,
        TBeginCopyContext* /*context*/) override
    {
        THROW_ERROR_EXCEPTION("Cross-cell copying of scions is not supported");
    }

    void DoEndCopy(
        TScionNode* /*trunkNode*/,
        TEndCopyContext* /*context*/,
        ICypressNodeFactory* /*factory*/) override
    {
        THROW_ERROR_EXCEPTION("Cross-cell copying of scions is not supported");
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateScionTypeHandler(TBootstrap* bootstrap)
{
    return New<TScionTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
