#include "document_node_type_handler.h"
#include "document_node.h"
#include "document_node_proxy.h"

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

class TDocumentNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TDocumentNode>
{
private:
    using TBase = TCypressNodeTypeHandlerBase<TDocumentNode>;

public:
    using TBase::TBase;

    virtual NObjectClient::EObjectType GetObjectType() const override
    {
        return EObjectType::Document;
    }

    virtual NYTree::ENodeType GetNodeType() const override
    {
        return ENodeType::Entity;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TDocumentNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateDocumentNodeProxy(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }

    virtual void DoBranch(
        const TDocumentNode* originatingNode,
        TDocumentNode* branchedNode,
        const TLockRequest& lockRequest) override
    {
        TBase::DoBranch(originatingNode, branchedNode, lockRequest);

        branchedNode->SetValue(CloneNode(originatingNode->GetValue()));
    }

    virtual void DoMerge(
        TDocumentNode* originatingNode,
        TDocumentNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        originatingNode->SetValue(branchedNode->GetValue());
    }

    virtual void DoClone(
        TDocumentNode* sourceNode,
        TDocumentNode* clonedNode,
        ICypressNodeFactory* factory,
        ENodeCloneMode mode,
        TAccount* account) override
    {
        TBase::DoClone(sourceNode, clonedNode, factory, mode, account);

        clonedNode->SetValue(CloneNode(sourceNode->GetValue()));
    }

    virtual bool HasBranchedChangesImpl(
        TDocumentNode* originatingNode,
        TDocumentNode* branchedNode) override
    {
        if (TBase::HasBranchedChangesImpl(originatingNode, branchedNode)) {
            return true;
        }

        return !AreNodesEqual(branchedNode->GetValue(), originatingNode->GetValue());
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateDocumentNodeTypeHandler(TBootstrap* bootstrap)
{
    return New<TDocumentNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
