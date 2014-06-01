#include "stdafx.h"
#include "journal_node.h"
#include "journal_node_proxy.h"
#include "private.h"

#include <server/chunk_server/chunk_owner_type_handler.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NJournalServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NChunkServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TJournalNode::TJournalNode(const TVersionedNodeId& id)
    : TChunkOwnerBase(id)
    , ReadConcern_(0)
    , WriteConcern_(0)
{ }

////////////////////////////////////////////////////////////////////////////////

class TJournalNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TJournalNode>
{
public:
    typedef TCypressNodeTypeHandlerBase<TJournalNode> TBase;

    explicit TJournalNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::Journal;
    }

    virtual ENodeType GetNodeType() override
    {
        return ENodeType::Entity;
    }

protected:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TJournalNode* trunkNode,
        TTransaction* transaction) override
    {
        return CreateJournalNodeProxy(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

    virtual std::unique_ptr<TJournalNode> DoCreate(
        const TVersionedNodeId& id,
        TTransaction* transaction,
        INodeTypeHandler::TReqCreate* request,
        INodeTypeHandler::TRspCreate* response) override
    {
        auto chunkManager = this->Bootstrap->GetChunkManager();
        auto objectManager = this->Bootstrap->GetObjectManager();

        auto node = TBase::DoCreate(id, transaction, request, response);

        // Create an empty chunk list and reference it from the node.
        auto* chunkList = chunkManager->CreateChunkList();
        node->SetChunkList(chunkList);
        YCHECK(chunkList->OwningNodes().insert(node.get()).second);
        objectManager->RefObject(chunkList);

        return node;
    }

    virtual void DoValidateCreated(TJournalNode* node) override
    {
        TBase::DoValidateCreated(node);

        int replicationFactor = node->GetReplicationFactor();
        int readConcern = node->GetReadConcern();
        int writeConcern = node->GetWriteConcern();

        if (replicationFactor == 0) {
            THROW_ERROR_EXCEPTION("\"replication_factor\" must be specified");
        }
        if (readConcern == 0) {
            THROW_ERROR_EXCEPTION("\"read_concern\" must be specified");
        }
        if (writeConcern == 0) {
            THROW_ERROR_EXCEPTION("\"write_concern\" must be specified");
        }

        if (readConcern > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"read_concern\" cannot be greater than \"replication_factor\"");
        }
        if (writeConcern > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"write_concern\" cannot be greater than \"replication_factor\"");
        }
        if (readConcern + writeConcern < replicationFactor + 1) {
            THROW_ERROR_EXCEPTION("Read/write concerns are not safe: read_concern + write_concern < replication_factor + 1");
        }
    }

    virtual void DoDestroy(TJournalNode* node) override
    {
        TBase::DoDestroy(node);

        auto objectManager = TBase::Bootstrap->GetObjectManager();

        auto* chunkList = node->GetChunkList();
        YCHECK(chunkList->OwningNodes().erase(node) == 1);
        objectManager->UnrefObject(chunkList);
    }

    virtual void DoBranch(
        const TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        TBase::DoBranch(originatingNode, branchedNode);

        // TODO(babenko): fixme
    }

    virtual void DoMerge(
        TJournalNode* originatingNode,
        TJournalNode* branchedNode) override
    {
        TBase::DoMerge(originatingNode, branchedNode);

        // TODO(babenko): fixme
    }

    virtual void DoClone(
        TJournalNode* /*sourceNode*/,
        TJournalNode* /*clonedNode*/,
        ICypressNodeFactoryPtr /*factory*/) override
    {
        THROW_ERROR_EXCEPTION("Journals cannot be cloned");
    }

};

INodeTypeHandlerPtr CreateJournalTypeHandler(TBootstrap* bootstrap)
{
    return New<TJournalNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT

