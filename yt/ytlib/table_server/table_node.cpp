#include "stdafx.h"
#include "table_node.h"
#include "table_node_proxy.h"
#include "common.h"

#include <ytlib/chunk_server/chunk.h>
#include <ytlib/chunk_server/chunk_list.h>
#include <ytlib/chunk_server/chunk_manager.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/load_context.h>
#include <ytlib/table_client/schema.h>

namespace NYT {
namespace NTableServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NYTree;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

TTableNode::TTableNode(const TVersionedNodeId& id)
    : TCypressNodeBase(id)
    , ChunkList_(NULL)
    , UpdateMode_(ETableUpdateMode::None)
{ }

EObjectType TTableNode::GetObjectType() const
{
    return EObjectType::Table;
}

void TTableNode::Save(TOutputStream* output) const
{
    TCypressNodeBase::Save(output);
    SaveObjectRef(output, ChunkList_);
    ::Save(output, UpdateMode_);
}

void TTableNode::Load(const TLoadContext& context, TInputStream* input)
{
    TCypressNodeBase::Load(context, input);
    LoadObjectRef(input, ChunkList_, context);
    ::Load(input, UpdateMode_);
}

////////////////////////////////////////////////////////////////////////////////

class TTableNodeTypeHandler
    : public TCypressNodeTypeHandlerBase<TTableNode>
{
public:
    typedef TCypressNodeTypeHandlerBase<TTableNode> TBase;

    explicit TTableNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
    { }

    EObjectType GetObjectType()
    {
        return EObjectType::Table;
    }

    ENodeType GetNodeType()
    {
        return ENodeType::Entity;
    }

    virtual bool IsLockModeSupported(ELockMode mode)
    {
        return
            mode == ELockMode::Exclusive ||
            mode == ELockMode::Shared ||
            mode == ELockMode::Snapshot;
    }

    virtual TAutoPtr<ICypressNode> Create(
        NTransactionServer::TTransaction* transaction,
        TReqCreate* request,
        TRspCreate* response) override
    {
        YCHECK(request);
        UNUSED(transaction);
        UNUSED(response);

        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        // Set default channels, if not given explicitly.
        auto ysonChannels = request->Attributes().FindYson("channels");
        if (!ysonChannels) {
            request->Attributes().SetYson("channels", TYsonString("[]"));
        }

        auto node = TBase::DoCreate(transaction, request, response);

        // Create an empty chunk list and reference it from the node.
        auto* chunkList = chunkManager->CreateChunkList();
        node->SetChunkList(chunkList);
        YCHECK(chunkList->OwningNodes().insert(~node).second);
        objectManager->RefObject(chunkList);

        // TODO(babenko): Release is needed due to cast to ICypressNode.
        return node.Release();
    }

    virtual ICypressNodeProxyPtr GetProxy(
        const NCypressServer::TNodeId& nodeId,
        NTransactionServer::TTransaction* transaction) override
    {
        return New<TTableNodeProxy>(
            this,
            Bootstrap,
            transaction,
            nodeId);
    }

protected:
    virtual void DoDestroy(TTableNode* node) override
    {
        auto objectManager = Bootstrap->GetObjectManager();

        auto* chunkList = node->GetChunkList();
        YCHECK(chunkList->OwningNodes().erase(node) == 1);
        objectManager->UnrefObject(chunkList);
    }

    virtual void DoBranch(const TTableNode* originatingNode, TTableNode* branchedNode) override
    {
        auto objectManager = Bootstrap->GetObjectManager();

        auto* chunkList = originatingNode->GetChunkList();

        branchedNode->SetChunkList(chunkList);
        objectManager->RefObject(branchedNode->GetChunkList());
        YCHECK(branchedNode->GetChunkList()->OwningNodes().insert(branchedNode).second);
        
        LOG_DEBUG_UNLESS(IsRecovery(), "Table node branched (BranchedNodeId: %s, ChunkListId: %s)",
            ~branchedNode->GetId().ToString(),
            ~originatingNode->GetChunkList()->GetId().ToString());
    }

    virtual void DoMerge(TTableNode* originatingNode, TTableNode* branchedNode) override
    {
        auto originatingChunkListId = originatingNode->GetChunkList()->GetId();
        auto branchedChunkListId = branchedNode->GetChunkList()->GetId();

        auto originatingUpdateMode = originatingNode->GetUpdateMode();
        auto branchedUpdateMode = branchedNode->GetUpdateMode();

        MergeChunkLists(originatingNode, branchedNode);

        LOG_DEBUG_UNLESS(IsRecovery(),
            "Table node merged (OriginatingNodeId: %s, OriginatingChunkListId: %s, OriginatingUpdateMode: %s, "
            "BranchedNodeId: %s, BranchedChunkListId: %s, BranchedUpdateMode: %s, "
            "NewOriginatingChunkListId: %s, NewOriginatingUpdateMode: %s)",
            ~originatingNode->GetId().ToString(),
            ~originatingChunkListId.ToString(),
            ~originatingUpdateMode.ToString(),
            ~branchedNode->GetId().ToString(),
            ~branchedChunkListId.ToString(),
            ~branchedUpdateMode.ToString(),
            ~originatingNode->GetChunkList()->GetId().ToString(),
            ~originatingNode->GetUpdateMode().ToString());
    }

    void MergeChunkLists(TTableNode* originatingNode, TTableNode* branchedNode)
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto objectManager = Bootstrap->GetObjectManager();

        auto* originatingChunkList = originatingNode->GetChunkList();
        auto* branchedChunkList = branchedNode->GetChunkList();

        auto originatingMode = originatingNode->GetUpdateMode();
        auto branchedMode = branchedNode->GetUpdateMode();

        YCHECK(branchedChunkList->OwningNodes().erase(branchedNode) == 1);

        if (branchedMode == ETableUpdateMode::None) {
            objectManager->UnrefObject(branchedChunkList);

            return;
        }

        if (branchedMode == ETableUpdateMode::Overwrite) {
            bool setOriginatingOverwriteMode = 
                originatingNode->GetId().IsBranched();

            YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
            YCHECK(branchedChunkList->OwningNodes().insert(originatingNode).second);
            originatingNode->SetChunkList(branchedChunkList);

            objectManager->UnrefObject(originatingChunkList);

            if (setOriginatingOverwriteMode) {
                originatingNode->SetUpdateMode(ETableUpdateMode::Overwrite);
            }

            return;
        }

        if ((originatingMode == ETableUpdateMode::None || originatingMode == ETableUpdateMode::Overwrite) &&
            branchedMode == ETableUpdateMode::Append)
        {
            YCHECK(branchedChunkList->Children().size() == 2);

            bool setOriginatingAppendMode =
                originatingMode == ETableUpdateMode::None &&
                originatingNode->GetId().IsBranched();

            auto* newOriginatingChunkList = chunkManager->CreateChunkList();
            newOriginatingChunkList->SetRigid(setOriginatingAppendMode);
            newOriginatingChunkList->SortedBy() = branchedChunkList->SortedBy();

            YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
            YCHECK(newOriginatingChunkList->OwningNodes().insert(originatingNode).second);
            originatingNode->SetChunkList(newOriginatingChunkList);
            objectManager->RefObject(newOriginatingChunkList);
         
            chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList);
            chunkManager->AttachToChunkList(newOriginatingChunkList, branchedChunkList->Children()[1]);

            objectManager->UnrefObject(originatingChunkList);
            objectManager->UnrefObject(branchedChunkList);


            if (setOriginatingAppendMode) {
                originatingNode->SetUpdateMode(ETableUpdateMode::Append);
            }

            return;
        }

        if (originatingMode == ETableUpdateMode::Append &&
            branchedMode == ETableUpdateMode::Append)
        {
            YCHECK(originatingChunkList->Children().size() == 2);
            YCHECK(branchedChunkList->Children().size() == 2);

            auto* newOriginatingChunkList = chunkManager->CreateChunkList();
            newOriginatingChunkList->SetRigid(true);
            newOriginatingChunkList->SortedBy() = branchedChunkList->SortedBy();

            YCHECK(originatingChunkList->OwningNodes().erase(originatingNode) == 1);
            YCHECK(newOriginatingChunkList->OwningNodes().insert(originatingNode).second);
            originatingNode->SetChunkList(newOriginatingChunkList);
            objectManager->RefObject(newOriginatingChunkList);

            chunkManager->AttachToChunkList(newOriginatingChunkList, originatingChunkList->Children()[0]);

            auto* newDeltaChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newOriginatingChunkList, newDeltaChunkList);

            chunkManager->AttachToChunkList(newDeltaChunkList, originatingChunkList->Children()[1]);
            chunkManager->AttachToChunkList(newDeltaChunkList, branchedChunkList->Children()[1]);

            objectManager->UnrefObject(originatingChunkList);
            objectManager->UnrefObject(branchedChunkList);

            // originatingMode remains Append.
            return;
        }

        YUNREACHABLE();
    }
};

INodeTypeHandlerPtr CreateTableTypeHandler(TBootstrap* bootstrap)
{
    return New<TTableNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

