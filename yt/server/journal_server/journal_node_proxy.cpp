#include "stdafx.h"
#include "journal_node_proxy.h"
#include "journal_node.h"
#include "private.h"

#include <server/chunk_server/chunk_owner_node_proxy.h>
#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>

namespace NYT {
namespace NJournalServer {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TJournalNodeProxy
    : public TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TJournalNode>
{
public:
    TJournalNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        TJournalNode* trunkNode)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

private:
    typedef TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TJournalNode> TBase;

    virtual NLog::TLogger CreateLogger() const override
    {
        return JournalServerLogger;
    }

    virtual ELockMode GetLockMode(EUpdateMode updateMode) override
    {
        return ELockMode::Exclusive;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        attributes->push_back("read_quorum");
        attributes->push_back("write_quorum");
        attributes->push_back("row_count");
        attributes->push_back(TAttributeInfo("quorum_row_count", true, true));
        attributes->push_back("sealed");
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisTypedImpl();

        if (key == "read_quorum") {
            BuildYsonFluently(consumer)
                .Value(node->GetReadQuorum());
            return true;
        }

        if (key == "write_quorum") {
            BuildYsonFluently(consumer)
                .Value(node->GetWriteQuorum());
            return true;
        }

        if (key == "row_count") {
            BuildYsonFluently(consumer)
                .Value(node->GetChunkList()->Statistics().RowCount);
            return true;
        }

        if (key == "sealed") {
            BuildYsonFluently(consumer)
                .Value(node->IsSealed());
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const Stroka& key, const TYsonString& value) override
    {
        if (key == "replication_factor") {
            // Prevent changing replication factor after construction.
            ValidateNoTransaction();
            auto* node = GetThisTypedImpl();
            YCHECK(node->IsTrunk());
            if (node->GetReplicationFactor() != 0) {
                ThrowCannotSetBuiltinAttribute("replication_factor");
            } else {
                return TCypressNodeProxyBase::SetBuiltinAttribute(key, value);
            }
        }

        if (key == "read_quorum") {
            int readQuorum = NYTree::ConvertTo<int>(value);
            if (readQuorum < 1) {
                THROW_ERROR_EXCEPTION("\"read_quorum\" must be positive");
            }

            ValidateNoTransaction();
            auto* node = GetThisTypedImpl();
            YCHECK(node->IsTrunk());

            // Prevent changing read quorum after construction.
            if (node->GetReadQuorum() != 0) {
                ThrowCannotSetBuiltinAttribute("read_quorum");
            }
            node->SetReadQuorum(readQuorum);
            return true;
        }

        if (key == "write_quorum") {
            int writeQuorum = NYTree::ConvertTo<int>(value);
            if (writeQuorum < 1) {
                THROW_ERROR_EXCEPTION("\"write_quorum\" must be positive");
            }

            ValidateNoTransaction();
            auto* node = GetThisTypedImpl();
            YCHECK(node->IsTrunk());

            // Prevent changing write quorum after construction.
            if (node->GetWriteQuorum() != 0) {
                ThrowCannotSetBuiltinAttribute("write_quorum");
            }
            node->SetWriteQuorum(writeQuorum);
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    virtual TFuture<void> GetBuiltinAttributeAsync(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisTypedImpl();
        if (key == "quorum_row_count") {
            const auto* chunkList = node->GetChunkList();
            if (chunkList->Children().empty()) {
                BuildYsonFluently(consumer)
                    .Value(0);
                return VoidFuture;
            }

            auto* chunk = chunkList->Children().back()->AsChunk();
            i64 penultimateRowCount = chunkList->RowCountSums().empty() ? 0 : chunkList->RowCountSums().back();

            auto chunkManager = Bootstrap->GetChunkManager();
            return chunkManager
                ->GetChunkQuorumInfo(chunk)
                .Apply(BIND([=] (const TMiscExt& miscExt) {
                    BuildYsonFluently(consumer)
                        .Value(penultimateRowCount + miscExt.row_count());
                }));
        }

        return TBase::GetBuiltinAttributeAsync(key, consumer);
    }


    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(PrepareForUpdate);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, PrepareForUpdate)
    {
        DeclareMutating();

        auto mode = EUpdateMode(request->mode());
        if (mode != EUpdateMode::Append) {
            THROW_ERROR_EXCEPTION("Journals only support %Qlv update mode",
                EUpdateMode::Append);
        }

        ValidateTransaction();
        ValidatePermission(
            NYTree::EPermissionCheckScope::This,
            NSecurityServer::EPermission::Write);

        auto* node = GetThisTypedImpl();
        if (!node->IsSealed()) {
            THROW_ERROR_EXCEPTION("Journal is not properly sealed");
        }

        ValidatePrepareForUpdate();

        auto* lockedNode = LockThisTypedImpl();
        auto* chunkList = node->GetChunkList();

        lockedNode->SetUpdateMode(mode);

        SetModified();

        LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Node is switched to \"append\" mode (NodeId: %v, ChunkListId: %v)",
            node->GetId(),
            chunkList->GetId());

        ToProto(response->mutable_chunk_list_id(), chunkList->GetId());

        context->SetResponseInfo("ChunkListId: %v",
            chunkList->GetId());

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateJournalNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
    TTransaction* transaction,
    TJournalNode* trunkNode)
{

    return New<TJournalNodeProxy>(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT
