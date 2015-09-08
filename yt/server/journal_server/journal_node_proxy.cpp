#include "stdafx.h"
#include "journal_node_proxy.h"
#include "journal_node.h"
#include "journal_manager.h"
#include "private.h"

#include <server/chunk_server/chunk_owner_node_proxy.h>
#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>

#include <ytlib/journal_client/journal_ypath_proxy.h>

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

    virtual NLogging::TLogger CreateLogger() const override
    {
        return JournalServerLogger;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* node = GetThisImpl();
        auto isExternal = node->IsExternal();

        descriptors->push_back(TAttributeDescriptor("read_quorum")
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("write_quorum")
            .SetReplicated(true));
        descriptors->push_back("row_count");
        descriptors->push_back(TAttributeDescriptor("quorum_row_count")
            .SetExternal(isExternal)
            .SetOpaque(true));
        descriptors->push_back("sealed");
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        auto* node = GetThisTypedImpl();
        auto statistics = node->ComputeTotalStatistics();

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
                .Value(statistics.row_count());
            return true;
        }

        if (key == "sealed") {
            BuildYsonFluently(consumer)
                .Value(node->GetSealed());
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(const Stroka& key) override
    {
        const auto* node = GetThisTypedImpl();
        auto isExternal = node->IsExternal();

        if (key == "quorum_row_count" && !isExternal) {
            const auto* chunkList = node->GetChunkList();
            if (chunkList->Children().empty()) {
                return MakeFuture(ConvertToYsonString(0));
            }

            auto* chunk = chunkList->Children().back()->AsChunk();
            i64 penultimateRowCount = chunkList->RowCountSums().empty() ? 0 : chunkList->RowCountSums().back();

            auto chunkManager = Bootstrap_->GetChunkManager();
            return chunkManager
                ->GetChunkQuorumInfo(chunk)
                .Apply(BIND([=] (const TMiscExt& miscExt) {
                    return ConvertToYsonString(penultimateRowCount + miscExt.row_count());
                }));
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    virtual void ValidateBeginUpload() override
    {
        TBase::ValidateBeginUpload();

        const auto* journal = GetThisTypedImpl();
        if (!journal->GetSealed()) {
            THROW_ERROR_EXCEPTION("Journal is not sealed");
        }
    }


    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Seal);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NJournalClient::NProto, Seal)
    {
        UNUSED(response);

        DeclareMutating();

        context->SetRequestInfo();

        auto* journal = GetThisTypedImpl();
        YCHECK(journal->IsTrunk());

        auto journalManager = Bootstrap_->GetJournalManager();
        journalManager->SealJournal(journal->GetTrunkNode(), &request->statistics());

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
