#include "journal_node_proxy.h"
#include "private.h"
#include "journal_node.h"
#include "journal_manager.h"

#include <yt/server/master/chunk_server/chunk.h>
#include <yt/server/master/chunk_server/chunk_list.h>
#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/chunk_owner_node_proxy.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/ytlib/journal_client/journal_ypath.pb.h>

namespace NYT::NJournalServer {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NObjectServer;
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
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TJournalNode* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

private:
    typedef TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TJournalNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* node = GetThisImpl();
        auto isExternal = node->IsExternal();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReadQuorum)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::WriteQuorum)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QuorumRowCount)
            .SetExternal(isExternal)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::Sealed);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        auto* node = GetThisImpl();
        auto statistics = node->ComputeTotalStatistics();

        switch (key) {
            case EInternedAttributeKey::ReadQuorum:
                BuildYsonFluently(consumer)
                    .Value(node->GetReadQuorum());
                return true;

            case EInternedAttributeKey::WriteQuorum:
                BuildYsonFluently(consumer)
                    .Value(node->GetWriteQuorum());
                return true;

            case EInternedAttributeKey::Sealed:
                BuildYsonFluently(consumer)
                    .Value(node->GetSealed());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        const auto* node = GetThisImpl();
        auto isExternal = node->IsExternal();

        switch (key) {
            case EInternedAttributeKey::QuorumRowCount: {
                if (isExternal) {
                    break;
                }
                const auto* chunkList = node->GetChunkList();
                if (chunkList->Children().empty()) {
                    return MakeFuture(ConvertToYsonString(0));
                }

                auto* chunk = chunkList->Children().back()->AsChunk();
                const auto& cumulativeStatistics = chunkList->CumulativeStatistics();
                i64 penultimateRowCount = cumulativeStatistics.Size() == 1
                    ? 0
                    : cumulativeStatistics[cumulativeStatistics.Size() - 2].RowCount;

                const auto& chunkManager = Bootstrap_->GetChunkManager();
                return chunkManager
                    ->GetChunkQuorumInfo(chunk)
                    .Apply(BIND([=] (const NChunkClient::NProto::TMiscExt& miscExt) {
                        return ConvertToYsonString(penultimateRowCount + miscExt.row_count());
                    }));
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    virtual void ValidateBeginUpload() override
    {
        TBase::ValidateBeginUpload();

        const auto* journal = GetThisImpl();
        if (!journal->GetSealed()) {
            THROW_ERROR_EXCEPTION("Journal is not sealed");
        }
    }

    virtual void ValidateStorageParametersUpdate() override
    {
        TBase::ValidateStorageParametersUpdate();
        THROW_ERROR_EXCEPTION("Changing storage settings for journal nodes is forbidden");
    }

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(UpdateStatistics);
        DISPATCH_YPATH_SERVICE_METHOD(Seal);
        DISPATCH_YPATH_SERVICE_METHOD(Truncate);
        return TBase::DoInvoke(context);
    }

    virtual void ValidateFetch(TFetchContext* context) override
    {
        for (const auto& range : context->Ranges) {
            const auto& lowerLimit = range.LowerLimit();
            const auto& upperLimit = range.UpperLimit();
            if (upperLimit.HasKey() || lowerLimit.HasKey()) {
                THROW_ERROR_EXCEPTION("Key selectors are not supported for journals");
            }
            if (upperLimit.HasOffset() || lowerLimit.HasOffset()) {
                THROW_ERROR_EXCEPTION("Offset selectors are not supported for journals");
            }
        }
    }

    DECLARE_YPATH_SERVICE_METHOD(NJournalClient::NProto, UpdateStatistics)
    {
        Y_UNUSED(response);

        DeclareMutating();

        context->SetRequestInfo("Statistics: %v", request->statistics());

        auto* journal = GetThisImpl();
        YT_VERIFY(journal->IsTrunk());

        const auto& journalManager = Bootstrap_->GetJournalManager();
        journalManager->UpdateStatistics(journal->GetTrunkNode(), &request->statistics());

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NJournalClient::NProto, Seal)
    {
        Y_UNUSED(response);

        DeclareMutating();

        context->SetRequestInfo();

        auto* journal = GetThisImpl();
        YT_VERIFY(journal->IsTrunk());

        const auto& journalManager = Bootstrap_->GetJournalManager();
        journalManager->SealJournal(journal->GetTrunkNode(), &request->statistics());

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NJournalClient::NProto, Truncate)
    {
        Y_UNUSED(response);

        DeclareMutating();

        ValidateNoTransaction();

        context->SetRequestInfo("RowCount: %v", request->row_count());

        auto* journal = LockThisImpl();
        YT_VERIFY(journal->IsTrunk());

        const auto& journalManager = Bootstrap_->GetJournalManager();
        journalManager->TruncateJournal(journal->GetTrunkNode(), request->row_count());

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateJournalNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TJournalNode* trunkNode)
{
    return New<TJournalNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer
