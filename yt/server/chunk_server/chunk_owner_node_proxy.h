#pragma once

#include "public.h"
#include "private.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_tree_traversing.h"
#include "node_directory_builder.h"

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <ytlib/chunk_client/schema.h>

#include <ytlib/ytree/node.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/system_attribute_provider.h>
#include <ytlib/ytree/attribute_helpers.h>

#include <server/cypress_server/node_proxy_detail.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TFetchChunkVisitor
    : public IChunkVisitor
{
public:
    typedef NYT::NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqFetch,
        NChunkClient::NProto::TRspFetch> TCtxFetch;

    typedef TIntrusivePtr<TCtxFetch> TCtxFetchPtr;

    TFetchChunkVisitor(
        NCellMaster::TBootstrap* bootstrap,
        TChunkList* chunkList,
        TCtxFetchPtr context,
        const NChunkClient::TChannel& channel);

    void StartSession(
        const NChunkClient::NProto::TReadLimit& lowerBound,
        const NChunkClient::NProto::TReadLimit& upperBound);

    void Complete();

private:
    NCellMaster::TBootstrap* Bootstrap;
    TChunkList* ChunkList;
    TCtxFetchPtr Context;
    NChunkClient::TChannel Channel;

    yhash_set<int> ExtensionTags;
    TNodeDirectoryBuilder NodeDirectoryBuilder;
    int SessionCount;
    bool Completed;
    bool Finished;

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

    void Reply();
    void ReplyError(const TError& error);

    virtual bool OnChunk(
        TChunk* chunk,
        const NChunkClient::NProto::TReadLimit& startLimit,
        const NChunkClient::NProto::TReadLimit& endLimit) override;

    virtual void OnError(const TError& error) override;
    virtual void OnFinish() override;

    static bool IsNontrivial(const NChunkClient::NProto::TReadLimit& limit);

};

typedef TIntrusivePtr<TFetchChunkVisitor> TFetchChunkVisitorPtr;

TAsyncError GetChunkIdsAttribute(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList,
    NYson::IYsonConsumer* consumer);

TAsyncError GetCodecStatisticsAttribute(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

template<class TChunkOwner>
class TChunkOwnerNodeProxy
    : public NCypressServer::TCypressNodeProxyBase<
        NCypressServer::TNontemplateCypressNodeProxyBase,
        NYTree::IEntityNode,
        TChunkOwner>
{
public:
    TChunkOwnerNodeProxy(
        NCypressServer::INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TChunkOwner* trunkNode);

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override;

    virtual NSecurityServer::TClusterResources GetResourceUsage() const override;

protected:
    typedef NCypressServer::TCypressNodeProxyBase<NCypressServer::TNontemplateCypressNodeProxyBase, NYTree::IEntityNode, TChunkOwner> TBase;

    virtual void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeInfo>* attributes) override;
    virtual bool GetSystemAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual TAsyncError GetSystemAttributeAsync(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual void ValidateUserAttributeUpdate(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue) override;
    virtual void ValidatePathAttributes(
        const TNullable<NChunkClient::TChannel>& channel,
        const NChunkClient::NProto::TReadLimit& upperLimit,
        const NChunkClient::NProto::TReadLimit& lowerLimit);
    virtual bool SetSystemAttribute(const Stroka& key, const NYTree::TYsonString& value) override;

    virtual NCypressClient::ELockMode GetLockMode(NChunkClient::EUpdateMode updateMode) = 0;

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PrepareForUpdate);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, Fetch);

private:
    NLog::TLogger& Logger;
};

////////////////////////////////////////////////////////////////////////////////

template <class TChunkOwner>
TChunkOwnerNodeProxy<TChunkOwner>::TChunkOwnerNodeProxy(
    NCypressServer::INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
    TChunkOwner* trunkNode)
    : TBase(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode)
    , Logger(ChunkServerLogger)
{ }

template <class TChunkOwner>
bool TChunkOwnerNodeProxy<TChunkOwner>::DoInvoke(NRpc::IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(PrepareForUpdate);
    DISPATCH_YPATH_HEAVY_SERVICE_METHOD(Fetch);
    return TBase::DoInvoke(context);
}

template <class TChunkOwner>
bool TChunkOwnerNodeProxy<TChunkOwner>::IsWriteRequest(NRpc::IServiceContextPtr context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(PrepareForUpdate);
    return TBase::IsWriteRequest(context);
}

template <class TChunkOwner>
NSecurityServer::TClusterResources TChunkOwnerNodeProxy<TChunkOwner>::GetResourceUsage() const
{
    const auto* node = TBase::GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();
    i64 diskSpace = chunkList->Statistics().DiskSpace * node->GetReplicationFactor();
    return NSecurityServer::TClusterResources(diskSpace, 1);
}

template <class TChunkOwner>
void TChunkOwnerNodeProxy<TChunkOwner>::ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeInfo>* attributes)
{
    attributes->push_back("chunk_list_id");
    attributes->push_back(NYTree::ISystemAttributeProvider::TAttributeInfo("chunk_ids", true, true));
    attributes->push_back(NYTree::ISystemAttributeProvider::TAttributeInfo("compression_statistics", true, true));
    attributes->push_back("chunk_count");
    attributes->push_back("uncompressed_data_size");
    attributes->push_back("compressed_data_size");
    attributes->push_back("compression_ratio");
    attributes->push_back("update_mode");
    attributes->push_back("replication_factor");
    TBase::ListSystemAttributes(attributes);
}

template <class TChunkOwner>
bool TChunkOwnerNodeProxy<TChunkOwner>::GetSystemAttribute(
    const Stroka& key,
    NYson::IYsonConsumer* consumer)
{
    const auto* node = TBase::GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();
    const auto& statistics = chunkList->Statistics();

    if (key == "chunk_list_id") {
        NYTree::BuildYsonFluently(consumer)
            .Value(ToString(chunkList->GetId()));
        return true;
    }

    if (key == "chunk_count") {
        NYTree::BuildYsonFluently(consumer)
            .Value(statistics.ChunkCount);
        return true;
    }

    if (key == "uncompressed_data_size") {
        NYTree::BuildYsonFluently(consumer)
            .Value(statistics.UncompressedDataSize);
        return true;
    }

    if (key == "compressed_data_size") {
        NYTree::BuildYsonFluently(consumer)
            .Value(statistics.CompressedDataSize);
        return true;
    }

    if (key == "compression_ratio") {
        double ratio =
            statistics.UncompressedDataSize > 0
            ? static_cast<double>(statistics.CompressedDataSize) / statistics.UncompressedDataSize
            : 0;
        NYTree::BuildYsonFluently(consumer)
            .Value(ratio);
        return true;
    }

    if (key == "update_mode") {
        NYTree::BuildYsonFluently(consumer)
            .Value(FormatEnum(node->GetUpdateMode()));
        return true;
    }

    if (key == "replication_factor") {
        NYTree::BuildYsonFluently(consumer)
            .Value(node->GetReplicationFactor());
        return true;
    }

    return TBase::GetSystemAttribute(key, consumer);
}

template <class TChunkOwner>
TAsyncError TChunkOwnerNodeProxy<TChunkOwner>::GetSystemAttributeAsync(
    const Stroka& key,
    NYson::IYsonConsumer* consumer)
{
    const auto* node = TBase::GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();

    if (key == "chunk_ids") {
        return GetChunkIdsAttribute(
            TBase::Bootstrap,
            const_cast<TChunkList*>(chunkList),
            consumer);
    }

    if (key == "compression_statistics") {
        return GetCodecStatisticsAttribute(
            TBase::Bootstrap,
            const_cast<TChunkList*>(chunkList),
            consumer);
    }

    return TBase::GetSystemAttributeAsync(key, consumer);
}

template <class TChunkOwner>
void TChunkOwnerNodeProxy<TChunkOwner>::ValidateUserAttributeUpdate(
    const Stroka& key,
    const TNullable<NYTree::TYsonString>& oldValue,
    const TNullable<NYTree::TYsonString>& newValue)
{
    UNUSED(oldValue);

    if (key == "compression_codec") {
        if (!newValue) {
            NYTree::ThrowCannotRemoveAttribute(key);
        }
        ParseEnum<NCompression::ECodec>(NYTree::ConvertTo<Stroka>(newValue.Get()));
        return;
    }
}

template <class TChunkOwner>
bool TChunkOwnerNodeProxy<TChunkOwner>::SetSystemAttribute(
    const Stroka& key,
    const NYTree::TYsonString& value)
{
    auto chunkManager = TBase::Bootstrap->GetChunkManager();

    if (key == "replication_factor") {
        TBase::ValidateNoTransaction();
        int replicationFactor = NYTree::ConvertTo<int>(value);
        const int MinReplicationFactor = 1;
        const int MaxReplicationFactor = 10;
        if (replicationFactor < MinReplicationFactor || replicationFactor > MaxReplicationFactor) {
            THROW_ERROR_EXCEPTION("Value must be in range [%d,%d]",
                MinReplicationFactor,
                MaxReplicationFactor);
        }

        auto* node = TBase::GetThisTypedImpl();
        YCHECK(node->IsTrunk());

        if (node->GetReplicationFactor() != replicationFactor) {
            node->SetReplicationFactor(replicationFactor);

            auto securityManager = TBase::Bootstrap->GetSecurityManager();
            securityManager->UpdateAccountNodeUsage(node);

            if (TBase::IsLeader()) {
                chunkManager->ScheduleRFUpdate(node->GetChunkList());
            }
        }

        return true;
    }

    return TBase::SetSystemAttribute(key, value);
}

template <class TChunkOwner>
void TChunkOwnerNodeProxy<TChunkOwner>::ValidatePathAttributes(
    const TNullable<NChunkClient::TChannel>& channel,
    const NChunkClient::NProto::TReadLimit& upperLimit,
    const NChunkClient::NProto::TReadLimit& lowerLimit)
{
    UNUSED(channel);
    UNUSED(upperLimit);
    UNUSED(lowerLimit);
}

template <class TChunkOwner>
DEFINE_RPC_SERVICE_METHOD(TChunkOwnerNodeProxy<TChunkOwner>, PrepareForUpdate)
{
    auto mode = NChunkClient::EUpdateMode(request->mode());
    YCHECK(mode == NChunkClient::EUpdateMode::Append || mode == NChunkClient::EUpdateMode::Overwrite);

    context->SetRequestInfo("Mode: %s", ~FormatEnum(mode));

    TBase::ValidateTransaction();
    TBase::ValidatePermission(
        NYTree::EPermissionCheckScope::This,
        NSecurityServer::EPermission::Write);

    auto* node = TBase::LockThisTypedImpl(GetLockMode(mode));

    if (node->GetUpdateMode() != NChunkClient::EUpdateMode::None) {
        THROW_ERROR_EXCEPTION("Node is already in %s mode",
            ~FormatEnum(node->GetUpdateMode()).Quote());
    }

    auto chunkManager = TBase::Bootstrap->GetChunkManager();
    auto objectManager = TBase::Bootstrap->GetObjectManager();

    TChunkList* resultChunkList;
    switch (mode) {
        case NChunkClient::EUpdateMode::Append: {
            auto* snapshotChunkList = node->GetChunkList();

            auto* newChunkList = chunkManager->CreateChunkList();
            YCHECK(newChunkList->OwningNodes().insert(node).second);

            YCHECK(snapshotChunkList->OwningNodes().erase(node) == 1);
            node->SetChunkList(newChunkList);
            objectManager->RefObject(newChunkList);

            newChunkList->SortedBy() = snapshotChunkList->SortedBy();
            chunkManager->AttachToChunkList(newChunkList, snapshotChunkList);

            auto* deltaChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newChunkList, deltaChunkList);

            objectManager->UnrefObject(snapshotChunkList);

            resultChunkList = deltaChunkList;

            LOG_DEBUG_UNLESS(
                TBase::IsRecovery(),
                "Node is switched to \"append\" mode (NodeId: %s, NewChunkListId: %s, SnapshotChunkListId: %s, DeltaChunkListId: %s)",
                ~ToString(node->GetId()),
                ~ToString(newChunkList->GetId()),
                ~ToString(snapshotChunkList->GetId()),
                ~ToString(deltaChunkList->GetId()));

            break;
        }

        case NChunkClient::EUpdateMode::Overwrite: {
            auto* oldChunkList = node->GetChunkList();
            YCHECK(oldChunkList->OwningNodes().erase(node) == 1);
            objectManager->UnrefObject(oldChunkList);

            auto* newChunkList = chunkManager->CreateChunkList();
            YCHECK(newChunkList->OwningNodes().insert(node).second);
            node->SetChunkList(newChunkList);
            objectManager->RefObject(newChunkList);

            resultChunkList = newChunkList;

            LOG_DEBUG_UNLESS(
                TBase::IsRecovery(),
                "Node is switched to \"overwrite\" mode (NodeId: %s, NewChunkListId: %s)",
                ~ToString(node->GetId()),
                ~ToString(newChunkList->GetId()));
            break;
        }

        default:
            YUNREACHABLE();
    }

    node->SetUpdateMode(mode);

    TBase::SetModified();

    ToProto(response->mutable_chunk_list_id(), resultChunkList->GetId());
    context->SetResponseInfo("ChunkListId: %s", ~ToString(resultChunkList->GetId()));

    context->Reply();
}

template <class TChunkOwner>
DEFINE_RPC_SERVICE_METHOD(TChunkOwnerNodeProxy<TChunkOwner>, Fetch)
{
    context->SetRequestInfo("");

    TBase::ValidatePermission(
        NYTree::EPermissionCheckScope::This,
        NSecurityServer::EPermission::Read);

    const auto* node = TBase::GetThisTypedImpl();

    auto attributes = NYTree::FromProto(request->attributes());
    auto channelAttribute = attributes->Find<NChunkClient::TChannel>("channel");
    auto lowerLimit = attributes->Get("lower_limit", NChunkClient::NProto::TReadLimit());
    auto upperLimit = attributes->Get("upper_limit", NChunkClient::NProto::TReadLimit());
    bool complement = attributes->Get("complement", false);

    ValidatePathAttributes(channelAttribute, lowerLimit, upperLimit);

    auto channel = channelAttribute.Get(NChunkClient::TChannel::Universal());

    auto* chunkList = node->GetChunkList();

    auto visitor = New<TFetchChunkVisitor>(
        TBase::Bootstrap,
        chunkList,
        context,
        channel);

    if (complement) {
        if (lowerLimit.has_row_index() || lowerLimit.has_key()) {
            visitor->StartSession(NChunkClient::NProto::TReadLimit(), lowerLimit);
        }
        if (upperLimit.has_row_index() || upperLimit.has_key()) {
            visitor->StartSession(upperLimit, NChunkClient::NProto::TReadLimit());
        }
    } else {
        visitor->StartSession(lowerLimit, upperLimit);
    }

    visitor->Complete();
}

////////////////////////////////////////////////////////////////////////////////

} // NChunkServer
} // NYT