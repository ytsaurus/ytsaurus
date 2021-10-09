#pragma once

#include "table_node.h"

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/yt/server/master/chunk_server/chunk_owner_node_proxy.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy
    : public NCypressServer::TCypressNodeProxyBase<NChunkServer::TChunkOwnerNodeProxy, NYTree::IEntityNode, TTableNode>
{
public:
    TTableNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TTableNode* trunkNode);

protected:
    using TBase = TCypressNodeProxyBase<TChunkOwnerNodeProxy, NYTree::IEntityNode, TTableNode>;

    void GetBasicAttributes(TGetBasicAttributesContext* context) override;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
    void ValidateCustomAttributeUpdate(
        const TString& key,
        const NYson::TYsonString& oldValue,
        const NYson::TYsonString& newValue) override;
    void ValidateReadLimit(const NChunkClient::NProto::TReadLimit& context) const override;
    NTableClient::TComparator GetComparator() const override;

    bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    void ValidateBeginUpload() override;
    void ValidateStorageParametersUpdate() override;
    void ValidateLockPossible() override;

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, ReshardAutomatic);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, GetMountInfo);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Alter);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, LockDynamicTable);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, CheckDynamicTableLock);
};

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableNodeProxy
    : public TTableNodeProxy
{
public:
    TReplicatedTableNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TReplicatedTableNode* trunkNode);

protected:
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer


