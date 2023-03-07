#pragma once

#include "table_node.h"

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/server/master/chunk_server/chunk_owner_node_proxy.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/client/api/public.h>

#include <yt/core/yson/string.h>

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

    virtual void GetBasicAttributes(TGetBasicAttributesContext* context) override;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    virtual bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
    virtual void ValidateCustomAttributeUpdate(
        const TString& key,
        const NYson::TYsonString& oldValue,
        const NYson::TYsonString& newValue) override;
    virtual void ValidateFetch(TFetchContext* context) override;

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual void ValidateBeginUpload() override;
    virtual void ValidateStorageParametersUpdate() override;
    virtual void ValidateLockPossible() override;

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
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer


