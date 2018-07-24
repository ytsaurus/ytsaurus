#pragma once

#include "table_node.h"

#include <yt/server/object_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/server/cypress_server/node_proxy_detail.h>

#include <yt/server/chunk_server/chunk_owner_node_proxy.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NTableServer {

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
    typedef TCypressNodeProxyBase<TChunkOwnerNodeProxy, NYTree::IEntityNode, TTableNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    virtual bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
    virtual void ValidateCustomAttributeUpdate(
        const TString& key,
        const NYson::TYsonString& oldValue,
        const NYson::TYsonString& newValue) override;
    virtual void ValidateFetchParameters(const std::vector<NChunkClient::TReadRange>& ranges) override;

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual void ValidateBeginUpload() override;
    virtual void ValidateStorageParametersUpdate() override;

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Mount);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Unmount);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Freeze);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Unfreeze);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Remount);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Reshard);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, GetMountInfo);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Alter);
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

} // namespace NTableServer
} // namespace NYT


