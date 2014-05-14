#pragma once

#include "public.h"
#include "chunk_owner_base.h"

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <ytlib/chunk_client/schema.h>

#include <server/cypress_server/node_proxy_detail.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkOwnerNodeProxy
    : public NCypressServer::TNontemplateCypressNodeProxyBase
{
public:
    TChunkOwnerNodeProxy(
        NCypressServer::INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TChunkOwnerBase* trunkNode);

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override;

    virtual NSecurityServer::TClusterResources GetResourceUsage() const override;

protected:
    virtual void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeInfo>* attributes) override;
    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual TAsyncError GetBuiltinAttributeAsync(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual void ValidateCustomAttributeUpdate(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue) override;
    virtual void ValidatePathAttributes(
        const TNullable<NChunkClient::TChannel>& channel,
        const NChunkClient::NProto::TReadLimit& upperLimit,
        const NChunkClient::NProto::TReadLimit& lowerLimit);
    virtual bool SetBuiltinAttribute(const Stroka& key, const NYTree::TYsonString& value) override;

    virtual NCypressClient::ELockMode GetLockMode(NChunkClient::EUpdateMode updateMode) = 0;

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PrepareForUpdate);
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, Fetch);

};

////////////////////////////////////////////////////////////////////////////////

} // NChunkServer
} // NYT
