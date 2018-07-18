#pragma once

#include "public.h"
#include "chunk_owner_base.h"

#include <yt/server/cypress_server/node_proxy_detail.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/client/chunk_client/read_limit.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkOwnerNodeProxy
    : public NCypressServer::TNontemplateCypressNodeProxyBase
{
public:
    TChunkOwnerNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TChunkOwnerBase* trunkNode);

    virtual NYTree::ENodeType GetType() const override;

protected:
    virtual void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    virtual void ValidateFetchParameters(const std::vector<NChunkClient::TReadRange>& ranges);

    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    void ValidateInUpdate();
    virtual void ValidateBeginUpload();
    virtual void ValidateFetch();
    virtual void ValidateStorageParametersUpdate() override;

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Fetch);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, BeginUpload);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, GetUploadParams);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, EndUpload);

private:
    void SetReplicationFactor(int replicationFactor);
    void SetVital(bool vital);
    void SetReplication(const TChunkReplication& replication);
    void SetPrimaryMedium(TMedium* medium);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
