#pragma once

#include "public.h"
#include "chunk_owner_base.h"

#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/yt/client/chunk_client/read_limit.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void BuildChunkSpec(
    TChunk* chunk,
    const TChunkLocationPtrWithReplicaInfoList& chunkReplicas,
    std::optional<i64> rowIndex,
    std::optional<int> tabletIndex,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit,
    const TChunkViewModifier* modifier,
    bool fetchParityReplicas,
    bool fetchAllMetaExtensions,
    const THashSet<int>& extensionTags,
    NNodeTrackerServer::TNodeDirectoryBuilder* nodeDirectoryBuilder,
    NCellMaster::TBootstrap* bootstrap,
    NChunkClient::NProto::TChunkSpec* chunkSpec);

void BuildDynamicStoreSpec(
    const TDynamicStore* dynamicStore,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit,
    NNodeTrackerServer::TNodeDirectoryBuilder* nodeDirectoryBuilder,
    NCellMaster::TBootstrap* bootstrap,
    NChunkClient::NProto::TChunkSpec* chunkSpec);

////////////////////////////////////////////////////////////////////////////////

class TChunkOwnerNodeProxy
    : public NCypressServer::TNontemplateCypressNodeProxyBase
{
public:
    using TNontemplateCypressNodeProxyBase::TNontemplateCypressNodeProxyBase;

    NYTree::ENodeType GetType() const override;

protected:
    void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;

    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;

    struct TFetchContext
    {
        NNodeTrackerClient::EAddressType AddressType = NNodeTrackerClient::EAddressType::InternalRpc;
        bool FetchParityReplicas = false;
        bool OmitDynamicStores = false;
        bool ThrowOnChunkViews = false;
        ui64 SupportedChunkFeatures = 0;
        std::vector<NChunkClient::TReadRange> Ranges;
    };

    bool DoInvoke(const NYTree::IYPathServiceContextPtr& context) override;

    //! This method validates if requested read limit is allowed for this type of node
    //! (i.e. key limits requires node to be a sorted table).
    virtual void ValidateReadLimit(const NChunkClient::NProto::TReadLimit& readLimit) const;

    //! If this node is a sorted table, return comparator corresponding to sort order.
    virtual NTableClient::TComparator GetComparator() const;

    void ValidateInUpdate();
    virtual void ValidateBeginUpload();
    void ValidateStorageParametersUpdate() override;

    void GetBasicAttributes(TGetBasicAttributesContext* context) override;

    void ReplicateBeginUploadRequestToExternalCell(
        TChunkOwnerBase* node,
        TTransactionId uploadTransactionId,
        NChunkClient::NProto::TReqBeginUpload* request,
        TChunkOwnerBase::TBeginUploadContext& uploadContext) const;

    void ReplicateEndUploadRequestToExternalCell(
        TChunkOwnerBase* node,
        NChunkClient::NProto::TReqEndUpload* request,
        TChunkOwnerBase::TEndUploadContext& uploadContext) const;

    NTableServer::TMasterTableSchema* CalculateEffectiveMasterTableSchema(
        TChunkOwnerBase* node,
        NTableClient::TTableSchemaPtr schema,
        NTableClient::TMasterTableSchemaId schemaId,
        NTransactionServer::TTransaction* schemaHolder);

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Fetch);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, BeginUpload);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, GetUploadParams);
    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, EndUpload);

private:
    using TBase = NCypressServer::TNontemplateCypressNodeProxyBase;

    class TFetchChunkVisitor;

    void OnStorageParametersUpdated();

    void SetReplicationFactor(int replicationFactor);
    void SetVital(bool vital);
    TString DoSetReplication(TChunkReplication* replicationStorage, const TChunkReplication& replication, int mediumIndex);
    void SetReplication(const TChunkReplication& replication);
    void SetHunkReplication(const TChunkReplication& replication);
    void SetPrimaryMedium(TMedium* medium);
    void SetHunkPrimaryMedium(TMedium* medium);
    void MaybeScheduleChunkMerge();

    void ScheduleRequisitionUpdate();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
