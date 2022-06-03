#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/server/lib/tablet_balancer/public.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TTableMutableInfo
    : public TRefCounted
{
    struct TTabletInfo
    {
        struct TTabletStatistics
        {
            i64 CompressedDataSize;
            i64 UncompressedDataSize;
        };

        i64 TabletIndex;
        TTabletId TabletId;
        TTabletStatistics Statistics;
    };

    TTableTabletBalancerConfigPtr TableConfig;
    std::vector<TTabletInfo> Tablets;
    bool Dynamic = false;

    TTableMutableInfo(
        TTableTabletBalancerConfigPtr config,
        bool isDynamic);
};

DEFINE_REFCOUNTED_TYPE(TTableMutableInfo)

void Deserialize(TTableMutableInfo::TTabletInfo& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

class TBundleState
    : public TRefCounted
{
public:
    struct TImmutableTableInfo
    {
        const bool Sorted;
        const NYPath::TYPath Path;
        const TCellTag ExternalCellTag;
    };

    struct TImmutableTabletInfo
    {
        const TTableId tableId;
    };

private:
    struct TCellTagBatch
    {
        NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr Request;
        TFuture<NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr> Response;
    };

public:
    DEFINE_BYVAL_RO_PROPERTY(TString, Name);
    DEFINE_BYVAL_RO_PROPERTY(NTabletClient::ETabletCellHealth, Health);
    DEFINE_BYVAL_RO_PROPERTY(std::vector<TTabletCellId>, CellIds);
    DEFINE_BYVAL_RO_PROPERTY(TBundleTabletBalancerConfigPtr, Config);

private:
    const NApi::NNative::IClientPtr Client_; 
    const IInvokerPtr Invoker_;

    THashMap<TTableId, TTableMutableInfoPtr> TableMutableInfo_;

    THashMap<TTabletId, TImmutableTabletInfo> TabletImmutableInfo_;
    THashMap<TTableId, TImmutableTableInfo> TableImmutableInfo_;

public:
    TBundleState(
        TString name,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker);

    void UpdateBundleAttributes(const NYTree::IAttributeDictionary* attributes);

    bool IsBalancingAllowed() const;

    TTableMutableInfoPtr GetTableMutableInfo(const TTableId& tableId) const;
    const THashMap<TTableId, TImmutableTableInfo>& GetTableImmutableInfo() const;
    const THashMap<TTabletId, TImmutableTabletInfo>& GetTabletImmutableInfo() const;

    TFuture<void> UpdateMetaRegistry();
    TFuture<void> FetchTableMutableInfo();

private:
    void ExecuteBatchRequests(THashMap<TCellTag, TCellTagBatch>* batchReqs) const;

    THashMap<NCypressClient::TObjectId, NYTree::IAttributeDictionaryPtr> FetchAttributes(
        const THashSet<NCypressClient::TObjectId>& objectIds,
        const std::vector<TString>& attributeKeys,
        bool useExternalCellTagForTables = false) const;

    void DoUpdateMetaRegistry();

    THashSet<TTabletId> FetchTabletIds() const;
    THashMap<TTabletId, TImmutableTabletInfo> FetchTabletInfos(
        const THashSet<TTabletId>& tabletIds) const;
    THashMap<TTableId, TImmutableTableInfo> FetchTableInfos(
        const THashSet<TTableId>& tableIds) const;

    void DoFetchTableMutableInfo();

    THashMap<TTableId, TTableMutableInfoPtr> FetchActualTableConfigs() const;
    THashMap<TTableId, std::vector<TTableMutableInfo::TTabletInfo>> FetchTablets(
        const THashSet<TTableId>& tableIds) const;

    bool IsTableBalancingAllowed(const TTableMutableInfoPtr& tableInfo) const;
};

DEFINE_REFCOUNTED_TYPE(TBundleState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
