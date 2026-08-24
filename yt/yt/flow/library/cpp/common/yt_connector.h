#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TPipelineAttributes
{
    std::string LeaderControllerAddress;
    i64 PipelineFormatVersion;
    std::string MonitoringProject;
    std::string MonitoringCluster;
};

struct TFlowTablesBundleInfo
{
    std::string Bundle;
    std::optional<NObjectClient::TCellTag> ClockClusterTag;
};

struct ICommonYTConnector
    : public TRefCounted
{
    virtual TFuture<TPipelineAttributes> GetPipelineAttributes() = 0;
    virtual TFuture<TFlowTablesBundleInfo> GetFlowTablesBundle() = 0;
    virtual NYPath::TRichYPath GetPipelinePath() = 0;
    virtual NApi::IClientPtr GetClient() = 0;
    virtual NClient::NCache::IClientsCachePtr GetClientsCache() = 0;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TInternalTableInfo
{
    NObjectClient::EObjectType Type;
    std::string TabletCellBundle;
};

//! Throws unless the pipeline has internal tables and all of them share one tablet cell bundle.
void EnsureSameTabletCellBundle(const std::vector<TInternalTableInfo>& tables);

//! Throws unless the pipeline has internal tables and all of them are of one object type.
void EnsureSameTableType(const std::vector<TInternalTableInfo>& tables);

//! Returns whether the pipeline uses chaos-replicated internal tables; throws if the list is
//! empty or contains different object types.
bool IsChaosTableLayout(const std::vector<TInternalTableInfo>& tables);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

ICommonYTConnectorPtr CreateCommonYTConnector(
    NClient::NCache::IClientsCachePtr clientsCache,
    NYPath::TRichYPath pipelinePath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
