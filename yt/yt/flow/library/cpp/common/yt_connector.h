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

ICommonYTConnectorPtr CreateCommonYTConnector(
    NClient::NCache::IClientsCachePtr clientsCache,
    NYPath::TRichYPath pipelinePath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
