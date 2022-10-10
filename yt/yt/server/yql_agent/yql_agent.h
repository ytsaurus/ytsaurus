#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/yql_client/proto/yql_service.pb.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/rpc/bus/public.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

struct IYqlAgent
    : public TRefCounted
{
    virtual void Start() = 0;

    virtual void Stop() = 0;

    virtual NYTree::IMapNodePtr GetOrchidNode() const = 0;

    virtual void OnDynamicConfigChanged(
        const TYqlAgentDynamicConfigPtr& oldConfig,
        const TYqlAgentDynamicConfigPtr& newConfig) = 0;

    virtual TFuture<NYqlClient::NProto::TYqlResponse> StartQuery(
        TQueryId queryId,
        const NYqlClient::NProto::TYqlRequest& request) = 0;
};

DEFINE_REFCOUNTED_TYPE(IYqlAgent)

IYqlAgentPtr CreateYqlAgent(
    TYqlAgentConfigPtr config,
    NHiveClient::TClusterDirectoryPtr clusterDirectory,
    IInvokerPtr controlInvoker,
    NCypressElection::ICypressElectionManagerPtr electionManager,
    TString agentId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
