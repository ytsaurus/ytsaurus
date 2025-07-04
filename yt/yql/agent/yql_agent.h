#pragma once

#include "private.h"
#include "bootstrap.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/yql_client/proto/yql_service.pb.h>

#include <yt/yt/library/program/config.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/rpc/bus/public.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

struct IYqlAgent
    : public TRefCounted
{
    virtual void Start() = 0;

    virtual void Stop() = 0;

    virtual NYTree::IYPathServicePtr CreateOrchidService() const = 0;

    virtual void OnDynamicConfigChanged(
        const TYqlAgentDynamicConfigPtr& oldConfig,
        const TYqlAgentDynamicConfigPtr& newConfig) = 0;

    virtual TFuture<std::pair<NYqlClient::NProto::TRspStartQuery, std::vector<TSharedRef>>> StartQuery(
        TQueryId queryId,
        const TString& impersonationUser,
        const NYqlClient::NProto::TReqStartQuery& request) = 0;

    virtual TFuture<void> AbortQuery(TQueryId queryId) = 0;

    virtual NYqlClient::NProto::TRspGetQueryProgress GetQueryProgress(TQueryId queryId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IYqlAgent)

////////////////////////////////////////////////////////////////////////////////

IYqlAgentPtr CreateYqlAgent(
    TBootstrap* bootstrap,
    TSingletonsConfigPtr singletonsConfig,
    TYqlAgentConfigPtr config,
    TYqlAgentDynamicConfigPtr dynamicConfig,
    NHiveClient::TClusterDirectoryPtr clusterDirectory,
    NHiveClient::TClientDirectoryPtr clientDirectory,
    IInvokerPtr controlInvoker,
    TString agentId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
