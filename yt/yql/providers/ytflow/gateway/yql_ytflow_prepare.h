#pragma once

#include "yql_ytflow_config_clusters.h"
#include "yql_ytflow_logbroker_cm_clients_cache.h"
#include "yql_ytflow_monium_clients_cache.h"

#include <library/cpp/yt/memory/ref_counted.h>

#include <yql/essentials/ast/yql_expr.h>

#include <yt/yql/providers/ytflow/provider/yql_ytflow_gateway.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>


namespace NYql::NYtflow::NPrepare {

struct TContext
{
    TExprContext& ExprContext;
    const IYtflowGateway::TRunOptions& RunOptions;
    TConfigClusters::TPtr ConfigClusters;
};

DECLARE_REFCOUNTED_CLASS(IAction);

class IAction: virtual public NYT::TRefCounted
{
public:
    virtual void Init(TExprNode::TPtr node, TContext& prepareCtx) = 0;
    virtual NYT::TFuture<void> Run(NYT::IInvokerPtr invoker) = 0;
};

IActionPtr CreatePipelineNodeAction();

IActionPtr CreateYtConsumersAction();
IActionPtr CreateYtProducersAction();
IActionPtr CreateOutputTablesAction();

IActionPtr CreateLogbrokerDirectories(
    ILogbrokerCmClientsCachePtr cmClientsCache,
    TString ydbToken);
IActionPtr CreateLogbrokerConsumers(
    ILogbrokerCmClientsCachePtr cmClientsCache,
    TString ydbToken);
IActionPtr CreateOutputLogbrokerTopics(
    ILogbrokerCmClientsCachePtr cmClientsCache,
    TString ydbToken);
IActionPtr CreateLogbrokerReadRules(
    ILogbrokerCmClientsCachePtr cmClientsCache,
    TString ydbToken);
IActionPtr CreateInputTopicPermissions(
    ILogbrokerCmClientsCachePtr cmClientsCache,
    TString ydbToken);

IActionPtr CheckMoniumProjects(
    IMoniumClientsCachePtr clientsCache,
    TString moniumToken);
IActionPtr CheckMoniumPermissions(
    IMoniumClientsCachePtr clientsCache,
    TString moniumToken);
IActionPtr CreateMoniumResources(
    IMoniumClientsCachePtr clientsCache,
    TString moniumToken);

} // namespace NYql::NYtflow::NPrepare
