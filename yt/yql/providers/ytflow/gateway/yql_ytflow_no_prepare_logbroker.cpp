#include "yql_ytflow_prepare.h"

#include <yt/yt/core/actions/future.h>


namespace NYql::NYtflow::NPrepare {

namespace {

class TNoopAction
    : public IAction
{
public:
    void Init(TExprNode::TPtr /*node*/, TContext& /*prepareCtx*/) override
    { }

    NYT::TFuture<void> Run(NYT::IInvokerPtr /*invoker*/) override
    {
        return NYT::OKFuture;
    }
};

IActionPtr CreateNoopAction()
{
    return NYT::New<TNoopAction>();
}

} // anonymous namespace

IActionPtr CreateLogbrokerDirectories(
    ILogbrokerCmClientsCachePtr /*cmClientsCache*/,
    TString /*ydbToken*/)
{
    return CreateNoopAction();
}

IActionPtr CreateLogbrokerConsumers(
    ILogbrokerCmClientsCachePtr /*cmClientsCache*/,
    TString /*ydbToken*/)
{
    return CreateNoopAction();
}

IActionPtr CreateOutputLogbrokerTopics(
    ILogbrokerCmClientsCachePtr /*cmClientsCache*/,
    TString /*ydbToken*/)
{
    return CreateNoopAction();
}

IActionPtr CreateLogbrokerReadRules(
    ILogbrokerCmClientsCachePtr /*cmClientsCache*/,
    TString /*ydbToken*/)
{
    return CreateNoopAction();
}

IActionPtr CreateInputTopicPermissions(
    ILogbrokerCmClientsCachePtr /*cmClientsCache*/,
    TString /*ydbToken*/)
{
    return CreateNoopAction();
}

} // namespace NYql::NYtflow::NPrepare
