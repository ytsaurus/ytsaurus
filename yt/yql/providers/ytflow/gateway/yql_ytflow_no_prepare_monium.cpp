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

IActionPtr CheckMoniumProjects(
    IMoniumClientsCachePtr /*clientsCache*/,
    TString /*moniumToken*/)
{
    return CreateNoopAction();
}

IActionPtr CheckMoniumPermissions(
    IMoniumClientsCachePtr /*clientsCache*/,
    TString /*moniumToken*/)
{
    return CreateNoopAction();
}

IActionPtr CreateMoniumResources(
    IMoniumClientsCachePtr /*clientsCache*/,
    TString /*moniumToken*/)
{
    return CreateNoopAction();
}

} // namespace NYql::NYtflow::NPrepare
