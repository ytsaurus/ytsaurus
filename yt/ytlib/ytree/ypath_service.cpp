#include "stdafx.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ephemeral.h"
#include "ypath_client.h"
#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IYPathService::TPtr IYPathService::FromProducer(TYsonProducer* producer)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    producer->Do(~builder);
    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

class TDefaultYPathProcessor
    : public IYPathProcessor
{
public:
    TDefaultYPathProcessor(IYPathService* rootService)
        : RootService(rootService)
    { }

    virtual void Resolve(
        const TYPath& path,
        const Stroka& verb,
        IYPathService::TPtr* suffixService,
        TYPath* suffixPath)
    {
        auto currentPath = path;

        if (currentPath.empty()) {
            ythrow yexception() << "YPath cannot be empty";
        }

        if (currentPath.has_prefix(RootMarker)) {
            currentPath = currentPath.substr(RootMarker.length());
        } else {
            ythrow yexception() << "Invalid YPath syntax";
        }

        ResolveYPath(
            ~RootService,
            currentPath,
            verb,
            suffixService,
            suffixPath);
    }

    virtual void Execute(
        IYPathService* service,
        NRpc::IServiceContext* context)
    {
        service->Invoke(context);
    }

private:
    IYPathService::TPtr RootService;
};

IYPathProcessor::TPtr CreateDefaultProcessor(IYPathService* rootService)
{
    return New<TDefaultYPathProcessor>(rootService);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
