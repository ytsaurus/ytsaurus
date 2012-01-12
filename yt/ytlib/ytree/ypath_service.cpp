#include "stdafx.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ephemeral.h"

#include <ytlib/misc/singleton.h>

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

class TDefaultYPathExecutor
    : public IYPathExecutor
{
public:
    virtual void ExecuteVerb(
        IYPathService* service,
        NRpc::IServiceContext* context)
    {
        service->Invoke(context);
    }
};

IYPathExecutor::TPtr GetDefaultExecutor()
{
    return RefCountedSingleton<TDefaultYPathExecutor>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
