#include "stdafx.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ephemeral.h"
#include "ypath_client.h"
#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IYPathService::TPtr IYPathService::FromProducer(TYsonProducer producer)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    producer->Do(~builder);
    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
