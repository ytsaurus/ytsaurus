#include "stdafx.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ephemeral.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

IYPathService::TPtr IYPathService::FromNode(INode* node)
{
    YASSERT(node != NULL);
    auto* service = dynamic_cast<IYPathService*>(node);
    if (service == NULL) {
        ythrow yexception() << "Node does not support YPath";
    }
    return service;
}

IYPathService::TPtr IYPathService::FromProducer(TYsonProducer* producer)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    producer->Do(~builder);
    return FromNode(~builder->EndTree());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
