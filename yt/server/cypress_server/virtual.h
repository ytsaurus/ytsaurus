#pragma once

#include "type_handler.h"

#include <server/hydra/entity_map.h>

#include <server/object_server/public.h>

#include <server/cypress_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_FLAGGED_ENUM(EVirtualNodeOptions,
    ((None)            (0x0000))
    ((RequireLeader)   (0x0001))
    ((RedirectSelf)    (0x0002))
);

typedef
    TCallback< NYTree::IYPathServicePtr(TCypressNodeBase*, NTransactionServer::TTransaction*) >
    TYPathServiceProducer;

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType objectType,
    TYPathServiceProducer producer,
    EVirtualNodeOptions options);

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType objectType,
    NYTree::IYPathServicePtr service,
    EVirtualNodeOptions options);

template <
    class TId,
    class TValue
>
NYTree::IYPathServicePtr CreateVirtualObjectMap(
    NCellMaster::TBootstrap* bootstrap,
    const NHydra::TReadOnlyEntityMap<TId, TValue>& map);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

#define VIRTUAL_INL_H_
#include "virtual-inl.h"
#undef VIRTUAL_INL_H_
