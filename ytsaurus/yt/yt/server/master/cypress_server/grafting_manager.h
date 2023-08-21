#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct IGraftingManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void OnRootstockCreated(
        TRootstockNode* rootstock,
        const NYTree::IAttributeDictionary& inheritedAttributes,
        const NYTree::IAttributeDictionary& explicitAttributes) = 0;
    virtual void OnRootstockDestroyed(TRootstockNode* rootstockNode) = 0;

    virtual void OnScionDestroyed(TScionNode* scionNode) = 0;

    using TRootstockNodeMap = THashMap<TNodeId, TRootstockNode*>;
    virtual const TRootstockNodeMap& RootstockNodes() = 0;

    using TScionNodeMap = THashMap<TNodeId, TScionNode*>;
    virtual const TScionNodeMap& ScionNodes() = 0;
};

DEFINE_REFCOUNTED_TYPE(IGraftingManager)

////////////////////////////////////////////////////////////////////////////////

IGraftingManagerPtr CreateGraftingManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
