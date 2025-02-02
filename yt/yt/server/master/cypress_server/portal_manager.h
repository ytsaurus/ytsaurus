#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct IPortalManager
    : public virtual TRefCounted
{
public:
    virtual void Initialize() = 0;

    virtual void RegisterEntranceNode(
        TPortalEntranceNode* node,
        const NYTree::IAttributeDictionary& inheritedAttributes,
        const NYTree::IAttributeDictionary& explicitAttributes) = 0;
    virtual void DestroyEntranceNode(TPortalEntranceNode* trunkNode) = 0;

    virtual void DestroyExitNode(TPortalExitNode* trunkNode) = 0;

    using TEntranceNodeMap = THashMap<TNodeId, TPortalEntranceNodeRawPtr>;
    virtual const TEntranceNodeMap& GetEntranceNodes() = 0;

    using TExitNodeMap = THashMap<TNodeId, TPortalExitNodeRawPtr>;
    virtual const TExitNodeMap& GetExitNodes() = 0;

    virtual void ValidateNoNodesBehindRemovedMastersPortal(const THashSet<NObjectClient::TCellTag>& removedMasterCellTags) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IPortalManager)

////////////////////////////////////////////////////////////////////////////////

IPortalManagerPtr CreatePortalManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
