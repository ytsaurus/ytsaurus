#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/core/misc/property.h>

#include <yt/core/ytree/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TPortalManager
    : public TRefCounted
{
public:
    explicit TPortalManager(NCellMaster::TBootstrap* bootstrap);

    ~TPortalManager();

    void RegisterEntranceNode(
        TPortalEntranceNode* node,
        const NYTree::IAttributeDictionary& inheritedAttributes,
        const NYTree::IAttributeDictionary& explicitAttributes);
    void DestroyEntranceNode(TPortalEntranceNode* trunkNode);

    void DestroyExitNode(TPortalExitNode* trunkNode);

    using TEntranceNodeMap = THashMap<TNodeId, TPortalEntranceNode*>;
    DECLARE_BYREF_RO_PROPERTY(TEntranceNodeMap, EntranceNodes);

    using TExitNodeMap = THashMap<TNodeId, TPortalExitNode*>;
    DECLARE_BYREF_RO_PROPERTY(TExitNodeMap, ExitNodes);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TPortalManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
