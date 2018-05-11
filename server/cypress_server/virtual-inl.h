#pragma once
#ifndef VIRTUAL_INL_H_
#error "Direct inclusion of this file is not allowed, include virtual.h"
#endif

#include <yt/server/cypress_server/lock.h>

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/object_server/object.h>
#include <yt/server/object_server/object_manager.h>

#include <yt/core/misc/string.h>
#include <yt/core/misc/collection_helpers.h>

#include <yt/core/ytree/virtual.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TVirtualObjectMap
    : public NYTree::TVirtualMapBase
{
public:
    TVirtualObjectMap(
        NCellMaster::TBootstrap* bootstrap,
        const NHydra::TReadOnlyEntityMap<TValue>* map,
        NYTree::INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
        , Map_(map)
    { }

protected:
    NCellMaster::TBootstrap* const Bootstrap_;
    const NHydra::TReadOnlyEntityMap<TValue>* const Map_;

    virtual std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        return ConvertToStrings(NYT::GetKeys(*Map_, sizeLimit));
    }

    virtual i64 GetSize() const override
    {
        return Map_->GetSize();
    }

    virtual NYTree::IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        auto id = NHydra::TEntityKey<TValue>::FromString(key);
        auto* object = Map_->Find(id);
        if (!NObjectServer::IsObjectAlive(object)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(object);
    }
};
   
template <class TValue>
NYTree::IYPathServicePtr CreateVirtualObjectMap(
    NCellMaster::TBootstrap* bootstrap,
    const NHydra::TReadOnlyEntityMap<TValue>& map,
    NYTree::INodePtr owningNode)
{
    return New<TVirtualObjectMap<TValue>>(
        bootstrap,
        &map,
        owningNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
