#ifndef VIRTUAL_INL_H_
#error "Direct inclusion of this file is not allowed, include virtual.h"
#endif

#include <core/misc/string.h>
#include <core/misc/collection_helpers.h>

#include <core/ytree/virtual.h>

#include <server/object_server/object.h>
#include <server/object_server/object_manager.h>

#include <server/cypress_server/lock.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

template <
    class TId,
    class TValue
>
class TVirtualObjectMap
    : public NYTree::TVirtualMapBase
{
public:
    TVirtualObjectMap(
        NCellMaster::TBootstrap* bootstrap,
        const NHydra::TReadOnlyEntityMap<NObjectServer::TObjectId, TValue>* map,
        NYTree::INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
        , Map_(map)
    { }

protected:
    NCellMaster::TBootstrap* const Bootstrap_;
    const NHydra::TReadOnlyEntityMap<NObjectServer::TObjectId, TValue>* const Map_;

    virtual std::vector<Stroka> GetKeys(i64 sizeLimit) const override
    {
        return ConvertToStrings(NYT::GetKeys(*Map_, sizeLimit));
    }

    virtual i64 GetSize() const override
    {
        return Map_->GetSize();
    }

    virtual NYTree::IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TId::FromString(key);
        auto* object = Map_->Find(id);
        if (!NObjectServer::IsObjectAlive(object)) {
            return nullptr;
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(object);
    }
};
   
template <class TId, class TValue>
NYTree::IYPathServicePtr CreateVirtualObjectMap(
    NCellMaster::TBootstrap* bootstrap,
    const NHydra::TReadOnlyEntityMap<TId, TValue>& map,
    NYTree::INodePtr owningNode)
{
    return New<TVirtualObjectMap<TId, TValue>>(
        bootstrap,
        &map,
        owningNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
