#ifndef VIRTUAL_INL_H_
#error "Direct inclusion of this file is not allowed, include virtual.h"
#endif
#undef VIRTUAL_INL_H_

#include <core/misc/string.h>

#include <core/ytree/virtual.h>

#include <server/object_server/object.h>
#include <server/object_server/object_manager.h>

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
    explicit TVirtualObjectMap(
        NCellMaster::TBootstrap* bootstrap,
        const NHydra::IReadOnlyEntityMap<NObjectServer::TObjectId, TValue>* map)
        : Bootstrap(bootstrap)
        , Map(map)
    { }

protected:
    NCellMaster::TBootstrap* Bootstrap;
    const NHydra::IReadOnlyEntityMap<NObjectServer::TObjectId, TValue>* Map;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto ids = Map->GetKeys(sizeLimit);
        return ConvertToStrings(ids);
    }

    virtual size_t GetSize() const override
    {
        return Map->GetSize();
    }

    virtual NYTree::IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TId::FromString(key);
        auto* object = Map->Find(id);
        if (!NObjectServer::IsObjectAlive(object)) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(object);
    }
};
   
template <
    class TId,
    class TValue
>
NYTree::IYPathServicePtr CreateVirtualObjectMap(
    NCellMaster::TBootstrap* bootstrap,
    const NHydra::IReadOnlyEntityMap<TId, TValue>& map)
{
    return New<TVirtualObjectMap<TId, TValue>>(
        bootstrap,
        &map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
