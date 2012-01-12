#pragma once

#include "type_handler.h"
#include "object_detail.h"

#include <yt/ytlib/meta_state/map.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectTypeHandlerBase
    : public IObjectTypeHandler
{
public:
    typedef typename NMetaState::TMetaStateMap<TObjectId, TObject> TMap;

    TObjectTypeHandlerBase(TMap* map)
        : Map(map)
    {
        YASSERT(map);
    }

    virtual bool Exists(const TObjectId& id)
    {
        return Map->Contains(id);
    }

    virtual i32 RefObject(const TObjectId& id)
    {
        auto& obj = Map->GetForUpdate(id);
        return obj.RefObject();
    }

    virtual i32 UnrefObject(const TObjectId& id)
    {
        auto& obj = Map->GetForUpdate(id);
        i32 result = obj.UnrefObject();
        if (result == 0) {
            OnObjectDestroyed(obj);
            Map->Remove(id);
        }
        return result;
    }

    virtual i32 GetObjectRefCounter(const TObjectId& id)
    {
        auto& obj = Map->Get(id);
        return obj.GetObjectRefCounter();
    }

    IObjectProxy::TPtr FindProxy(const TObjectId& id)
    {
        return
            Map->Contains(id)
            ? New< TObjectProxyBase<TObject> >(id, Map)
            : NULL;
    }

protected:
    // We store map by a raw pointer. In most cases this should be OK.
    TMap* Map;

    virtual void OnObjectDestroyed(TObject& obj)
    {
        UNUSED(obj);
    }
    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

