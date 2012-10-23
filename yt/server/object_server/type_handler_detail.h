#pragma once

#include "type_handler.h"
#include "object_detail.h"
#include "object_manager.h"

#include <ytlib/meta_state/map.h>
#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectTypeHandlerBase
    : public IObjectTypeHandler
{
public:
    typedef typename NMetaState::TMetaStateMap<TObjectId, TObject> TMap;

    TObjectTypeHandlerBase(NCellMaster::TBootstrap* bootstrap, TMap* map)
        : Bootstrap(bootstrap)
        , Map(map)
    {
        YASSERT(map);
    }

    virtual bool Exists(const TObjectId& id)
    {
        auto* obj = Map->Find(id);
        return obj && obj->GetObjectRefCounter() > 0;
    }

    virtual i32 RefObject(const TObjectId& id)
    {
        auto* obj = Map->Get(id);
        return obj->RefObject();
    }

    virtual i32 UnrefObject(const TObjectId& id)
    {
        auto* obj = Map->Get(id);
        return obj->UnrefObject();
    }

    virtual i32 GetObjectRefCounter(const TObjectId& id)
    {
        auto* obj = Map->Get(id);
        return obj->GetObjectRefCounter();
    }

    virtual IObjectProxyPtr GetProxy(
        const TObjectId& id,
        NTransactionServer::TTransaction* transaction)
    {
        UNUSED(transaction);
        return New< TUnversionedObjectProxyBase<TObject> >(Bootstrap, id, Map);
    }

    virtual TObjectId Create(
        NTransactionServer::TTransaction* transaction,
        TReqCreateObject* request,
        TRspCreateObject* response)
    {
        UNUSED(transaction);
        UNUSED(request);
        UNUSED(response);

        THROW_ERROR_EXCEPTION("Cannot create an instance of %s directly",
            ~FormatEnum(GetType()));
    }

    virtual bool IsTransactionRequired() const
    {
        return true;
    }

    virtual void Destroy(const TObjectId& objectId)
    {
        // Remove the object from the map but keep it alive.
        TAutoPtr<TObject> objHolder(Map->Release(objectId));
        DoDestroy(~objHolder);
    }

protected:
    NCellMaster::TBootstrap* Bootstrap;
    // We store map by a raw pointer. In most cases this should be OK.
    TMap* Map;

    virtual void DoDestroy(TObject* obj)
    {
        UNUSED(obj);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

