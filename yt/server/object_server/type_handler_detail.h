#pragma once

#include "type_handler.h"
#include "object_detail.h"
#include "object_manager.h"

#include <ytlib/meta_state/map.h>

#include <server/cell_master/public.h>

#include <server/transaction_server/public.h>

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
        YCHECK(bootstrap);
        YCHECK(map);
    }

    virtual NObjectServer::TObjectBase* FindObject(const TObjectId& id) override
    {
        return Map->Find(id);
    }

    virtual IObjectProxyPtr GetProxy(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction) override
    {
        return DoGetProxy(static_cast<TObject*>(object), transaction);
    }

    virtual TObjectBase* Create(
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account,
        NYTree::IAttributeDictionary* attributes,
        TReqCreateObject* request,
        TRspCreateObject* response) override
    {
        UNUSED(transaction);
        UNUSED(account);
        UNUSED(attributes);
        UNUSED(request);
        UNUSED(response);

        THROW_ERROR_EXCEPTION("Cannot create an instance of %s directly",
            ~FormatEnum(GetType()));
    }

    virtual void Destroy(const TObjectId& id) override
    {
        // Remove the object from the map but keep it alive.
        auto object = Map->Release(id);
        DoDestroy(static_cast<TObject*>(~object));
    }

    virtual void Unstage(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction,
        bool recursive) override
    {
        DoUnstage(static_cast<TObject*>(object), transaction, recursive);
    }

protected:
    NCellMaster::TBootstrap* Bootstrap;
    // We store map by a raw pointer. In most cases this should be OK.
    TMap* Map;

    virtual IObjectProxyPtr DoGetProxy(
        TObject* object,
        NTransactionServer::TTransaction* transaction)
    {
        UNUSED(transaction);
        return New< TNonversionedObjectProxyBase<TObject> >(Bootstrap, object, Map);
    }

    virtual void DoDestroy(TObject* object)
    {
        UNUSED(object);
    }

    virtual void DoUnstage(
        TObject* object,
        NTransactionServer::TTransaction* transaction,
        bool recursive)
    {
        UNUSED(object);
        UNUSED(transaction);
        UNUSED(recursive);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

