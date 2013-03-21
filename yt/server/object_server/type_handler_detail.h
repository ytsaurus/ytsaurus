#pragma once

#include "type_handler.h"
#include "object_detail.h"
#include "object_manager.h"

#include <ytlib/meta_state/map.h>

#include <server/cell_master/bootstrap.h>

#include <server/transaction_server/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectTypeHandlerBase
    : public IObjectTypeHandler
{
public:
    explicit TObjectTypeHandlerBase(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    {
        YCHECK(bootstrap);
    }

    virtual Stroka GetName(TObjectBase* object) override
    {
        return DoGetName(static_cast<TObject*>(object));
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

    virtual void Unstage(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction,
        bool recursive) override
    {
        DoUnstage(static_cast<TObject*>(object), transaction, recursive);
    }

    virtual NSecurityServer::TAccessControlDescriptor* FindAcd(TObjectBase* object) override
    {
        return DoFindAcd(static_cast<TObject*>(object));
    }

    virtual TObjectBase* GetParent(TObjectBase* object) override
    {
        return DoGetParent(static_cast<TObject*>(object));
    }

    virtual NYTree::EPermissionSet GetSupportedPermissions() const override
    {
        return NYTree::EPermissionSet(
            NYTree::EPermission::Read |
            NYTree::EPermission::Write);
    }
    
protected:
    NCellMaster::TBootstrap* Bootstrap;

    virtual Stroka DoGetName(TObject* object) = 0;

    virtual IObjectProxyPtr DoGetProxy(
        TObject* object,
        NTransactionServer::TTransaction* transaction)
    {
        UNUSED(transaction);
        return New< TNonversionedObjectProxyBase<TObject> >(Bootstrap, object);
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

    virtual NSecurityServer::TAccessControlDescriptor* DoFindAcd(TObject* object)
    {
        UNUSED(object);
        return nullptr;
    }

    virtual TObjectBase* DoGetParent(TObject* object)
    {
        UNUSED(object);
        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->FindSchema(GetType());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectTypeHandlerWithMapBase
    : public TObjectTypeHandlerBase<TObject>
{
public:
    typedef typename NMetaState::TMetaStateMap<TObjectId, TObject> TMap;

    TObjectTypeHandlerWithMapBase(NCellMaster::TBootstrap* bootstrap, TMap* map)
        : TObjectTypeHandlerBase<TObject>(bootstrap)
        , Map(map)
    { }

    virtual void Destroy(TObjectBase* object) override
    {
        // Clear ACD, if any.
        auto* acd = this->FindAcd(object);
        if (acd) {
            acd->Clear();
        }

        // Remove user attributes, if any.
        auto objectManager = this->Bootstrap->GetObjectManager();
        objectManager->TryRemoveAttributes(TVersionedObjectId(object->GetId()));

        // Remove the object from the map but keep it alive.
        auto objectHolder = Map->Release(object->GetId());

        DoDestroy(static_cast<TObject*>(object));
    }
    
    virtual NObjectServer::TObjectBase* FindObject(const TObjectId& id) override
    {
        return Map->Find(id);
    }

private:
    // We store map by a raw pointer. In most cases this should be OK.
    TMap* Map;

    virtual void DoDestroy(TObject* object)
    {
        UNUSED(object);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

