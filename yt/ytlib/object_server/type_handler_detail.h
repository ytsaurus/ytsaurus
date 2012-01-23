#pragma once

#include "type_handler.h"
#include "object_detail.h"
#include "object_manager.h"

#include <ytlib/meta_state/map.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectTypeHandlerBase
    : public IObjectTypeHandler
{
public:
    typedef typename NMetaState::TMetaStateMap<TObjectId, TObject> TMap;

    TObjectTypeHandlerBase(TObjectManager* objectManager, TMap* map)
        : ObjectManager(objectManager)
        , Map(map)
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

    virtual IObjectProxy::TPtr GetProxy(const TObjectId& id)
    {
        return CreateProxy(id);
    }

    virtual TObjectId CreateFromManifest(
        const NObjectServer::TTransactionId& transactionId,
        NYTree::IMapNode* manifest)
    {
        UNUSED(transactionId);
        UNUSED(manifest);
        ythrow yexception() << Sprintf("Object cannot be created from a manifest (Type: %s)",
            ~GetType().ToString());
    }

    virtual bool IsTransactionRequired() const
    {
        return true;
    }

protected:
    TIntrusivePtr<TObjectManager> ObjectManager;
    // We store map by a raw pointer. In most cases this should be OK.
    TMap* Map;

    virtual void OnObjectDestroyed(TObject& obj)
    {
        UNUSED(obj);
    }

    virtual IObjectProxy::TPtr CreateProxy(const TObjectId& id)
    {
        return New< TObjectProxyBase<TObject> >(~ObjectManager, id, Map);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

