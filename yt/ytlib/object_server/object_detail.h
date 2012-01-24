#pragma once

#include "id.h"
#include "object_proxy.h"
#include "object_manager.h"
#include "object_ypath.pb.h"
#include "ypath.pb.h"

#include <ytlib/misc/property.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/ytree/ypath_detail.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectBase
{
public:
    TObjectBase();

    //! Increments the object's reference counter.
    /*!
     *  \returns the incremented counter.
     */
    i32 RefObject();

    //! Decrements the object's reference counter.
    /*!
     *  \note
     *  Objects do not self-destruct, it's callers responsibility to check
     *  if the counter reaches zero.
     *  
     *  \returns the decremented counter.
     */
    i32 UnrefObject();

    //! Returns the current reference counter.
    i32 GetObjectRefCounter() const;

protected:
    TObjectBase(const TObjectBase& other);

    i32 RefCounter;

};

////////////////////////////////////////////////////////////////////////////////

class TObjectWithIdBase
    : public TObjectBase
{
    DEFINE_BYVAL_RO_PROPERTY(TObjectId, Id);

public:
    TObjectWithIdBase();
    TObjectWithIdBase(const TObjectId& id);
    TObjectWithIdBase(const TObjectWithIdBase& other);

};

////////////////////////////////////////////////////////////////////////////////

class TUntypedObjectProxyBase
    : public IObjectProxy
    , public NYTree::TYPathServiceBase
{
public:
    TUntypedObjectProxyBase(
        TObjectManager* objectManager,
        const TObjectId& id,
        const Stroka& loggingCategory = ObjectServerLogger.GetCategory());

    virtual TObjectId GetId() const;

private:
    TObjectManager::TPtr ObjectManager;
    TObjectId Id;

    virtual TResolveResult ResolveAttributes(const NYTree::TYPath& path, const Stroka& verb);

    DECLARE_RPC_SERVICE_METHOD(NObjectServer::NProto, GetId);
    DECLARE_RPC_SERVICE_METHOD(NYTree::NProto, Get);
    DECLARE_RPC_SERVICE_METHOD(NYTree::NProto, List);
    DECLARE_RPC_SERVICE_METHOD(NYTree::NProto, Set);
    DECLARE_RPC_SERVICE_METHOD(NYTree::NProto, Remove);

    Stroka DoGetAttribute(const Stroka& name, bool* isSystem = NULL);
    void DoSetAttribute(const Stroka name, NYTree::INode* value, bool isSystem);

protected:
    virtual TTransactionId GetTransactionId() const;

    virtual void DoInvoke(NRpc::IServiceContext* context);

    virtual void GetSystemAttributeNames(yvector<Stroka>* names);
    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer);
    
    //! Returns false if there is no system attribute with the given name.
    //! Throws if the system attribute cannot be set.
    virtual bool SetSystemAttribute(const Stroka& name, NYTree::TYsonProducer* producer);

    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context);
    virtual void GetRecursive(const NYTree::TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context);
    virtual void GetAttribute(const NYTree::TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context);

    virtual void ListSelf(TReqList* request, TRspList* response, TCtxList* context);
    virtual void ListRecursive(const NYTree::TYPath& path, TReqList* request, TRspList* response, TCtxList* context);
    virtual void ListAttribute(const NYTree::TYPath& path, TReqList* request, TRspList* response, TCtxList* context);

    virtual void SetSelf(TReqSet* request, TRspSet* response, TCtxSet* context);
    virtual void SetRecursive(const NYTree::TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context);
    virtual void SetAttribute(const NYTree::TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context);

    virtual void RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemove* context);
    virtual void RemoveRecursive(const NYTree::TYPath& path, TReqRemove* request, TRspRemove* response, TCtxRemove* context);
    virtual void RemoveAttribute(const NYTree::TYPath& path, TReqRemove* request, TRspRemove* response, TCtxRemove* context);
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectProxyBase
    : public TUntypedObjectProxyBase
{
public:
    typedef typename NMetaState::TMetaStateMap<TObjectId, TObject> TMap;

    TObjectProxyBase(
        TObjectManager* objectManager,
        const TObjectId& id,
        TMap* map,
        const Stroka& loggingCategory = ObjectServerLogger.GetCategory())
        : TUntypedObjectProxyBase(objectManager, id, loggingCategory)
        , Map(map)
    {
        YASSERT(map);
    }

protected:
    TMap* Map;

    const TObject& GetTypedImpl() const
    {
        return Map->Get(GetId());
    }

    TObject& GetTypedImplForUpdate()
    {
        return Map->GetForUpdate(GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

