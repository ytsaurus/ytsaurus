#pragma once

#include "id.h"
#include "object_proxy.h"
#include "object_manager.h"
#include "object_ypath.pb.h"
#include "ypath.pb.h"

#include <ytlib/misc/property.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/fluent.h>

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

    void Save(TOutputStream* output) const;
    void Load(TInputStream* input);

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

class TObjectProxyBase
    : public virtual NYTree::TYPathServiceBase
    , public virtual NYTree::TSupportsGetAttribute
    , public virtual NYTree::TSupportsListAttribute
    , public virtual NYTree::TSupportsSetAttribute
    , public virtual NYTree::TSupportsRemoveAttribute
    , public virtual NYTree::ISystemAttributeProvider
    , public virtual IObjectProxy
{
public:
    typedef TIntrusivePtr<TObjectProxyBase> TPtr;

    TObjectProxyBase(
        TObjectManager* objectManager,
        const TObjectId& id,
        const Stroka& loggingCategory = ObjectServerLogger.GetCategory());

    // IObjectProxy members
    virtual TObjectId GetId() const;

protected:
    TObjectManager::TPtr ObjectManager;
    TObjectId Id;

    DECLARE_RPC_SERVICE_METHOD(NObjectServer::NProto, GetId);

    // NYTree::TYPathServiceBase members
    virtual TResolveResult ResolveAttributes(const NYTree::TYPath& path, const Stroka& verb);
    virtual void DoInvoke(NRpc::IServiceContext* context);
    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const;

    // NYTree::IAttributeProvider members
    virtual NYTree::IAttributeDictionary::TPtr GetUserAttributeDictionary();
    virtual ISystemAttributeProvider::TPtr GetSystemAttributeProvider();

    // NYTree::ISystemAttributeProvider members
    virtual void GetSystemAttributes(yvector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer);
    virtual bool SetSystemAttribute(const Stroka& name, NYTree::TYsonProducer* producer);

    // We need definition of this class in header because we want to inherit it.
    class TUserAttributeDictionary
        : public NYTree::IAttributeDictionary
    {
    public:
        TUserAttributeDictionary(TObjectId objectId, TObjectManager* objectManager);

        // NYTree::IAttributeDictionary members
        virtual yhash_set<Stroka> ListAttributes();
        virtual NYTree::TYson FindAttribute(const Stroka& name);
        virtual void SetAttribute(const Stroka& name, const NYTree::TYson& value);
        virtual bool RemoveAttribute(const Stroka& name);

    protected:
        TObjectId ObjectId;
        TObjectManager::TPtr ObjectManager;
    };
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TUnversionedObjectProxyBase
    : public TObjectProxyBase
{
public:
    typedef typename NMetaState::TMetaStateMap<TObjectId, TObject> TMap;

    TUnversionedObjectProxyBase(
        TObjectManager* objectManager,
        const TObjectId& id,
        TMap* map,
        const Stroka& loggingCategory = ObjectServerLogger.GetCategory())
        : TObjectProxyBase(objectManager, id, loggingCategory)
        , Map(map)
    {
        YASSERT(map);
    }

protected:
    TMap* Map;

    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
    {
        UNUSED(request);

        response->set_value(NYTree::BuildYsonFluently().Entity());
        context->Reply();
    }


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

