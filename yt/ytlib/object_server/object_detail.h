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
    , public virtual NYTree::TSupportsAttributes
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
    virtual NYTree::IAttributeDictionary* Attributes();
    virtual void Invoke(NRpc::IServiceContext* context);

protected:
    TObjectManager::TPtr ObjectManager;
    TObjectId Id;
    TAutoPtr<NYTree::IAttributeDictionary> UserAttributes;

    DECLARE_RPC_SERVICE_METHOD(NObjectServer::NProto, GetId);

    virtual TVersionedObjectId GetVersionedId() const;

    // NYTree::TYPathServiceBase members
    virtual void DoInvoke(NRpc::IServiceContext* context);
    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const;

    // NYTree::TSupportsAttributes members
    virtual NYTree::IAttributeDictionary* GetUserAttributes();
    virtual ISystemAttributeProvider* GetSystemAttributeProvider();

    virtual TAutoPtr<NYTree::IAttributeDictionary> DoCreateUserAttributes();

    // NYTree::ISystemAttributeProvider members
    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer);
    virtual bool SetSystemAttribute(const Stroka& name, NYTree::TYsonProducer* producer);

    // We need definition of this class in header because we want to inherit it.
    class TUserAttributeDictionary
        : public NYTree::IAttributeDictionary
    {
    public:
        TUserAttributeDictionary(const TObjectId& objectId, TObjectManager* objectManager);

        // NYTree::IAttributeDictionary members
        virtual yhash_set<Stroka> List();
        virtual NYTree::TYson FindYson(const Stroka& name);
        virtual void SetYson(const Stroka& name, const NYTree::TYson& value);
        virtual bool Remove(const Stroka& name);

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

