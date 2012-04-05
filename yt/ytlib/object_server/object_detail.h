#pragma once

#include "public.h"
#include "id.h"
#include "object_proxy.h"
#include "object_manager.h"
#include <ytlib/object_server/object_ypath.pb.h>
#include <ytlib/ytree/ypath.pb.h>

#include <ytlib/misc/property.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/cell_master/public.h>

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

    TObjectProxyBase(NCellMaster::TBootstrap* bootstrap, const TObjectId& id);
    ~TObjectProxyBase();

    // IObjectProxy members
    virtual TObjectId GetId() const;
    virtual NYTree::IAttributeDictionary& Attributes();
    virtual const NYTree::IAttributeDictionary& Attributes() const;
    virtual void Invoke(NRpc::IServiceContext* context);

protected:
    NCellMaster::TBootstrap* Bootstrap;
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
    virtual bool GetSystemAttribute(const Stroka& key, NYTree::IYsonConsumer* consumer);
    virtual bool SetSystemAttribute(const Stroka& key, NYTree::TYsonProducer producer);

    // We need definition of this class in header because we want to inherit it.
    class TUserAttributeDictionary
        : public NYTree::IAttributeDictionary
    {
    public:
        TUserAttributeDictionary(TObjectManager* objectManager, const TObjectId& objectId);

        // NYTree::IAttributeDictionary members
        virtual yhash_set<Stroka> List() const;
        virtual TNullable<NYTree::TYson> FindYson(const Stroka& key) const;
        virtual void SetYson(const Stroka& key, const NYTree::TYson& value);
        virtual bool Remove(const Stroka& key);

    protected:
        TObjectManager::TPtr ObjectManager;
        TObjectId ObjectId;
    };

    class TCombinedAttributeDictionary;
    THolder<TCombinedAttributeDictionary> CombinedAttributes;
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TUnversionedObjectProxyBase
    : public TObjectProxyBase
{
public:
    typedef typename NMetaState::TMetaStateMap<TObjectId, TObject> TMap;

    TUnversionedObjectProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        const TObjectId& id,
        TMap* map)
        : TObjectProxyBase(bootstrap, id)
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

    TObject& GetTypedImpl()
    {
        return Map->Get(GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

