#pragma once

#include "public.h"
#include "object_proxy.h"
#include "object_manager.h"

#include <ytlib/object_client/object_ypath.pb.h>

#include <ytlib/ytree/ypath.pb.h>

#include <ytlib/misc/property.h>

#include <ytlib/meta_state/map.h>

#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/fluent.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectBase
    : private TNonCopyable
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
    void Save(const NMetaState::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

    i32 RefCounter;

};

////////////////////////////////////////////////////////////////////////////////

class TObjectWithIdBase
    : public TObjectBase
{
    DEFINE_BYVAL_RO_PROPERTY(TObjectId, Id);

public:
    TObjectWithIdBase();
    explicit TObjectWithIdBase(const TObjectId& id);

};

////////////////////////////////////////////////////////////////////////////////

// We need definition of this class in header because we want to inherit it.
class TUserAttributeDictionary
    : public NYTree::IAttributeDictionary
{
public:
    TUserAttributeDictionary(TObjectManagerPtr objectManager, const TObjectId& objectId);

    // NYTree::IAttributeDictionary members
    virtual std::vector<Stroka> List() const override;
    virtual TNullable<NYTree::TYsonString> FindYson(const Stroka& key) const override;
    virtual void SetYson(const Stroka& key, const NYTree::TYsonString& value) override;
    virtual bool Remove(const Stroka& key) override;

protected:
    TObjectManagerPtr ObjectManager;
    TObjectId ObjectId;
};

////////////////////////////////////////////////////////////////////////////////

class TObjectProxyBase
    : public virtual NYTree::TYPathServiceBase
    , public virtual NYTree::TSupportsAttributes
    , public virtual NYTree::ISystemAttributeProvider
    , public virtual IObjectProxy
{
public:
    TObjectProxyBase(NCellMaster::TBootstrap* bootstrap, const TObjectId& id);
    ~TObjectProxyBase();

    // IObjectProxy members
    virtual const TObjectId& GetId() const override;
    virtual NYTree::IAttributeDictionary& Attributes() override;
    virtual const NYTree::IAttributeDictionary& Attributes() const override;
    virtual void Invoke(NRpc::IServiceContextPtr context) override;

protected:
    NCellMaster::TBootstrap* Bootstrap;
    TObjectId Id;
    TAutoPtr<NYTree::IAttributeDictionary> UserAttributes;

    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, GetId);

    //! Returns the full object id that coincides with #Id
    //! for non-versioned objects and additionally includes transaction id for
    //! versioned ones.
    virtual TVersionedObjectId GetVersionedId() const;

    void GuardedInvoke(NRpc::IServiceContextPtr context);
    virtual void DoInvoke(NRpc::IServiceContextPtr context) override;
    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override;

    // NYTree::TSupportsAttributes members
    virtual NYTree::IAttributeDictionary* GetUserAttributes() override;
    virtual ISystemAttributeProvider* GetSystemAttributeProvider() override;

    virtual TAutoPtr<NYTree::IAttributeDictionary> DoCreateUserAttributes();

    // NYTree::ISystemAttributeProvider members
    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetSystemAttribute(const Stroka& key, NYTree::IYsonConsumer* consumer) override;
    virtual bool SetSystemAttribute(const Stroka& key, const NYTree::TYsonString& value) override;

    bool IsRecovery() const;
    void ValidateLeaderStatus();
    void ForwardToLeader(NRpc::IServiceContextPtr context);
    void OnLeaderResponse(NRpc::IServiceContextPtr context, NBus::IMessagePtr responseMessage);

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
        YCHECK(map);
    }

protected:
    TMap* Map;

    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context) override
    {
        UNUSED(request);

        response->set_value(NYTree::BuildYsonFluently().Entity().ToString());
        context->Reply();
    }


    const TObject* GetTypedImpl() const
    {
        return Map->Get(GetId());
    }

    TObject* GetTypedImpl()
    {
        return Map->Get(GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

