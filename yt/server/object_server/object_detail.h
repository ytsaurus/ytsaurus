#pragma once

#include "public.h"
#include "object.h"
#include "object_proxy.h"
#include "object_manager.h"

#include <core/misc/property.h>

#include <server/hydra/entity_map.h>

#include <core/ytree/ypath_detail.h>
#include <core/ytree/system_attribute_provider.h>

#include <ytlib/object_client/object_ypath.pb.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <server/cell_master/public.h>

#include <server/transaction_server/public.h>

#include <server/security_server/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectProxyBase
    : public virtual NYTree::TSupportsAttributes
    , public virtual IObjectProxy
{
public:
    TObjectProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        TObjectBase* object);

    // IObjectProxy members
    virtual const TObjectId& GetId() const override;
    virtual const NYTree::IAttributeDictionary& Attributes() const override;
    virtual NYTree::IAttributeDictionary* MutableAttributes() override;
    virtual TResolveResult Resolve(const NYPath::TYPath& path, NRpc::IServiceContextPtr context) override;
    virtual void Invoke(NRpc::IServiceContextPtr context) override;
    virtual void WriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const NYTree::TAttributeFilter& filter,
        bool sortKeys) override;

protected:
    class TCustomAttributeDictionary;

    NCellMaster::TBootstrap* const Bootstrap_;
    TObjectBase* const Object_;

    std::unique_ptr<NYTree::IAttributeDictionary> CustomAttributes_;


    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetBasicAttributes);
    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CheckPermission);


    //! Returns the full object id that coincides with #Id
    //! for non-versioned objects and additionally includes transaction id for
    //! versioned ones.
    virtual TVersionedObjectId GetVersionedId() const = 0;

    //! Returns the ACD for the object or |nullptr| is none exists.
    virtual NSecurityServer::TAccessControlDescriptor* FindThisAcd() = 0;

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;

    // NYTree::TSupportsAttributes members
    virtual void SetAttribute(
        const NYTree::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        TCtxSetPtr context) override;
    virtual void RemoveAttribute(
        const NYTree::TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        TCtxRemovePtr context) override;

    void ReplicateAttributeUpdate(NRpc::IServiceContextPtr context);

    virtual NYTree::IAttributeDictionary* GetCustomAttributes() override;
    virtual NYTree::ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    virtual std::unique_ptr<NYTree::IAttributeDictionary> DoCreateCustomAttributes();

    // NYTree::ISystemAttributeProvider members
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(const Stroka& key) override;
    virtual bool SetBuiltinAttribute(const Stroka& key, const NYson::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(const Stroka& key) override;

    void DeclareMutating();
    void DeclareNonMutating();

    void ValidateTransaction();
    void ValidateNoTransaction();

    // TSupportsPermissions members
    virtual void ValidatePermission(
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission) override;

    void ValidatePermission(
        TObjectBase* object,
        NYTree::EPermission permission);

    bool IsRecovery() const;
    bool IsMutating() const;
    bool IsLeader() const;
    bool IsFollower() const;

    bool IsPrimaryMaster() const;
    bool IsSecondaryMaster() const;

    //! Returns |true| if reads for this node can only be served at leaders.
    /*!
     *  This flag is checked during YPath resolution so the fallback happens
     *  not only for this particular node but also for all services reachable through
     *  this node.
     */
    virtual bool IsLeaderReadRequired() const;

    //! Posts the request to all secondary masters.
    void PostToSecondaryMasters(NRpc::IServiceContextPtr context);

    //! Posts the request to a given secondary master.
    void PostToSecondaryMaster(NRpc::IServiceContextPtr context, TCellTag cellTag);

    virtual bool IsLoggingEnabled() const override;
    virtual NLogging::TLogger CreateLogger() const override;

};

////////////////////////////////////////////////////////////////////////////////

class TNontemplateNonversionedObjectProxyBase
    : public TObjectProxyBase
{
public:
    TNontemplateNonversionedObjectProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        TObjectBase* object);

protected:
    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;

    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context) override;

    virtual void ValidateRemoval();

    virtual void RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemovePtr context) override;

    virtual TVersionedObjectId GetVersionedId() const override;
    virtual NSecurityServer::TAccessControlDescriptor* FindThisAcd() override;

};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TNonversionedObjectProxyBase
    : public TNontemplateNonversionedObjectProxyBase
{
public:
    TNonversionedObjectProxyBase(NCellMaster::TBootstrap* bootstrap, TObject* object)
        : TNontemplateNonversionedObjectProxyBase(bootstrap, object)
    { }

protected:
    const TObject* GetThisTypedImpl() const
    {
        return static_cast<const TObject*>(Object_);
    }

    TObject* GetThisTypedImpl()
    {
        return static_cast<TObject*>(Object_);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

