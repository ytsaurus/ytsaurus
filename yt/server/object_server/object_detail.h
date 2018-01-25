#pragma once

#include "public.h"
#include "object.h"
#include "object_manager.h"
#include "object_proxy.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/hydra/entity_map.h>

#include <yt/server/security_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/object_ypath.pb.h>

#include <yt/core/misc/property.h>

#include <yt/core/ytree/system_attribute_provider.h>
#include <yt/core/ytree/ypath_detail.h>

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
        TObjectTypeMetadata* metadata,
        TObjectBase* object);

    virtual bool ShouldHideAttributes() override;

    // IObjectProxy members
    virtual const TObjectId& GetId() const override;
    virtual TObjectBase* GetObject() const override;
    virtual const NYTree::IAttributeDictionary& Attributes() const override;
    virtual NYTree::IAttributeDictionary* MutableAttributes() override;
    virtual void Invoke(const NRpc::IServiceContextPtr& context) override;
    virtual void DoWriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const TNullable<std::vector<TString>>& attributeKeys,
        bool stable) override;


protected:
    NCellMaster::TBootstrap* const Bootstrap_;
    TObjectTypeMetadata* const Metadata_;
    TObjectBase* const Object_;

    NYTree::IAttributeDictionary* CustomAttributes_ = nullptr;


    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetBasicAttributes);
    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CheckPermission);


    //! Returns the full object id that coincides with #Id
    //! for non-versioned objects and additionally includes transaction id for
    //! versioned ones.
    virtual TVersionedObjectId GetVersionedId() const = 0;

    //! Returns the ACD for the object or |nullptr| is none exists.
    virtual NSecurityServer::TAccessControlDescriptor* FindThisAcd() = 0;

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    // NYTree::TSupportsAttributes members
    virtual void SetAttribute(
        const NYTree::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;
    virtual void RemoveAttribute(
        const NYTree::TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;

    void ReplicateAttributeUpdate(NRpc::IServiceContextPtr context);

    virtual NYTree::IAttributeDictionary* GetCustomAttributes() override;
    virtual NYTree::ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    // NYTree::ISystemAttributeProvider members
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual const THashSet<const char*>& GetBuiltinAttributeKeys() override;
    virtual bool GetBuiltinAttribute(const TString& key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(const TString& key) override;
    virtual bool SetBuiltinAttribute(const TString& key, const NYson::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(const TString& key) override;

    //! Called before attribute #key is updated (added, removed or changed).
    virtual void ValidateCustomAttributeUpdate(
        const TString& key,
        const NYson::TYsonString& oldValue,
        const NYson::TYsonString& newValue);

    void ValidateCustomAttributeLength(const NYson::TYsonString& value);

    //! Same as #ValidateCustomAttributeUpdate but wraps the exceptions.
    void GuardedValidateCustomAttributeUpdate(
        const TString& key,
        const NYson::TYsonString& oldValue,
        const NYson::TYsonString& newValue);

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
    void RequireLeader() const;

    bool IsPrimaryMaster() const;
    bool IsSecondaryMaster() const;

    //! Posts the request to all secondary masters.
    void PostToSecondaryMasters(NRpc::IServiceContextPtr context);

    //! Posts the request to given masters.
    void PostToMasters(NRpc::IServiceContextPtr context, const TCellTagList& cellTags);

    //! Posts the request to a given master, either primary or secondary.
    void PostToMaster(NRpc::IServiceContextPtr context, TCellTag cellTag);

};

////////////////////////////////////////////////////////////////////////////////

class TNontemplateNonversionedObjectProxyBase
    : public TObjectProxyBase
{
public:
    TNontemplateNonversionedObjectProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TObjectBase* object);

protected:
    class TCustomAttributeDictionary
        : public NYTree::IAttributeDictionary
    {
    public:
        explicit TCustomAttributeDictionary(TNontemplateNonversionedObjectProxyBase* proxy);

        // IAttributeDictionary members
        virtual std::vector<TString> List() const override;
        virtual NYson::TYsonString FindYson(const TString& key) const override;
        virtual void SetYson(const TString& key, const NYson::TYsonString& value) override;
        virtual bool Remove(const TString& key) override;

    private:
        TNontemplateNonversionedObjectProxyBase* const Proxy_;

    } CustomAttributesImpl_;

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;

    virtual void ValidateRemoval();

    virtual void RemoveSelf(TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override;

    virtual TVersionedObjectId GetVersionedId() const override;
    virtual NSecurityServer::TAccessControlDescriptor* FindThisAcd() override;

};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TNonversionedObjectProxyBase
    : public TNontemplateNonversionedObjectProxyBase
{
public:
    TNonversionedObjectProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TObject* object)
        : TNontemplateNonversionedObjectProxyBase(bootstrap, metadata, object)
    { }

protected:
    const TObject* GetThisImpl() const
    {
        return Object_->As<TObject>();
    }

    TObject* GetThisImpl()
    {
        return Object_->As<TObject>();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

