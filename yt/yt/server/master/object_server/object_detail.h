#pragma once

#include "public.h"
#include "object.h"
#include "object_manager.h"
#include "object_proxy.h"
#include "permission_validator.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/master/security_server/security_tags.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/ytree/system_attribute_provider.h>
#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectProxyBase
    : public virtual NYTree::TSupportsAttributes
    , public virtual IObjectProxy
{
public:
    TObjectProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TObject* object);

    bool ShouldHideAttributes() override;

    // IObjectProxy members
    TObjectId GetId() const override;
    TObject* GetObject() const override;
    NTransactionServer::TTransaction* GetTransaction() const override;
    const NYTree::IAttributeDictionary& Attributes() const override;
    NYTree::IAttributeDictionary* MutableAttributes() override;

    // NB: Method marked as final to ensure that BeforeInvoke and AfterInvoke will be called.
    // Consider using aforementioned methods instead of expanding this.
    void Invoke(const NYTree::IYPathServiceContextPtr& context) override final;
    void DoWriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const NYTree::TAttributeFilter& attributeFilter,
        bool stable) override;

    // This method is made public for orchid nodes, which obtain their manifests
    // from owning node's custom attributes.
    NYTree::IAttributeDictionary* GetCustomAttributes() override;

protected:
    NCellMaster::TBootstrap* const Bootstrap_;
    TObjectTypeMetadata* const Metadata_;
    TObject* const Object_;
    bool ModificationTrackingSuppressed_ = false;
    NYTree::IAttributeDictionary* CustomAttributes_ = nullptr;

    struct TGetBasicAttributesContext
    {
        std::optional<NYTree::EPermission> Permission;
        TCellTag ExternalCellTag = NObjectClient::InvalidCellTag;
        NTransactionClient::TTransactionId ExternalTransactionId;
        std::optional<std::vector<TString>> Columns;
        bool OmitInaccessibleColumns = false;
        std::optional<std::vector<TString>> OmittedInaccessibleColumns;
        bool PopulateSecurityTags = false;
        std::optional<NSecurityServer::TSecurityTags> SecurityTags;
        NHydra::TRevision Revision = NHydra::NullRevision;
        NHydra::TRevision AttributeRevision = NHydra::NullRevision;
        NHydra::TRevision ContentRevision = NHydra::NullRevision;
    };

    virtual void SetModified(EModificationType modificationType);
    void SuppressModificationTracking();

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetBasicAttributes);
    virtual void GetBasicAttributes(TGetBasicAttributesContext* context);

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CheckPermission);

    //! Returns the full object id that coincides with #Id
    //! for non-versioned objects and additionally includes transaction id for
    //! versioned ones.
    virtual TVersionedObjectId GetVersionedId() const = 0;

    //! Returns the ACD for the object or |nullptr| is none exists.
    virtual NSecurityServer::TAccessControlDescriptor* FindThisAcd() = 0;

    void BeforeInvoke(const NYTree::IYPathServiceContextPtr& context) override;
    bool DoInvoke(const NYTree::IYPathServiceContextPtr& context) override;

    // NYTree::TSupportsAttributes members
    void SetAttribute(
        const NYTree::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;
    void RemoveAttribute(
        const NYTree::TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;
    void SetAttributes(
        const NYTree::TYPath& path,
        TReqMultisetAttributes* request,
        TRspMultisetAttributes* response,
        const TCtxMultisetAttributesPtr& context) override;

    void ReplicateAttributeUpdate(const NRpc::IServiceContextPtr& context);

    NYTree::ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    // NYTree::ISystemAttributeProvider members
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    const THashSet<NYTree::TInternedAttributeKey>& GetBuiltinAttributeKeys() override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
    bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    virtual void LogAcdUpdate(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value);

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
    void ValidatePermission(
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission,
        const TString& /*user*/ = {}) override;

    void ValidatePermission(
        TObject* object,
        NYTree::EPermission permission);

    class TPermissionValidator
        : public IPermissionValidator
    {
    public:
        explicit TPermissionValidator(const TIntrusivePtr<TObjectProxyBase>& owner)
            : Owner_(owner)
        { }

        void ValidatePermission(
            NYTree::EPermissionCheckScope scope,
            NYTree::EPermission permission,
            const TString& user = {}) override
        {
            if (auto owner = Owner_.Lock()) {
                owner->ValidatePermission(scope, permission, user);
            }
        }

        void ValidatePermission(
            TObject* object,
            NYTree::EPermission permission) override
        {
            if (auto owner = Owner_.Lock()) {
                owner->ValidatePermission(object, permission);
            }
        }

    private:
        const TWeakPtr<TObjectProxyBase> Owner_;
    };

    std::unique_ptr<IPermissionValidator> CreatePermissionValidator();

    void ValidateAnnotation(const TString& annotation);

    bool IsRecovery() const;
    bool IsLeader() const;
    bool IsFollower() const;
    void RequireLeader() const;

    bool IsPrimaryMaster() const;
    bool IsSecondaryMaster() const;

    //! Posts the request to all secondary masters.
    void PostToSecondaryMasters(NRpc::IServiceContextPtr context);

    //! Posts the request to given masters externalizing the transaction if needed.
    void ExternalizeToMasters(NRpc::IServiceContextPtr context, const TCellTagList& cellTags);

    const NCypressServer::TDynamicCypressManagerConfigPtr& GetDynamicCypressManagerConfig() const;

private:
    void ClearPrerequisiteTransactions(NRpc::IServiceContextPtr& context);
    void ValidateModifiedPermission(NSecurityServer::TAcdOverride modification);

public:
    template <class T>
    static void HandleCheckPermissionRequest(
        NCellMaster::TBootstrap* bootstrap,
        const TCtxCheckPermissionPtr& context,
        T doCheckPermission);
};

////////////////////////////////////////////////////////////////////////////////

class TNontemplateNonversionedObjectProxyBase
    : public TObjectProxyBase
{
public:
    TNontemplateNonversionedObjectProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TObject* object);

protected:
    class TCustomAttributeDictionary
        : public NYTree::IAttributeDictionary
    {
    public:
        explicit TCustomAttributeDictionary(TNontemplateNonversionedObjectProxyBase* proxy);

        // IAttributeDictionary members
        std::vector<TString> ListKeys() const override;
        std::vector<TKeyValuePair> ListPairs() const override;
        NYson::TYsonString FindYson(TStringBuf key) const override;
        void SetYson(const TString& key, const NYson::TYsonString& value) override;
        bool Remove(const TString& key) override;

    private:
        TNontemplateNonversionedObjectProxyBase* const Proxy_;
    };

    using TCustomAttributeDictionaryPtr = TIntrusivePtr<TCustomAttributeDictionary>;

    TCustomAttributeDictionaryPtr CustomAttributesImpl_;

    bool DoInvoke(const NYTree::IYPathServiceContextPtr& context) override;

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;

    virtual void ValidateRemoval();

    void RemoveSelf(TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override;

    TVersionedObjectId GetVersionedId() const override;
    NSecurityServer::TAccessControlDescriptor* FindThisAcd() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TNonversionedObjectProxyBase
    : public TNontemplateNonversionedObjectProxyBase
{
public:
    using TNontemplateNonversionedObjectProxyBase::TNontemplateNonversionedObjectProxyBase;

protected:
    template <class TActualImpl = TObject>
    const TActualImpl* GetThisImpl() const
    {
        return Object_->As<TActualImpl>();
    }

    template <class TActualImpl = TObject>
    TActualImpl* GetThisImpl()
    {
        return Object_->As<TActualImpl>();
    }

    TFuture<NYson::TYsonString> FetchFromShepherd(const NYPath::TYPath& path);

    template <class T>
    TFuture<std::vector<T>> FetchFromSwarm(NYTree::TInternedAttributeKey key);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define OBJECT_DETAIL_INL_H_
#include "object_detail-inl.h"
#undef OBJECT_DETAIL_INL_H_
