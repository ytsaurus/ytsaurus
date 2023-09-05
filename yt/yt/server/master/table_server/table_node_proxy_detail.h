#pragma once

#include "table_node.h"
#include "replicated_table_node.h"

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/tablet_server/tablet_owner_proxy_base.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy
    : public NTabletServer::TTabletOwnerProxyBase
{
public:
    using TTabletOwnerProxyBase::TTabletOwnerProxyBase;

protected:
    using TBase = TTabletOwnerProxyBase;

    void GetBasicAttributes(TGetBasicAttributesContext* context) override;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
    void ValidateCustomAttributeUpdate(
        const TString& key,
        const NYson::TYsonString& oldValue,
        const NYson::TYsonString& newValue) override;
    void ValidateReadLimit(const NChunkClient::NProto::TReadLimit& context) const override;
    NTableClient::TComparator GetComparator() const override;

    bool DoInvoke(const NYTree::IYPathServiceContextPtr& context) override;

    void ValidateBeginUpload() override;
    void ValidateStorageParametersUpdate() override;
    void ValidateLockPossible() override;

    TTableNode* GetThisImpl();
    const TTableNode* GetThisImpl() const;

    TTableNode* LockThisImpl(
        const NCypressServer::TLockRequest& request = NCypressClient::ELockMode::Exclusive,
        bool recursive = false);

    NYTree::IAttributeDictionary* GetCustomAttributes() override;

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, ReshardAutomatic);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, GetMountInfo);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Alter);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, LockDynamicTable);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, CheckDynamicTableLock);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, StartBackup);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, StartRestore);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, CheckBackup);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, FinishBackup);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, FinishRestore);

private:
    TMountConfigAttributeDictionaryPtr WrappedAttributes_;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableNodeProxy
    : public TTableNodeProxy
{
public:
    using TTableNodeProxy::TTableNodeProxy;

protected:
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer


