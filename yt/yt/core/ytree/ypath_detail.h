#pragma once

#include "attributes.h"
#include "permission.h"
#include "tree_builder.h"
#include "ypath_service.h"
#include "system_attribute_provider.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/assert.h>
#include <yt/yt/core/misc/cast.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/producer.h>
#include <yt/yt/core/yson/forwarding_consumer.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_YPATH_SERVICE_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method> TCtx##method; \
    typedef ::NYT::TIntrusivePtr<TCtx##method> TCtx##method##Ptr; \
    typedef TCtx##method::TTypedRequest  TReq##method; \
    typedef TCtx##method::TTypedResponse TRsp##method; \
    \
    void method##Thunk( \
        const ::NYT::NRpc::IServiceContextPtr& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(context, options); \
        if (!typedContext->DeserializeRequest()) \
            return; \
        this->method( \
            &typedContext->Request(), \
            &typedContext->Response(), \
            typedContext); \
    } \
    \
    void method( \
        [[maybe_unused]] TReq##method* request, \
        [[maybe_unused]] TRsp##method* response, \
        [[maybe_unused]] const TCtx##method##Ptr& context)

#define DEFINE_YPATH_SERVICE_METHOD(type, method) \
    void type::method( \
        [[maybe_unused]] TReq##method* request, \
        [[maybe_unused]] TRsp##method* response, \
        [[maybe_unused]] const TCtx##method##Ptr& context)

////////////////////////////////////////////////////////////////////////////////

#define DISPATCH_YPATH_SERVICE_METHOD(method, ...) \
    if (context->GetMethod() == #method) { \
        auto options = ::NYT::NRpc::THandlerInvocationOptions() __VA_ARGS__; \
        method##Thunk(context, options); \
        return true; \
    }

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceBase
    : public virtual IYPathService
{
public:
    void Invoke(const NRpc::IServiceContextPtr& context) override;
    TResolveResult Resolve(const TYPath& path, const NRpc::IServiceContextPtr& context) override;
    void DoWriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const std::optional<std::vector<TString>>& attributeKeys,
        bool stable) override;
    bool ShouldHideAttributes() override;

protected:
    virtual void BeforeInvoke(const NRpc::IServiceContextPtr& context);
    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context);
    virtual void AfterInvoke(const NRpc::IServiceContextPtr& context);

    virtual TResolveResult ResolveSelf(const TYPath& path, const NRpc::IServiceContextPtr& context);
    virtual TResolveResult ResolveAttributes(const TYPath& path, const NRpc::IServiceContextPtr& context);
    virtual TResolveResult ResolveRecursive(const TYPath& path, const NRpc::IServiceContextPtr& context);

};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SUPPORTS_METHOD(method, base) \
    class TSupports##method \
        : public base \
    { \
    protected: \
        DECLARE_YPATH_SERVICE_METHOD(NProto, method); \
        virtual void method##Self(TReq##method* request, TRsp##method* response, const TCtx##method##Ptr& context); \
        virtual void method##Recursive(const TYPath& path, TReq##method* request, TRsp##method* response, const TCtx##method##Ptr& context); \
        virtual void method##Attribute(const TYPath& path, TReq##method* request, TRsp##method* response, const TCtx##method##Ptr& context); \
    }

class TSupportsExistsBase
    : public virtual TRefCounted
{
protected:
    typedef NRpc::TTypedServiceContext<NProto::TReqExists, NProto::TRspExists> TCtxExists;
    typedef TIntrusivePtr<TCtxExists> TCtxExistsPtr;

    void Reply(const TCtxExistsPtr& context, bool value);

};

class TSupportsMultisetAttributes
    : public virtual TRefCounted
{
protected:
    DECLARE_YPATH_SERVICE_METHOD(NProto, Multiset);
    DECLARE_YPATH_SERVICE_METHOD(NProto, MultisetAttributes);

    virtual void SetAttributes(
        const TYPath& path,
        TReqMultisetAttributes* request,
        TRspMultisetAttributes* response);

private:
    // COMPAT(gritukan) Move it to MultisetAttributes.
    void DoSetAttributes(
        const TYPath& path,
        TReqMultisetAttributes* request,
        TRspMultisetAttributes* response);
};

DECLARE_SUPPORTS_METHOD(GetKey, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Get, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Set, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(List, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Remove, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Exists, TSupportsExistsBase);

#undef DECLARE_SUPPORTS_METHOD

////////////////////////////////////////////////////////////////////////////////

class TSupportsPermissions
{
protected:
    virtual ~TSupportsPermissions() = default;

    // The last argument will be empty for contexts where authenticated user is known
    // a-priori (like in object proxies in master), otherwise it will be set to user name
    // (like in operation controller orchid).
    virtual void ValidatePermission(
        EPermissionCheckScope scope,
        EPermission permission,
        const TString& user = "");

    class TCachingPermissionValidator
    {
    public:
        TCachingPermissionValidator(
            TSupportsPermissions* owner,
            EPermissionCheckScope scope);

        void Validate(EPermission permission, const TString& user = "");

    private:
        TSupportsPermissions* const Owner_;
        const EPermissionCheckScope Scope_;

        THashMap<TString, EPermissionSet> ValidatedPermissions_;
    };

};

////////////////////////////////////////////////////////////////////////////////

class TSupportsAttributes
    : public virtual TYPathServiceBase
    , public virtual TSupportsGet
    , public virtual TSupportsList
    , public virtual TSupportsSet
    , public virtual TSupportsMultisetAttributes
    , public virtual TSupportsRemove
    , public virtual TSupportsExists
    , public virtual TSupportsPermissions
{
protected:
    TSupportsAttributes();

    IAttributeDictionary* GetCombinedAttributes();

    //! Can be |nullptr|.
    virtual IAttributeDictionary* GetCustomAttributes();

    //! Can be |nullptr|.
    virtual ISystemAttributeProvider* GetBuiltinAttributeProvider();

    TResolveResult ResolveAttributes(
        const NYPath::TYPath& path,
        const NRpc::IServiceContextPtr& context) override;

    void GetAttribute(
        const TYPath& path,
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override;

    void ListAttribute(
        const TYPath& path,
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context) override;

    void ExistsAttribute(
        const TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;

    void SetAttribute(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;

    void RemoveAttribute(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;

    void SetAttributes(
        const TYPath& path,
        TReqMultisetAttributes* request,
        TRspMultisetAttributes* response) override;

private:
    class TCombinedAttributeDictionary
        : public IAttributeDictionary
    {
    public:
        explicit TCombinedAttributeDictionary(TSupportsAttributes* owner);

        std::vector<TString> ListKeys() const override;
        std::vector<TKeyValuePair> ListPairs() const override;
        NYson::TYsonString FindYson(TStringBuf key) const override;
        void SetYson(const TString& key, const NYson::TYsonString& value) override;
        bool Remove(const TString& key) override;

    private:
        TSupportsAttributes* const Owner_;
    };

    using TCombinedAttributeDictionaryPtr = TIntrusivePtr<TCombinedAttributeDictionary>;

    TCombinedAttributeDictionaryPtr CombinedAttributes_;

    TFuture<NYson::TYsonString> DoFindAttribute(TStringBuf key);

    static NYson::TYsonString DoGetAttributeFragment(
        TStringBuf key,
        const TYPath& path,
        const NYson::TYsonString& wholeYson);
    TFuture<NYson::TYsonString> DoGetAttribute(
        const TYPath& path,
        const std::optional<std::vector<TString>>& attributeKeys);

    static bool DoExistsAttributeFragment(
        TStringBuf key,
        const TYPath& path,
        const TErrorOr<NYson::TYsonString>& wholeYsonOrError);
    TFuture<bool> DoExistsAttribute(const TYPath& path);

    static NYson::TYsonString DoListAttributeFragment(
        TStringBuf key,
        const TYPath& path,
        const NYson::TYsonString& wholeYson);
    TFuture<NYson::TYsonString> DoListAttribute(const TYPath& path);

    void DoSetAttribute(const TYPath& path, const NYson::TYsonString& newYson);
    void DoRemoveAttribute(const TYPath& path, bool force);

    bool GuardedGetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer);
    bool GuardedSetBuiltinAttribute(TInternedAttributeKey key, const NYson::TYsonString& value);
    bool GuardedRemoveBuiltinAttribute(TInternedAttributeKey key);

    void ValidateAttributeKey(TStringBuf key) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSystemBuiltinAttributeKeysCache
{
public:
    const THashSet<TInternedAttributeKey>& GetBuiltinAttributeKeys(ISystemAttributeProvider* provider);

private:
    bool Initialized_ = false;
    THashSet<TInternedAttributeKey> BuiltinKeys_;

};

////////////////////////////////////////////////////////////////////////////////

class TSystemCustomAttributeKeysCache
{
public:
    const THashSet<TString>& GetCustomAttributeKeys(ISystemAttributeProvider* provider);

private:
    bool Initialized_ = false;
    THashSet<TString> CustomKeys_;

};

////////////////////////////////////////////////////////////////////////////////

class TOpaqueAttributeKeysCache
{
public:
    const THashSet<TString>& GetOpaqueAttributeKeys(ISystemAttributeProvider* provider);

private:
    bool Initialized_ = false;
    THashSet<TString> OpaqueKeys_;

};

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase
    : public NYson::TForwardingYsonConsumer
{
public:
    void Commit();

protected:
    TNodeSetterBase(INode* node, ITreeBuilder* builder);
    ~TNodeSetterBase();

    void ThrowInvalidType(ENodeType actualType);
    virtual ENodeType GetExpectedType() = 0;

    void OnMyStringScalar(TStringBuf value) override;
    void OnMyInt64Scalar(i64 value) override;
    void OnMyUint64Scalar(ui64 value) override;
    void OnMyDoubleScalar(double value) override;
    void OnMyBooleanScalar(bool value) override;
    void OnMyEntity() override;

    void OnMyBeginList() override;

    void OnMyBeginMap() override;

    void OnMyBeginAttributes() override;
    void OnMyEndAttributes() override;

protected:
    class TAttributesSetter;

    INode* const Node_;
    ITreeBuilder* const TreeBuilder_;

    const std::unique_ptr<ITransactionalNodeFactory> NodeFactory_;

    std::unique_ptr<TAttributesSetter> AttributesSetter_;

};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TNodeSetter
{ };

#define BEGIN_SETTER(name, type) \
    template <> \
    class TNodeSetter<I##name##Node> \
        : public TNodeSetterBase \
    { \
    public: \
        TNodeSetter(I##name##Node* node, ITreeBuilder* builder) \
            : TNodeSetterBase(node, builder) \
            , Node_(node) \
        { } \
    \
    private: \
        I##name##Node* const Node_; \
        \
        virtual ENodeType GetExpectedType() override \
        { \
            return ENodeType::name; \
        }

#define END_SETTER() \
    };

BEGIN_SETTER(String, TString)
    void OnMyStringScalar(TStringBuf value) override
    {
        Node_->SetValue(TString(value));
    }
END_SETTER()

BEGIN_SETTER(Int64, i64)
    void OnMyInt64Scalar(i64 value) override
    {
        Node_->SetValue(value);
    }

    void OnMyUint64Scalar(ui64 value) override
    {
        Node_->SetValue(CheckedIntegralCast<i64>(value));
    }
END_SETTER()

BEGIN_SETTER(Uint64,  ui64)
    void OnMyInt64Scalar(i64 value) override
    {
        Node_->SetValue(CheckedIntegralCast<ui64>(value));
    }

    void OnMyUint64Scalar(ui64 value) override
    {
        Node_->SetValue(value);
    }
END_SETTER()

BEGIN_SETTER(Double, double)
    void OnMyDoubleScalar(double value) override
    {
        Node_->SetValue(value);
    }
END_SETTER()

BEGIN_SETTER(Boolean, bool)
    void OnMyBooleanScalar(bool value) override
    {
        Node_->SetValue(value);
    }
END_SETTER()

#undef BEGIN_SETTER
#undef END_SETTER

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IMapNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IMapNode* map, ITreeBuilder* builder)
        : TNodeSetterBase(map, builder)
        , Map_(map)
    { }

private:
    IMapNode* const Map_;

    TString ItemKey_;


    ENodeType GetExpectedType() override
    {
        return ENodeType::Map;
    }

    void OnMyBeginMap() override
    {
        Map_->Clear();
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        ItemKey_ = key;
        TreeBuilder_->BeginTree();
        Forward(TreeBuilder_, std::bind(&TNodeSetter::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        YT_VERIFY(Map_->AddChild(ItemKey_, TreeBuilder_->EndTree()));
        ItemKey_.clear();
    }

    void OnMyEndMap() override
    {
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IListNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IListNode* list, ITreeBuilder* builder)
        : TNodeSetterBase(list, builder)
        , List_(list)
    { }

private:
    IListNode* const List_;


    ENodeType GetExpectedType() override
    {
        return ENodeType::List;
    }

    void OnMyBeginList() override
    {
        List_->Clear();
    }

    void OnMyListItem() override
    {
        TreeBuilder_->BeginTree();
        Forward(TreeBuilder_, [this] {
            List_->AddChild(TreeBuilder_->EndTree());
        });
    }

    void OnMyEndList() override
    {
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TNodeSetter<IEntityNode>
    : public TNodeSetterBase
{
public:
    TNodeSetter(IEntityNode* entity, ITreeBuilder* builder)
        : TNodeSetterBase(entity, builder)
    { }

private:
    ENodeType GetExpectedType() override
    {
        return ENodeType::Entity;
    }

    void OnMyEntity() override
    {
        // Just do nothing.
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TNode>
void SetNodeFromProducer(
    TNode* node,
    NYson::TYsonProducer producer,
    ITreeBuilder* builder)
{
    YT_VERIFY(node);
    YT_VERIFY(builder);

    TNodeSetter<TNode> setter(node, builder);
    producer.Run(&setter);
    setter.Commit();
}

////////////////////////////////////////////////////////////////////////////////

NRpc::IServiceContextPtr CreateYPathContext(
    TSharedRefArray requestMessage,
    NLogging::TLogger logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

NRpc::IServiceContextPtr CreateYPathContext(
    std::unique_ptr<NRpc::NProto::TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    NLogging::TLogger logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
