#pragma once

#include "attributes.h"
#include "permission.h"
#include "tree_builder.h"
#include "ypath_service.h"
#include "system_attribute_provider.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/assert.h>
#include <yt/core/misc/cast.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/writer.h>
#include <yt/core/yson/producer.h>
#include <yt/core/yson/forwarding_consumer.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/proto/ypath.pb.h>

namespace NYT {
namespace NYTree {

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
        TReq##method* request, \
        TRsp##method* response, \
        const TCtx##method##Ptr& context)

#define DEFINE_YPATH_SERVICE_METHOD(type, method) \
    void type::method( \
        TReq##method* request, \
        TRsp##method* response, \
        const TCtx##method##Ptr& context)

////////////////////////////////////////////////////////////////////////////////

#define DISPATCH_YPATH_SERVICE_METHOD(method) \
    if (context->GetMethod() == #method) { \
        ::NYT::NRpc::THandlerInvocationOptions options; \
        method##Thunk(context, options); \
        return true; \
    }

#define DISPATCH_YPATH_HEAVY_SERVICE_METHOD(method) \
    if (context->GetMethod() == #method) { \
        ::NYT::NRpc::THandlerInvocationOptions options; \
        options.Heavy = true; \
        options.ResponseCodec = NCompression::ECodec::Lz4; \
        method##Thunk(context, options); \
        return true; \
    }

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceBase
    : public virtual IYPathService
{
public:
    virtual void Invoke(const NRpc::IServiceContextPtr& context) override;
    virtual TResolveResult Resolve(const TYPath& path, const NRpc::IServiceContextPtr& context) override;
    virtual void DoWriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const std::optional<std::vector<TString>>& attributeKeys,
        bool stable) override;
    virtual bool ShouldHideAttributes() override;

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

class TSupportsMultiset
    : public virtual TRefCounted
{
protected:
    DECLARE_YPATH_SERVICE_METHOD(NProto, Multiset);

    virtual void SetChildren(TReqMultiset* request, TRspMultiset* response);
    virtual void SetAttributes(const TYPath& path, TReqMultiset* request, TRspMultiset* response);
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
    , public virtual TSupportsMultiset
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

    virtual TResolveResult ResolveAttributes(
        const NYPath::TYPath& path,
        const NRpc::IServiceContextPtr& context) override;

    virtual void GetAttribute(
        const TYPath& path,
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override;

    virtual void ListAttribute(
        const TYPath& path,
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context) override;

    virtual void ExistsAttribute(
        const TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;

    virtual void SetAttribute(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;

    virtual void RemoveAttribute(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;

    virtual void SetAttributes(const TYPath& path, TReqMultiset* request, TRspMultiset* response) override;

private:
    class TCombinedAttributeDictionary
        : public IAttributeDictionary
    {
    public:
        explicit TCombinedAttributeDictionary(TSupportsAttributes* owner);

        virtual std::vector<TString> List() const override;
        virtual NYson::TYsonString FindYson(const TString& key) const override;
        virtual void SetYson(const TString& key, const NYson::TYsonString& value) override;
        virtual bool Remove(const TString& key) override;

    private:
        TSupportsAttributes* const Owner_;

    };

    TCombinedAttributeDictionary CombinedAttributes_;

    TFuture<NYson::TYsonString> DoFindAttribute(const TString& key);

    static NYson::TYsonString DoGetAttributeFragment(
        const TString& key,
        const TYPath& path,
        const NYson::TYsonString& wholeYson);
    TFuture<NYson::TYsonString> DoGetAttribute(
        const TYPath& path,
        const std::optional<std::vector<TString>>& attributeKeys);

    static bool DoExistsAttributeFragment(
        const TString& key,
        const TYPath& path,
        const TErrorOr<NYson::TYsonString>& wholeYsonOrError);
    TFuture<bool> DoExistsAttribute(const TYPath& path);

    static NYson::TYsonString DoListAttributeFragment(
        const TString& key,
        const TYPath& path,
        const NYson::TYsonString& wholeYson);
    TFuture<NYson::TYsonString> DoListAttribute(const TYPath& path);

    void DoSetAttribute(const TYPath& path, const NYson::TYsonString& newYson);
    void DoRemoveAttribute(const TYPath& path, bool force);

    bool GuardedSetBuiltinAttribute(TInternedAttributeKey key, const NYson::TYsonString& value);
    bool GuardedRemoveBuiltinAttribute(TInternedAttributeKey key);

    void ValidateAttributeKey(const TString& key) const;
};

////////////////////////////////////////////////////////////////////////////////

class TBuiltinAttributeKeysCache
{
public:
    const THashSet<TInternedAttributeKey>& GetBuiltinAttributeKeys(ISystemAttributeProvider* provider);

private:
    bool Initialized_ = false;
    THashSet<TInternedAttributeKey> BuiltinKeys_;

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

    virtual void OnMyStringScalar(TStringBuf value) override;
    virtual void OnMyInt64Scalar(i64 value) override;
    virtual void OnMyUint64Scalar(ui64 value) override;
    virtual void OnMyDoubleScalar(double value) override;
    virtual void OnMyBooleanScalar(bool value) override;
    virtual void OnMyEntity() override;

    virtual void OnMyBeginList() override;

    virtual void OnMyBeginMap() override;

    virtual void OnMyBeginAttributes() override;
    virtual void OnMyEndAttributes() override;

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
    virtual void OnMyStringScalar(TStringBuf value) override
    {
        Node_->SetValue(TString(value));
    }
END_SETTER()

BEGIN_SETTER(Int64, i64)
    virtual void OnMyInt64Scalar(i64 value) override
    {
        Node_->SetValue(value);
    }

    virtual void OnMyUint64Scalar(ui64 value) override
    {
        Node_->SetValue(CheckedIntegralCast<i64>(value));
    }
END_SETTER()

BEGIN_SETTER(Uint64,  ui64)
    virtual void OnMyInt64Scalar(i64 value) override
    {
        Node_->SetValue(CheckedIntegralCast<ui64>(value));
    }

    virtual void OnMyUint64Scalar(ui64 value) override
    {
        Node_->SetValue(value);
    }
END_SETTER()

BEGIN_SETTER(Double, double)
    virtual void OnMyDoubleScalar(double value) override
    {
        Node_->SetValue(value);
    }
END_SETTER()

BEGIN_SETTER(Boolean, bool)
    virtual void OnMyBooleanScalar(bool value) override
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


    virtual ENodeType GetExpectedType() override
    {
        return ENodeType::Map;
    }

    virtual void OnMyBeginMap() override
    {
        Map_->Clear();
    }

    virtual void OnMyKeyedItem(TStringBuf key) override
    {
        ItemKey_ = key;
        TreeBuilder_->BeginTree();
        Forward(TreeBuilder_, std::bind(&TNodeSetter::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        YCHECK(Map_->AddChild(ItemKey_, TreeBuilder_->EndTree()));
        ItemKey_.clear();
    }

    virtual void OnMyEndMap() override
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


    virtual ENodeType GetExpectedType() override
    {
        return ENodeType::List;
    }

    virtual void OnMyBeginList() override
    {
        List_->Clear();
    }

    virtual void OnMyListItem() override
    {
        TreeBuilder_->BeginTree();
        Forward(TreeBuilder_, [this] {
            List_->AddChild(TreeBuilder_->EndTree());
        });
    }

    virtual void OnMyEndList() override
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
    virtual ENodeType GetExpectedType() override
    {
        return ENodeType::Entity;
    }

    virtual void OnMyEntity() override
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
    YCHECK(node);
    YCHECK(builder);

    TNodeSetter<TNode> setter(node, builder);
    producer.Run(&setter);
    setter.Commit();
}

////////////////////////////////////////////////////////////////////////////////

NRpc::IServiceContextPtr CreateYPathContext(
    TSharedRefArray requestMessage,
    NLogging::TLogger logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug,
    TString loggingInfo = TString());

NRpc::IServiceContextPtr CreateYPathContext(
    std::unique_ptr<NRpc::NProto::TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    NLogging::TLogger logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug,
    TString loggingInfo = TString());

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
