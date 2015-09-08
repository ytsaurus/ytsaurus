#pragma once

#include "ypath_service.h"
#include "tree_builder.h"
#include "attributes.h"
#include "permission.h"

#include <core/misc/assert.h>

#include <core/yson/consumer.h>
#include <core/yson/writer.h>
#include <core/yson/producer.h>
#include <core/yson/forwarding_consumer.h>

#include <core/ytree/node.h>
#include <core/ytree/ypath.pb.h>

#include <core/logging/log.h>

#include <core/rpc/service_detail.h>

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
        options.HeavyResponse = true; \
        options.ResponseCodec = NCompression::ECodec::Lz4; \
        method##Thunk(context, options); \
        return true; \
    }

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceBase
    : public virtual IYPathService
{
public:
    virtual void Invoke(NRpc::IServiceContextPtr context) override;
    virtual TResolveResult Resolve(const TYPath& path, NRpc::IServiceContextPtr context) override;
    virtual NLogging::TLogger GetLogger() const override;
    virtual void WriteAttributesFragment(
        NYson::IAsyncYsonConsumer* consumer,
        const TAttributeFilter& filter,
        bool sortKeys) override;

protected:
    mutable NLogging::TLogger Logger;
    mutable bool LoggerCreated_ = false;


    virtual void BeforeInvoke(NRpc::IServiceContextPtr context);
    virtual bool DoInvoke(NRpc::IServiceContextPtr context);
    virtual void AfterInvoke(NRpc::IServiceContextPtr context);

    void EnsureLoggerCreated() const;
    virtual bool IsLoggingEnabled() const;
    virtual NLogging::TLogger CreateLogger() const;

    virtual TResolveResult ResolveSelf(const TYPath& path, NRpc::IServiceContextPtr context);
    virtual TResolveResult ResolveAttributes(const TYPath& path, NRpc::IServiceContextPtr context);
    virtual TResolveResult ResolveRecursive(const TYPath& path, NRpc::IServiceContextPtr context);

};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SUPPORTS_METHOD(method, base) \
    class TSupports##method \
        : public base \
    { \
    protected: \
        DECLARE_YPATH_SERVICE_METHOD(NProto, method); \
        virtual void method##Self(TReq##method* request, TRsp##method* response, TCtx##method##Ptr context); \
        virtual void method##Recursive(const TYPath& path, TReq##method* request, TRsp##method* response, TCtx##method##Ptr context); \
        virtual void method##Attribute(const TYPath& path, TReq##method* request, TRsp##method* response, TCtx##method##Ptr context); \
    }

class TSupportsExistsBase
    : public virtual TRefCounted
{
protected:
    typedef NRpc::TTypedServiceContext<NProto::TReqExists, NProto::TRspExists> TCtxExists;
    typedef TIntrusivePtr<TCtxExists> TCtxExistsPtr;

    void Reply(TCtxExistsPtr context, bool value);

};

DECLARE_SUPPORTS_METHOD(GetKey, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Get, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Set, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(List, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Remove, virtual TRefCounted);
DECLARE_SUPPORTS_METHOD(Exists, TSupportsExistsBase);

#undef DECLARE_SUPPORTS_VERB

////////////////////////////////////////////////////////////////////////////////

class TSupportsPermissions
{
protected:
    virtual ~TSupportsPermissions();

    virtual void ValidatePermission(
        EPermissionCheckScope scope,
        EPermission permission);

    class TCachingPermissionValidator
    {
    public:
        TCachingPermissionValidator(
            TSupportsPermissions* owner,
            EPermissionCheckScope scope);

        void Validate(EPermission permission);

    private:
        TSupportsPermissions* const Owner_;
        const EPermissionCheckScope Scope_;

        EPermissionSet ValidatedPermissions_ = NonePermissions;

    };

};

////////////////////////////////////////////////////////////////////////////////

class TSupportsAttributes
    : public virtual TYPathServiceBase
    , public virtual TSupportsGet
    , public virtual TSupportsList
    , public virtual TSupportsSet
    , public virtual TSupportsRemove
    , public virtual TSupportsExists
    , public virtual TSupportsPermissions
{
protected:
    //! Can be |nullptr|.
    virtual IAttributeDictionary* GetCustomAttributes();

    //! Can be |nullptr|.
    virtual ISystemAttributeProvider* GetBuiltinAttributeProvider();

    virtual TResolveResult ResolveAttributes(
        const NYPath::TYPath& path,
        NRpc::IServiceContextPtr context) override;

    virtual void GetAttribute(
        const TYPath& path,
        TReqGet* request,
        TRspGet* response,
        TCtxGetPtr context) override;

    virtual void ListAttribute(
        const TYPath& path,
        TReqList* request,
        TRspList* response,
        TCtxListPtr context) override;

    virtual void ExistsAttribute(
        const TYPath& path,
        TReqExists* request,
        TRspExists* response,
        TCtxExistsPtr context) override;

    virtual void SetAttribute(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        TCtxSetPtr context) override;

    virtual void RemoveAttribute(
        const TYPath& path,
        TReqRemove* request,
        TRspRemove* response,
        TCtxRemovePtr context) override;

private:
    TFuture<NYson::TYsonString> DoFindAttribute(const Stroka& key);

    static NYson::TYsonString DoGetAttributeFragment(
        const Stroka& key,
        const TYPath& path,
        const NYson::TYsonString& wholeYson);
    TFuture<NYson::TYsonString> DoGetAttribute(const TYPath& path);

    static bool DoExistsAttributeFragment(
        const Stroka& key,
        const TYPath& path,
        const TErrorOr<NYson::TYsonString>& wholeYsonOrError);
    TFuture<bool> DoExistsAttribute(const TYPath& path);

    static NYson::TYsonString DoListAttributeFragment(
        const Stroka& key,
        const TYPath& path,
        const NYson::TYsonString& wholeYson);
    TFuture<NYson::TYsonString> DoListAttribute(const TYPath& path);

    TFuture<void> DoSetAttribute(const TYPath& path, const NYson::TYsonString& newYson);

    TFuture<void> DoRemoveAttribute(const TYPath& path);

    TFuture<void> GuardedSetBuiltinAttribute(const Stroka& key, const NYson::TYsonString& value);
    TFuture<void> GuardedRemoveBuiltinAttribute(const Stroka& key);

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

    virtual void OnMyStringScalar(const TStringBuf& value) override;
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

    const INodeFactoryPtr NodeFactory_;

    std::unique_ptr<TAttributesSetter> AttributesSetter_;

};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TNodeSetter
{ };

#define DECLARE_SCALAR_TYPE(name, type) \
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
        } \
        \
        virtual void OnMy##name##Scalar(NDetail::TScalarTypeTraits<type>::TConsumerType value) override \
        { \
            Node_->SetValue(type(value)); \
        } \
    }

DECLARE_SCALAR_TYPE(String, Stroka);
DECLARE_SCALAR_TYPE(Int64,  i64);
DECLARE_SCALAR_TYPE(Uint64,  ui64);
DECLARE_SCALAR_TYPE(Double, double);
DECLARE_SCALAR_TYPE(Boolean, bool);

#undef DECLARE_SCALAR_TYPE

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

    Stroka ItemKey_;


    virtual ENodeType GetExpectedType() override
    {
        return ENodeType::Map;
    }

    virtual void OnMyBeginMap() override
    {
        Map_->Clear();
    }

    virtual void OnMyKeyedItem(const TStringBuf& key) override
    {
        ItemKey_ = key;
        TreeBuilder_->BeginTree();
        Forward(TreeBuilder_, BIND(&TNodeSetter::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        YCHECK(Map_->AddChild(TreeBuilder_->EndTree(), ItemKey_));
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
        Forward(TreeBuilder_, BIND(&TNodeSetter::OnForwardingFinished, this));
    }

    void OnForwardingFinished()
    {
        List_->AddChild(TreeBuilder_->EndTree());
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
    const NLogging::TLogger& logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

NRpc::IServiceContextPtr CreateYPathContext(
    std::unique_ptr<NRpc::NProto::TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    const NLogging::TLogger& logger = NLogging::TLogger(),
    NLogging::ELogLevel logLevel = NLogging::ELogLevel::Debug);

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
