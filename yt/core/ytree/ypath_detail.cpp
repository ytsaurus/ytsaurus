#include "stdafx.h"
#include "ypath_detail.h"
#include "ypath_client.h"
#include "node_detail.h"

#include <core/ytree/convert.h>
#include <core/ytree/node.h>
#include <core/ytree/attribute_helpers.h>
#include <core/ytree/system_attribute_provider.h>

#include <core/ypath/tokenizer.h>

#include <core/rpc/rpc.pb.h>
#include <core/rpc/server_detail.h>
#include <core/rpc/message.h>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NYPath;
using namespace NRpc::NProto;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TYPathServiceBase::Resolve(
    const TYPath& path,
    IServiceContextPtr context)
{
    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::EndOfStream:
            return ResolveSelf(tokenizer.GetSuffix(), context);

        case NYPath::ETokenType::Slash: {
            if (tokenizer.Advance() == NYPath::ETokenType::At) {
                return ResolveAttributes(tokenizer.GetSuffix(), context);
            } else {
                return ResolveRecursive(tokenizer.GetInput(), context);
            }
        }

        default:
            tokenizer.ThrowUnexpected();
            YUNREACHABLE();
    }
}

IYPathService::TResolveResult TYPathServiceBase::ResolveSelf(
    const TYPath& path,
    IServiceContextPtr /*context*/)
{
    return TResolveResult::Here(path);
}

IYPathService::TResolveResult TYPathServiceBase::ResolveAttributes(
    const TYPath& /*path*/,
    IServiceContextPtr /*context*/)
{
    THROW_ERROR_EXCEPTION("Object cannot have attributes");
}

IYPathService::TResolveResult TYPathServiceBase::ResolveRecursive(
    const TYPath& /*path*/,
    IServiceContextPtr /*context*/)
{
    THROW_ERROR_EXCEPTION("Object cannot have children");
}

void TYPathServiceBase::EnsureLoggerCreated() const
{
    if (!LoggerCreated_) {
        if (IsLoggingEnabled()) {
            Logger = CreateLogger();
        }
        LoggerCreated_ = true;
    }
}

bool TYPathServiceBase::IsLoggingEnabled() const
{
    // Logging is enabled by default...
    return true;
}

NLogging::TLogger TYPathServiceBase::CreateLogger() const
{
    // ... but a null logger is returned :)
    return NLogging::TLogger();
}

void TYPathServiceBase::Invoke(IServiceContextPtr context)
{
    TError error;
    try {
        BeforeInvoke(context);
        if (!DoInvoke(context)) {
            ThrowMethodNotSupported(context->GetMethod());
        }
    } catch (const std::exception& ex) {
        error = ex;
    }

    AfterInvoke(context);

    if (!error.IsOK()) {
        context->Reply(error);
    }
}

void TYPathServiceBase::BeforeInvoke(IServiceContextPtr /*context*/)
{
    EnsureLoggerCreated();
}

bool TYPathServiceBase::DoInvoke(IServiceContextPtr /*context*/)
{
    return false;
}

void TYPathServiceBase::AfterInvoke(IServiceContextPtr /*context*/)
{ }

NLogging::TLogger TYPathServiceBase::GetLogger() const
{
    EnsureLoggerCreated();
    return Logger;
}

void TYPathServiceBase::SerializeAttributes(
    IYsonConsumer* /*consumer*/,
    const TAttributeFilter& /*filter*/,
    bool /*sortKeys*/)
{ }

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_SUPPORTS_VERB_RESOLVE(method, onPathError) \
    DEFINE_RPC_SERVICE_METHOD(TSupports##method, method) \
    { \
        NYPath::TTokenizer tokenizer(GetRequestYPath(context)); \
        switch (tokenizer.Advance()) { \
            case NYPath::ETokenType::EndOfStream: \
                method##Self(request, response, context); \
                break; \
            \
            case NYPath::ETokenType::Slash: \
                if (tokenizer.Advance() == NYPath::ETokenType::At) { \
                    method##Attribute(tokenizer.GetSuffix(), request, response, context); \
                } else { \
                    method##Recursive(tokenizer.GetInput(), request, response, context); \
                } \
                break; \
            \
            default: \
                onPathError \
        } \
    }

#define IMPLEMENT_SUPPORTS_VERB(method) \
    IMPLEMENT_SUPPORTS_VERB_RESOLVE( \
        method, \
        { \
            tokenizer.ThrowUnexpected(); \
            YUNREACHABLE(); \
        } \
    ) \
    \
    void TSupports##method::method##Attribute(const TYPath& path, TReq##method* request, TRsp##method* response, TCtx##method##Ptr context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        ThrowMethodNotSupported(context->GetMethod(), Stroka("attribute")); \
    } \
    \
    void TSupports##method::method##Self(TReq##method* request, TRsp##method* response, TCtx##method##Ptr context) \
    { \
        UNUSED(request); \
        UNUSED(response); \
        ThrowMethodNotSupported(context->GetMethod(), Stroka("self")); \
    } \
    \
    void TSupports##method::method##Recursive(const TYPath& path, TReq##method* request, TRsp##method* response, TCtx##method##Ptr context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        ThrowMethodNotSupported(context->GetMethod(), Stroka("recursive")); \
    }

IMPLEMENT_SUPPORTS_VERB(GetKey)
IMPLEMENT_SUPPORTS_VERB(Get)
IMPLEMENT_SUPPORTS_VERB(Set)
IMPLEMENT_SUPPORTS_VERB(List)
IMPLEMENT_SUPPORTS_VERB(Remove)

IMPLEMENT_SUPPORTS_VERB_RESOLVE(
    Exists,
    {
        Reply(context, false);
    })

#undef IMPLEMENT_SUPPORTS_VERB
#undef IMPLEMENT_SUPPORTS_VERB_RESOLVE

void TSupportsExistsBase::Reply(TCtxExistsPtr context, bool value)
{
    context->Response().set_value(value);
    context->SetResponseInfo("Result: %v", value);
    context->Reply();
}

void TSupportsExists::ExistsAttribute(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    TCtxExistsPtr context)
{
    context->SetRequestInfo();

    Reply(context, false);
}

void TSupportsExists::ExistsSelf(
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    TCtxExistsPtr context)
{
    context->SetRequestInfo();

    Reply(context, true);
}

void TSupportsExists::ExistsRecursive(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    TCtxExistsPtr context)
{
    context->SetRequestInfo();

    Reply(context, false);
}

////////////////////////////////////////////////////////////////////////////////

TSupportsPermissions::~TSupportsPermissions()
{ }

void TSupportsPermissions::ValidatePermission(
    EPermissionCheckScope /*scope*/,
    EPermission /*permission*/)
{ }

////////////////////////////////////////////////////////////////////////////////

TSupportsPermissions::TCachingPermissionValidator::TCachingPermissionValidator(
    TSupportsPermissions* owner,
    EPermissionCheckScope scope)
    : Owner_(owner)
    , Scope_(scope)
{ }

void TSupportsPermissions::TCachingPermissionValidator::Validate(EPermission permission)
{
    if (None(ValidatedPermissions_ & permission)) {
        Owner_->ValidatePermission(Scope_, permission);
        ValidatedPermissions_ |= permission;
    }
}

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TSupportsAttributes::ResolveAttributes(
    const TYPath& path,
    IServiceContextPtr context)
{
    const auto& method = context->GetMethod();
    if (method != "Get" &&
        method != "Set" &&
        method != "List" &&
        method != "Remove" &&
        method != "Exists")
    {
        ThrowMethodNotSupported(method);
    }

    return TResolveResult::Here("/@" + path);
}

TFuture<TYsonString> TSupportsAttributes::DoFindAttribute(const Stroka& key)
{
    auto* customAttributes = GetCustomAttributes();
    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    if (customAttributes) {
        auto attribute = customAttributes->FindYson(key);
        if (attribute) {
            return MakeFuture(*attribute);
        }
    }

    if (builtinAttributeProvider) {
        TStringStream syncStream;
        TYsonWriter syncWriter(&syncStream);
        if (builtinAttributeProvider->GetBuiltinAttribute(key, &syncWriter)) {
            TYsonString builtinYson(syncStream.Str());
            return MakeFuture(builtinYson);
        }

        auto onAsyncAttribute = [] (TStringStream* stream, TYsonWriter* /*writer*/) {
            return TYsonString(stream->Str());
        };

        std::unique_ptr<TStringStream> asyncStream(new TStringStream());
        std::unique_ptr<TYsonWriter> asyncWriter(new TYsonWriter(asyncStream.get()));
        auto asyncResult = builtinAttributeProvider->GetBuiltinAttributeAsync(key, asyncWriter.get());
        if (asyncResult) {
            return asyncResult.Apply(BIND(
                onAsyncAttribute,
                Owned(asyncStream.release()),
                Owned(asyncWriter.release())));
        }
    }

    return Null;
}

TYsonString TSupportsAttributes::DoGetAttributeFragment(const TYPath& path, const TYsonString& wholeYson)
{
    auto node = ConvertToNode<TYsonString>(wholeYson);
    return SyncYPathGet(node, path, TAttributeFilter::All);
}

TFuture<TYsonString> TSupportsAttributes::DoGetAttribute(const TYPath& path)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        TStringStream stream;
        TYsonWriter writer(&stream);

        writer.OnBeginMap();

        if (builtinAttributeProvider) {
            std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinDescriptors;
            builtinAttributeProvider->ListBuiltinAttributes(&builtinDescriptors);
            for (const auto& descriptor : builtinDescriptors) {
                if (descriptor.Present) {
                    writer.OnKeyedItem(descriptor.Key);
                    if (descriptor.Opaque) {
                        writer.OnEntity();
                    } else {
                        YCHECK(builtinAttributeProvider->GetBuiltinAttribute(descriptor.Key, &writer));
                    }
                }
            }
        }

        auto* customAttributes = GetCustomAttributes();
        if (customAttributes) {
            for (const auto& key : customAttributes->List()) {
                writer.OnKeyedItem(key);
                Consume(customAttributes->GetYson(key), &writer);
            }
        }

        writer.OnEndMap();
        TYsonString yson(stream.Str());
        return MakeFuture(yson);
    } else {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        auto asyncYson = DoFindAttribute(key);
        if (!asyncYson) {
            return MakeFuture<TYsonString>(TError(
                NYTree::EErrorCode::ResolveError,
                "Attribute %Qv is not found",
                ToYPathLiteral(key)));
        }

        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            return asyncYson;
        }

        auto suffixPath = tokenizer.GetInput();
        return asyncYson.Apply(BIND(&TSupportsAttributes::DoGetAttributeFragment, suffixPath));
   }
}

void TSupportsAttributes::GetAttribute(
    const TYPath& path,
    TReqGet* /*request*/,
    TRspGet* response,
    TCtxGetPtr context)
{
    DoGetAttribute(path).Subscribe(BIND([=] (const TErrorOr<TYsonString>& ysonOrError) {
        if (!ysonOrError.IsOK()) {
            context->Reply(ysonOrError);
            return;
        }
        response->set_value(ysonOrError.Value().Data());
        context->Reply();
    }));
}

TYsonString TSupportsAttributes::DoListAttributeFragment(const TYPath& path, const TYsonString& wholeYson)
{
    auto node = ConvertToNode(wholeYson);
    auto listedKeys = SyncYPathList(node, path);

    TStringStream stream;
    TYsonWriter writer(&stream);
    writer.OnBeginList();
    for (const auto& listedKey : listedKeys) {
        writer.OnListItem();
        writer.OnStringScalar(listedKey);
    }
    writer.OnEndList();

    return TYsonString(stream.Str());
}

TFuture<TYsonString> TSupportsAttributes::DoListAttribute(const TYPath& path)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        TStringStream stream;
        TYsonWriter writer(&stream);
        writer.OnBeginList();

        auto* customAttributes = GetCustomAttributes();
        if (customAttributes) {
            auto userKeys = customAttributes->List();
            for (const auto& key : userKeys) {
                writer.OnListItem();
                writer.OnStringScalar(key);
            }
        }

        auto* builtinAttributeProvider = GetBuiltinAttributeProvider();
        if (builtinAttributeProvider) {
            std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinDescriptors;
            builtinAttributeProvider->ListBuiltinAttributes(&builtinDescriptors);
            for (const auto& descriptor : builtinDescriptors) {
                if (descriptor.Present) {
                    writer.OnListItem();
                    writer.OnStringScalar(descriptor.Key);
                }
            }
        }

        writer.OnEndList();

        TYsonString yson(stream.Str());
        return MakeFuture(TErrorOr<TYsonString>(yson));
    } else  {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        auto asyncYson = DoFindAttribute(key);
        if (!asyncYson) {
            return MakeFuture(TErrorOr<TYsonString>(TError(
                NYTree::EErrorCode::ResolveError,
                "Attribute %Qv is not found",
                ToYPathLiteral(key))));
        }

        auto pathSuffix = tokenizer.GetSuffix();
        return asyncYson.Apply(BIND(&TSupportsAttributes::DoListAttributeFragment, pathSuffix));
    }
}

void TSupportsAttributes::ListAttribute(
    const TYPath& path,
    TReqList* /*request*/,
    TRspList* response,
    TCtxListPtr context)
{
    DoListAttribute(path).Subscribe(BIND([=] (const TErrorOr<TYsonString>& ysonOrError) {
        if (ysonOrError.IsOK()) {
            response->set_keys(ysonOrError.Value().Data());
            context->Reply();
        } else {
            context->Reply(ysonOrError);
        }
    }));
}

bool TSupportsAttributes::DoExistsAttributeFragment(
    const TYPath& path,
    const TErrorOr<TYsonString>& wholeYsonOrError)
{
    if (!wholeYsonOrError.IsOK()) {
        return false;
    }
    auto node = ConvertToNode<TYsonString>(wholeYsonOrError.Value());
    try {
        return SyncYPathExists(node, path);
    } catch (const std::exception&) {
        return false;
    }
}

TFuture<bool> TSupportsAttributes::DoExistsAttribute(const TYPath& path)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    NYPath::TTokenizer tokenizer(path);
    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        return TrueFuture;
    }

    tokenizer.Expect(NYPath::ETokenType::Literal);
    auto key = tokenizer.GetLiteralValue();

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        auto* customAttributes = GetCustomAttributes();
        if (customAttributes && customAttributes->FindYson(key)) {
            return TrueFuture;
        }

        auto* builtinAttributeProvider = GetBuiltinAttributeProvider();
        if (builtinAttributeProvider) {
            std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinDescriptors;
            builtinAttributeProvider->ListBuiltinAttributes(&builtinDescriptors);
            for (const auto& descriptor : builtinDescriptors) {
                if (descriptor.Key == key && descriptor.Present) {
                    return TrueFuture;
                }
            }
        }

        return FalseFuture;
    } else {
        auto asyncYson = DoFindAttribute(key);
        if (!asyncYson) {
            return FalseFuture;
        }

        auto pathSuffix = tokenizer.GetInput();
        return asyncYson.Apply(BIND(&TSupportsAttributes::DoExistsAttributeFragment, pathSuffix));
    }
}

void TSupportsAttributes::ExistsAttribute(
    const TYPath& path,
    TReqExists* /*request*/,
    TRspExists* response,
    TCtxExistsPtr context)
{
    context->SetRequestInfo();

    DoExistsAttribute(path).Subscribe(BIND([=] (const TErrorOr<bool>& result) {
        if (!result.IsOK()) {
            context->Reply(result);
            return;
        }
        bool exists = result.Value();
        response->set_value(exists);
        context->SetResponseInfo("Result: %v", exists);
        context->Reply();
    }));
}

void TSupportsAttributes::DoSetAttribute(const TYPath& path, const TYsonString& newYson)
{
    TCachingPermissionValidator permissionValidator(this, EPermissionCheckScope::This);

    auto* customAttributes = GetCustomAttributes();
    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        auto newAttributes = ConvertToAttributes(newYson);

        if (builtinAttributeProvider) {
            std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinDescriptors;
            builtinAttributeProvider->ListBuiltinAttributes(&builtinDescriptors);

            for (const auto& descriptor : builtinDescriptors) {
                permissionValidator.Validate(descriptor.WritePermission);
                Stroka key(descriptor.Key);
                auto newAttributeYson = newAttributes->FindYson(key);
                if (newAttributeYson) {
                    if (!descriptor.Present) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }
                    GuardedSetBuiltinAttribute(key, *newAttributeYson);
                    YCHECK(newAttributes->Remove(key));
                } else {
                    if (descriptor.Present && descriptor.Removable) {
                        GuardedRemoveBuiltinAttribute(key);
                    }
                }
            }
        }

        auto newCustomKeys = newAttributes->List();
        std::sort(newCustomKeys.begin(), newCustomKeys.end());

        if (!customAttributes) {
             if (!newCustomKeys.empty()) {
                 THROW_ERROR_EXCEPTION("Custom attributes are not supported");
             }
             return;
        }

        auto oldCustomKeys = customAttributes->List();
        std::sort(oldCustomKeys.begin(), oldCustomKeys.end());

        for (const auto& key : newCustomKeys) {
            permissionValidator.Validate(EPermission::Write);
            auto value = newAttributes->GetYson(key);
            customAttributes->SetYson(key, value);
        }

        for (const auto& key : oldCustomKeys) {
            if (!newAttributes->FindYson(key)) {
                permissionValidator.Validate(EPermission::Write);
                customAttributes->Remove(key);
            }
        }
    } else {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        if (key.Empty()) {
            THROW_ERROR_EXCEPTION("Attribute key cannot be empty");
        }

        TNullable<ISystemAttributeProvider::TAttributeDescriptor> descriptor;
        if (builtinAttributeProvider) {
            descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(key);
        }

        if (descriptor) {
            permissionValidator.Validate(descriptor->WritePermission);

            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                GuardedSetBuiltinAttribute(key, newYson);
            } else {
                TStringStream stream;
                TYsonWriter writer(&stream);
                if (!builtinAttributeProvider->GetBuiltinAttribute(key, &writer)) {
                    ThrowNoSuchBuiltinAttribute(key);
                }

                TYsonString oldWholeYson(stream.Str());
                auto wholeNode = ConvertToNode(oldWholeYson);
                SyncYPathSet(wholeNode, tokenizer.GetInput(), newYson);
                auto newWholeYson = ConvertToYsonStringStable(wholeNode);

                GuardedSetBuiltinAttribute(key, newWholeYson);
            }
        } else {
            if (!customAttributes) {
                THROW_ERROR_EXCEPTION("Custom attributes are not supported");
            }

            permissionValidator.Validate(EPermission::Write);

            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                customAttributes->SetYson(key, newYson);
            } else {
                auto oldWholeYson = customAttributes->FindYson(key);
                if (!oldWholeYson) {
                    ThrowNoSuchCustomAttribute(key);
                }

                auto wholeNode = ConvertToNode(oldWholeYson.Get());
                SyncYPathSet(wholeNode, tokenizer.GetInput(), newYson);
                auto newWholeYson = ConvertToYsonStringStable(wholeNode);

                customAttributes->SetYson(key, newWholeYson);
            }
        }
    }

    OnCustomAttributesUpdated();
}

void TSupportsAttributes::SetAttribute(
    const TYPath& path,
    TReqSet* request,
    TRspSet* /*response*/,
    TCtxSetPtr context)
{
    context->SetRequestInfo();

    DoSetAttribute(path, TYsonString(request->value()));
    
    context->Reply();
}

void TSupportsAttributes::DoRemoveAttribute(const TYPath& path)
{
    TCachingPermissionValidator permissionValidator(this, EPermissionCheckScope::This);
    
    auto* customAttributes = GetCustomAttributes();
    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::Asterisk: {
            if (builtinAttributeProvider) {
                std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinDescriptors;
                builtinAttributeProvider->ListBuiltinAttributes(&builtinDescriptors);

                for (const auto& descriptor : builtinDescriptors) {
                    if (descriptor.Present && descriptor.Removable) {
                        permissionValidator.Validate(descriptor.WritePermission);
                        Stroka key(descriptor.Key);
                        GuardedRemoveBuiltinAttribute(key);
                    }
                }
            }

            if (customAttributes) {
                auto customKeys = customAttributes->List();
                std::sort(customKeys.begin(), customKeys.end());
                for (const auto& key : customKeys) {
                    permissionValidator.Validate(EPermission::Write);
                    YCHECK(customAttributes->Remove(key));
                }
            }
            break;
        }

        case NYPath::ETokenType::Literal: {
            auto key = tokenizer.GetLiteralValue();
            auto customYson = customAttributes ? customAttributes->FindYson(key) : TNullable<TYsonString>(Null);
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                if (customYson) {
                    permissionValidator.Validate(EPermission::Write);
                    YCHECK(customAttributes->Remove(key));
                } else {
                    if (!builtinAttributeProvider) {
                        ThrowNoSuchCustomAttribute(key);
                    }

                    auto descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(key);
                    if (!descriptor || !descriptor->Present) {
                        ThrowNoSuchCustomAttribute(key);
                    }

                    permissionValidator.Validate(descriptor->WritePermission);
                    GuardedRemoveBuiltinAttribute(key);
                }
            } else {
                if (customYson) {
                    permissionValidator.Validate(EPermission::Write);
                    auto customNode = ConvertToNode(customYson);
                    SyncYPathRemove(customNode, tokenizer.GetInput());
                    auto updatedCustomYson = ConvertToYsonStringStable(customNode);
                    customAttributes->SetYson(key, updatedCustomYson);
                } else {
                    if (!builtinAttributeProvider) {
                        ThrowNoSuchBuiltinAttribute(key);
                    }

                    auto descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(key);
                    if (!descriptor) {
                        ThrowNoSuchBuiltinAttribute(key);
                    }

                    permissionValidator.Validate(descriptor->WritePermission);
            
                    TStringStream stream;
                    TYsonWriter writer(&stream);
                    YCHECK(builtinAttributeProvider->GetBuiltinAttribute(key, &writer));

                    TYsonString builtinYson(stream.Str());
                    auto builtinNode = ConvertToNode(builtinYson);
                    SyncYPathRemove(builtinNode, tokenizer.GetInput());
                    auto updatedSystemYson = ConvertToYsonStringStable(builtinNode);

                    GuardedSetBuiltinAttribute(key, updatedSystemYson);
                }
            }
            break;
        }

        default:
            tokenizer.ThrowUnexpected();
            break;
    }

    OnCustomAttributesUpdated();
}

void TSupportsAttributes::RemoveAttribute(
    const TYPath& path,
    TReqRemove* /*request*/,
    TRspRemove* /*response*/,
    TCtxRemovePtr context)
{
    context->SetRequestInfo();

    DoRemoveAttribute(path);

    context->Reply();
}

void TSupportsAttributes::ValidateCustomAttributeUpdate(
    const Stroka& /*key*/,
    const TNullable<TYsonString>& /*oldValue*/,
    const TNullable<TYsonString>& /*newValue*/)
{ }

void TSupportsAttributes::OnCustomAttributesUpdated()
{ }

IAttributeDictionary* TSupportsAttributes::GetCustomAttributes()
{
    return nullptr;
}

ISystemAttributeProvider* TSupportsAttributes::GetBuiltinAttributeProvider()
{
    return nullptr;
}

void TSupportsAttributes::GuardedSetBuiltinAttribute(const Stroka& key, const TYsonString& yson)
{
    bool result;
    try {
        result = GetBuiltinAttributeProvider()->SetBuiltinAttribute(key, yson);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error setting builtin attribute %Qv",
            ToYPathLiteral(key))
            << ex;
    }

    if (!result) {
        ThrowCannotSetBuiltinAttribute(key);
    }
}

void TSupportsAttributes::GuardedRemoveBuiltinAttribute(const Stroka& key)
{
    bool result;
    try {
        result = GetBuiltinAttributeProvider()->RemoveBuiltinAttribute(key);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error removing builtin attribute %Qv",
            ToYPathLiteral(key))
            << ex;
    }

    if (!result) {
        ThrowCannotRemoveAttribute(key);
    }
}

void TSupportsAttributes::GuardedValidateCustomAttributeUpdate(
    const Stroka& key,
    const TNullable<TYsonString>& oldValue,
    const TNullable<TYsonString>& newValue)
{
    try {
        ValidateCustomAttributeUpdate(key, oldValue, newValue);
    } catch (const std::exception& ex) {
        if (newValue) {
            THROW_ERROR_EXCEPTION("Error setting custom attribute %Qv",
                ToYPathLiteral(key))
                << ex;
        } else {
            THROW_ERROR_EXCEPTION("Error removing custom attribute %Qv",
                ToYPathLiteral(key))
                << ex;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase::TAttributesSetter
    : public TForwardingYsonConsumer
{
public:
    explicit TAttributesSetter(IAttributeDictionary* attributes)
        : Attributes_(attributes)
    { }

private:
    IAttributeDictionary* const Attributes_;

    TStringStream AttributeStream_;
    std::unique_ptr<TYsonWriter> AttributeWriter_;


    virtual void OnMyKeyedItem(const TStringBuf& key) override
    {
        Stroka keyString(key);
        AttributeWriter_.reset(new TYsonWriter(&AttributeStream_));
        Forward(
            AttributeWriter_.get(),
            BIND ([=] () {
                AttributeWriter_.reset();
                Attributes_->SetYson(keyString, TYsonString(AttributeStream_.Str()));
                AttributeStream_.clear();
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INode* node, ITreeBuilder* builder)
    : Node_(node)
    , TreeBuilder_(builder)
    , NodeFactory_(node->CreateFactory())
{
    Node_->MutableAttributes()->Clear();
}

TNodeSetterBase::~TNodeSetterBase()
{ }

void TNodeSetterBase::ThrowInvalidType(ENodeType actualType)
{
    THROW_ERROR_EXCEPTION("Invalid node type: expected %Qlv, actual %Qlv",
        GetExpectedType(),
        actualType);
}

void TNodeSetterBase::OnMyStringScalar(const TStringBuf& /*value*/)
{
    ThrowInvalidType(ENodeType::String);
}

void TNodeSetterBase::OnMyInt64Scalar(i64 /*value*/)
{
    ThrowInvalidType(ENodeType::Int64);
}

void TNodeSetterBase::OnMyUint64Scalar(ui64 /*value*/)
{
    ThrowInvalidType(ENodeType::Uint64);
}

void TNodeSetterBase::OnMyDoubleScalar(double /*value*/)
{
    ThrowInvalidType(ENodeType::Double);
}

void TNodeSetterBase::OnMyBooleanScalar(bool /*value*/)
{
    ThrowInvalidType(ENodeType::Boolean);
}

void TNodeSetterBase::OnMyEntity()
{
    ThrowInvalidType(ENodeType::Entity);
}

void TNodeSetterBase::OnMyBeginList()
{
    ThrowInvalidType(ENodeType::List);
}

void TNodeSetterBase::OnMyBeginMap()
{
    ThrowInvalidType(ENodeType::Map);
}

void TNodeSetterBase::OnMyBeginAttributes()
{
    AttributesSetter_.reset(new TAttributesSetter(Node_->MutableAttributes()));
    Forward(AttributesSetter_.get(), TClosure(), EYsonType::MapFragment);
}

void TNodeSetterBase::OnMyEndAttributes()
{
    AttributesSetter_.reset();
}

void TNodeSetterBase::Commit()
{
    NodeFactory_->Commit();
}

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceContext
    : public TServiceContextBase
{
public:
    TYPathServiceContext(
        TSharedRefArray requestMessage,
        const NLogging::TLogger& logger,
        NLogging::ELogLevel logLevel)
        : TServiceContextBase(
            std::move(requestMessage),
            logger,
            logLevel)
    { }

    TYPathServiceContext(
        std::unique_ptr<TRequestHeader> requestHeader,
        TSharedRefArray requestMessage,
        const NLogging::TLogger& logger,
        NLogging::ELogLevel logLevel)
        : TServiceContextBase(
            std::move(requestHeader),
            std::move(requestMessage),
            logger,
            logLevel)
    { }

protected:
    virtual void DoReply() override
    { }

    virtual void LogRequest() override
    {
        TStringBuilder builder;

        auto mutationId = GetMutationId(*RequestHeader_);
        if (mutationId != NullMutationId) {
            AppendInfo(&builder, "MutationId: %v", mutationId);
        }

        AppendInfo(&builder, "Retry: %v", IsRetry());

        if (!RequestInfo_.empty()) {
            AppendInfo(&builder, "%v", RequestInfo_);
        }

        LOG_DEBUG("%v:%v %v <- %v",
            GetService(),
            GetMethod(),
            GetRequestYPath(this),
            builder.Flush());
    }

    virtual void LogResponse(const TError& error) override
    {
        TStringBuilder builder;

        AppendInfo(&builder, "Error: %v", error);

        if (!ResponseInfo_.empty()) {
            AppendInfo(&builder, "%v", ResponseInfo_);
        }

        LOG_DEBUG("%v:%v %v -> %v",
            GetService(),
            GetMethod(),
            GetRequestYPath(this),
            builder.Flush());
    }

};

IServiceContextPtr CreateYPathContext(
    TSharedRefArray requestMessage,
    const NLogging::TLogger& logger,
    NLogging::ELogLevel logLevel)
{
    YASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestMessage),
        logger,
        logLevel);
}

IServiceContextPtr CreateYPathContext(
    std::unique_ptr<TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    const NLogging::TLogger& logger,
    NLogging::ELogLevel logLevel)
{
    YASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestHeader),
        std::move(requestMessage),
        logger,
        logLevel);
}

////////////////////////////////////////////////////////////////////////////////

class TRootService
    : public IYPathService
{
public:
    explicit TRootService(IYPathServicePtr underlyingService)
        : UnderlyingService_(underlyingService)
    { }

    virtual void Invoke(IServiceContextPtr /*context*/) override
    {
        YUNREACHABLE();
    }

    virtual TResolveResult Resolve(const
        TYPath& path,
        IServiceContextPtr /*context*/) override
    {
        NYPath::TTokenizer tokenizer(path);
        if (tokenizer.Advance() != NYPath::ETokenType::Slash) {
            THROW_ERROR_EXCEPTION("YPath must start with \"/\"");
        }

        return TResolveResult::There(UnderlyingService_, tokenizer.GetSuffix());
    }

    virtual NLogging::TLogger GetLogger() const override
    {
        return UnderlyingService_->GetLogger();
    }

    // TODO(panin): remove this when getting rid of IAttributeProvider
    virtual void SerializeAttributes(
        IYsonConsumer* consumer,
        const TAttributeFilter& filter,
        bool sortKeys) override
    {
        UnderlyingService_->SerializeAttributes(consumer, filter, sortKeys);
    }

private:
    IYPathServicePtr UnderlyingService_;

};

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService)
{
    return New<TRootService>(underlyingService);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
