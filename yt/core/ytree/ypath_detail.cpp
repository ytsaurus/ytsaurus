#include "ypath_detail.h"
#include "node_detail.h"
#include "ypath_client.h"

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>
#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/system_attribute_provider.h>

#include <yt/core/yson/async_writer.h>
#include <yt/core/yson/attribute_consumer.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/rpc/proto/rpc.pb.h>
#include <yt/core/rpc/server_detail.h>
#include <yt/core/rpc/message.h>

#include <yt/core/bus/bus.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NYPath;
using namespace NRpc::NProto;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto NoneYsonFuture = MakeFuture(TYsonString());

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TYPathServiceBase::Resolve(
    const TYPath& path,
    const IServiceContextPtr& context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Skip(NYPath::ETokenType::Ampersand);
    if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
        return ResolveSelf(TYPath(tokenizer.GetSuffix()), context);
    }

    tokenizer.Expect(NYPath::ETokenType::Slash);

    if (tokenizer.Advance() == NYPath::ETokenType::At) {
        return ResolveAttributes(TYPath(tokenizer.GetSuffix()), context);
    } else {
        return ResolveRecursive(TYPath(tokenizer.GetInput()), context);
    }
}

IYPathService::TResolveResult TYPathServiceBase::ResolveSelf(
    const TYPath& path,
    const IServiceContextPtr& /*context*/)
{
    return TResolveResultHere{path};
}

IYPathService::TResolveResult TYPathServiceBase::ResolveAttributes(
    const TYPath& /*path*/,
    const IServiceContextPtr& /*context*/)
{
    THROW_ERROR_EXCEPTION("Object cannot have attributes");
}

IYPathService::TResolveResult TYPathServiceBase::ResolveRecursive(
    const TYPath& /*path*/,
    const IServiceContextPtr& /*context*/)
{
    THROW_ERROR_EXCEPTION("Object cannot have children");
}

void TYPathServiceBase::Invoke(const IServiceContextPtr& context)
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

void TYPathServiceBase::BeforeInvoke(const IServiceContextPtr& /*context*/)
{ }

bool TYPathServiceBase::DoInvoke(const IServiceContextPtr& /*context*/)
{
    return false;
}

void TYPathServiceBase::AfterInvoke(const IServiceContextPtr& /*context*/)
{ }

void TYPathServiceBase::DoWriteAttributesFragment(
    NYson::IAsyncYsonConsumer* /*consumer*/,
    const TNullable<std::vector<TString>>& /*attributeKeys*/,
    bool /*stable*/)
{ }

bool TYPathServiceBase::ShouldHideAttributes()
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_SUPPORTS_VERB_RESOLVE(method, onPathError) \
    DEFINE_RPC_SERVICE_METHOD(TSupports##method, method) \
    { \
        NYPath::TTokenizer tokenizer(GetRequestYPath(context->RequestHeader())); \
        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) { \
            method##Self(request, response, context); \
            return; \
        } \
        tokenizer.Skip(NYPath::ETokenType::Ampersand); \
        if (tokenizer.GetType() == NYPath::ETokenType::Slash) { \
            if (tokenizer.Advance() == NYPath::ETokenType::At) { \
                method##Attribute(TYPath(tokenizer.GetSuffix()), request, response, context); \
            } else { \
                method##Recursive(TYPath(tokenizer.GetInput()), request, response, context); \
            } \
            return; \
        } \
        onPathError \
    }

#define IMPLEMENT_SUPPORTS_VERB(method) \
    IMPLEMENT_SUPPORTS_VERB_RESOLVE( \
        method, \
        { \
            tokenizer.ThrowUnexpected(); \
            Y_UNREACHABLE(); \
        } \
    ) \
    \
    void TSupports##method::method##Attribute(const TYPath& path, TReq##method* request, TRsp##method* response, const TCtx##method##Ptr& context) \
    { \
        Y_UNUSED(path); \
        Y_UNUSED(request); \
        Y_UNUSED(response); \
        ThrowMethodNotSupported(context->GetMethod(), TString("attribute")); \
    } \
    \
    void TSupports##method::method##Self(TReq##method* request, TRsp##method* response, const TCtx##method##Ptr& context) \
    { \
        Y_UNUSED(request); \
        Y_UNUSED(response); \
        ThrowMethodNotSupported(context->GetMethod(), TString("self")); \
    } \
    \
    void TSupports##method::method##Recursive(const TYPath& path, TReq##method* request, TRsp##method* response, const TCtx##method##Ptr& context) \
    { \
        Y_UNUSED(path); \
        Y_UNUSED(request); \
        Y_UNUSED(response); \
        ThrowMethodNotSupported(context->GetMethod(), TString("recursive")); \
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

void TSupportsExistsBase::Reply(const TCtxExistsPtr& context, bool value)
{
    context->Response().set_value(value);
    context->SetResponseInfo("Result: %v", value);
    context->Reply();
}

void TSupportsExists::ExistsAttribute(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    Reply(context, false);
}

void TSupportsExists::ExistsSelf(
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    Reply(context, true);
}

void TSupportsExists::ExistsRecursive(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    const TCtxExistsPtr& context)
{
    context->SetRequestInfo();

    Reply(context, false);
}

////////////////////////////////////////////////////////////////////////////////

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

TSupportsAttributes::TCombinedAttributeDictionary::TCombinedAttributeDictionary(TSupportsAttributes* owner)
    : Owner_(owner)
{ }

std::vector<TString> TSupportsAttributes::TCombinedAttributeDictionary::List() const
{
    std::vector<TString> keys;

    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ReserveAndListSystemAttributes(&descriptors);
        for (const auto& descriptor : descriptors) {
            if (descriptor.Present && !descriptor.Custom && !descriptor.Opaque) {
                keys.push_back(descriptor.Key);
            }
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (customAttributes) {
        auto customKeys = customAttributes->List();
        keys.insert(keys.end(), customKeys.begin(), customKeys.end());
    }

    return keys;
}

TYsonString TSupportsAttributes::TCombinedAttributeDictionary::FindYson(const TString& key) const
{
    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        const auto& builtinKeys = provider->GetBuiltinAttributeKeys();
        if (builtinKeys.find(key) != builtinKeys.end()) {
            return provider->FindBuiltinAttribute(key);
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (!customAttributes) {
        return TYsonString();
    }
    return customAttributes->FindYson(key);
}

void TSupportsAttributes::TCombinedAttributeDictionary::SetYson(const TString& key, const TYsonString& value)
{
    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        const auto& builtinKeys = provider->GetBuiltinAttributeKeys();
        if (builtinKeys.find(key) != builtinKeys.end()) {
            if (!provider->SetBuiltinAttribute(key, value)) {
                ThrowCannotSetBuiltinAttribute(key);
            }
            return;
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (!customAttributes) {
        ThrowNoSuchBuiltinAttribute(key);
    }
    customAttributes->SetYson(key, value);
}

bool TSupportsAttributes::TCombinedAttributeDictionary::Remove(const TString& key)
{
    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        const auto& builtinKeys = provider->GetBuiltinAttributeKeys();
        if (builtinKeys.find(key) != builtinKeys.end()) {
            return provider->RemoveBuiltinAttribute(key);
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (!customAttributes) {
        ThrowNoSuchBuiltinAttribute(key);
    }
    return customAttributes->Remove(key);
}

////////////////////////////////////////////////////////////////////////////////

TSupportsAttributes::TSupportsAttributes()
    : CombinedAttributes_(this)
{ }

IYPathService::TResolveResult TSupportsAttributes::ResolveAttributes(
    const TYPath& path,
    const IServiceContextPtr& context)
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

    return TResolveResultHere{"/@" + path};
}

TFuture<TYsonString> TSupportsAttributes::DoFindAttribute(const TString& key)
{
    auto* customAttributes = GetCustomAttributes();
    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    if (customAttributes) {
        auto attribute = customAttributes->FindYson(key);
        if (attribute) {
            return MakeFuture(attribute);
        }
    }

    if (builtinAttributeProvider) {
        auto builtinYson = builtinAttributeProvider->FindBuiltinAttribute(key);
        if (builtinYson) {
            return MakeFuture(builtinYson);
        }

        auto asyncResult = builtinAttributeProvider->GetBuiltinAttributeAsync(key);
        if (asyncResult) {
            return asyncResult;
        }
    }

    return Null;
}

TYsonString TSupportsAttributes::DoGetAttributeFragment(
    const TString& key,
    const TYPath& path,
    const TYsonString& wholeYson)
{
    if (!wholeYson) {
        ThrowNoSuchAttribute(key);
    }
    auto node = ConvertToNode<TYsonString>(wholeYson);
    return SyncYPathGet(node, path, Null);
}

TFuture<TYsonString> TSupportsAttributes::DoGetAttribute(
    const TYPath& path,
    const TNullable<std::vector<TString>>& attributeKeys)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        TAsyncYsonWriter writer;

        writer.OnBeginMap();

        if (attributeKeys) {
            WriteAttributesFragment(&writer, attributeKeys, /*stable*/false);
        } else {
            if (builtinAttributeProvider) {
                std::vector<ISystemAttributeProvider::TAttributeDescriptor> builtinDescriptors;
                builtinAttributeProvider->ListBuiltinAttributes(&builtinDescriptors);
                for (const auto& descriptor : builtinDescriptors) {
                    if (!descriptor.Present)
                        continue;

                    auto key = TString(descriptor.Key);
                    TAttributeValueConsumer attributeValueConsumer(&writer, descriptor.Key);

                    if (descriptor.Opaque) {
                        attributeValueConsumer.OnEntity();
                        continue;
                    }

                    if (builtinAttributeProvider->GetBuiltinAttribute(key, &attributeValueConsumer)) {
                        continue;
                    }

                    auto asyncValue = builtinAttributeProvider->GetBuiltinAttributeAsync(key);
                    if (asyncValue) {
                        attributeValueConsumer.OnRaw(std::move(asyncValue));
                    }
                }
            }

            auto* customAttributes = GetCustomAttributes();
            if (customAttributes) {
                for (const auto& key : customAttributes->List()) {
                    writer.OnKeyedItem(key);
                    Serialize(customAttributes->GetYson(key), &writer);
                }
            }
        }

        writer.OnEndMap();

        return writer.Finish();
    } else {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        auto asyncYson = DoFindAttribute(key);
        if (!asyncYson) {
            asyncYson = NoneYsonFuture;
        }

        tokenizer.Advance();
        return asyncYson.Apply(BIND(
            &TSupportsAttributes::DoGetAttributeFragment,
            key,
            TYPath(tokenizer.GetInput())));
   }
}

void TSupportsAttributes::GetAttribute(
    const TYPath& path,
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    context->SetRequestInfo();

    auto attributeKeys = request->has_attributes()
        ? MakeNullable(FromProto<std::vector<TString>>(request->attributes().keys()))
        : Null;

    DoGetAttribute(path, attributeKeys).Subscribe(BIND([=] (const TErrorOr<TYsonString>& ysonOrError) {
        if (!ysonOrError.IsOK()) {
            context->Reply(ysonOrError);
            return;
        }
        response->set_value(ysonOrError.Value().GetData());
        context->Reply();
    }));
}

TYsonString TSupportsAttributes::DoListAttributeFragment(
    const TString& key,
    const TYPath& path,
    const TYsonString& wholeYson)
{
    if (!wholeYson) {
        ThrowNoSuchAttribute(key);
    }

    auto node = ConvertToNode(wholeYson);
    auto listedKeys = SyncYPathList(node, path);

    TStringStream stream;
    TBufferedBinaryYsonWriter writer(&stream);
    writer.OnBeginList();
    for (const auto& listedKey : listedKeys) {
        writer.OnListItem();
        writer.OnStringScalar(listedKey);
    }
    writer.OnEndList();
    writer.Flush();

    return TYsonString(stream.Str());
}

TFuture<TYsonString> TSupportsAttributes::DoListAttribute(const TYPath& path)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream);

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
        writer.Flush();

        return MakeFuture(TYsonString(stream.Str()));
    } else  {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        auto asyncYson = DoFindAttribute(key);
        if (!asyncYson) {
            asyncYson = NoneYsonFuture;
        }

        tokenizer.Advance();
        return asyncYson.Apply(BIND(
            &TSupportsAttributes::DoListAttributeFragment,
            key,
            TYPath(tokenizer.GetInput())));
    }
}

void TSupportsAttributes::ListAttribute(
    const TYPath& path,
    TReqList* /*request*/,
    TRspList* response,
    const TCtxListPtr& context)
{
    context->SetRequestInfo();

    DoListAttribute(path).Subscribe(BIND([=] (const TErrorOr<TYsonString>& ysonOrError) {
        if (ysonOrError.IsOK()) {
            response->set_value(ysonOrError.Value().GetData());
            context->Reply();
        } else {
            context->Reply(ysonOrError);
        }
    }));
}

bool TSupportsAttributes::DoExistsAttributeFragment(
    const TString& key,
    const TYPath& path,
    const TErrorOr<TYsonString>& wholeYsonOrError)
{
    if (!wholeYsonOrError.IsOK()) {
        return false;
    }
    const auto& wholeYson = wholeYsonOrError.Value();
    if (!wholeYson) {
        return false;
    }
    auto node = ConvertToNode<TYsonString>(wholeYson);
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
            auto maybeDescriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(key);
            if (maybeDescriptor) {
                const auto& descriptor = *maybeDescriptor;
                return descriptor.Present ? TrueFuture : FalseFuture;
            }
        }

        return FalseFuture;
    } else {
        auto asyncYson = DoFindAttribute(key);
        if (!asyncYson) {
            return FalseFuture;
        }

        return asyncYson.Apply(BIND(
            &TSupportsAttributes::DoExistsAttributeFragment,
            key,
            TYPath(tokenizer.GetInput())));
    }
}

void TSupportsAttributes::ExistsAttribute(
    const TYPath& path,
    TReqExists* /*request*/,
    TRspExists* response,
    const TCtxExistsPtr& context)
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
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::EndOfStream: {
            auto newAttributes = ConvertToAttributes(newYson);

            std::map<TString, ISystemAttributeProvider::TAttributeDescriptor> descriptorMap;
            if (builtinAttributeProvider) {
                builtinAttributeProvider->ListSystemAttributes(&descriptorMap);
            }

            // Set custom attributes.
            if (customAttributes) {
                auto customAttributeKeys = customAttributes->List();
                std::sort(customAttributeKeys.begin(), customAttributeKeys.end());
                for (const auto& key : customAttributeKeys) {
                    if (!newAttributes->Contains(key)) {
                        permissionValidator.Validate(EPermission::Write);

                        YCHECK(customAttributes->Remove(key));
                    }
                }

                auto newAttributeKeys = newAttributes->List();
                std::sort(newAttributeKeys.begin(), newAttributeKeys.end());
                for (const auto& key : newAttributeKeys) {
                    auto it = descriptorMap.find(key);
                    if (it == descriptorMap.end() || it->second.Custom) {
                        permissionValidator.Validate(EPermission::Write);

                        customAttributes->SetYson(key, newAttributes->GetYson(key));

                        YCHECK(newAttributes->Remove(key));
                    }
                }
            }

            // Set builtin attributes.
            if (builtinAttributeProvider) {
                for (const auto& pair : descriptorMap) {
                    const auto& key = pair.first;
                    const auto& descriptor = pair.second;

                    if (descriptor.Custom) {
                        continue;
                    }

                    auto newAttributeYson = newAttributes->FindYson(key);
                    if (newAttributeYson) {
                        if (!descriptor.Writable) {
                            ThrowCannotSetBuiltinAttribute(key);
                        }

                        permissionValidator.Validate(descriptor.WritePermission);

                        if (!GuardedSetBuiltinAttribute(key, newAttributeYson)) {
                            ThrowCannotSetBuiltinAttribute(key);
                        }

                        YCHECK(newAttributes->Remove(key));
                    } else if (descriptor.Removable) {
                        permissionValidator.Validate(descriptor.WritePermission);

                        if (!GuardedRemoveBuiltinAttribute(key)) {
                            ThrowCannotRemoveAttribute(key);
                        }
                    }
                }
            }

            auto remainingNewKeys = newAttributes->List();
            if (!remainingNewKeys.empty()) {
                ThrowCannotSetBuiltinAttribute(remainingNewKeys[0]);
            }

            break;
        }

        case NYPath::ETokenType::Literal: {
            auto key = tokenizer.GetLiteralValue();

            if (key.Empty()) {
                THROW_ERROR_EXCEPTION("Attribute key cannot be empty");
            }

            TNullable<ISystemAttributeProvider::TAttributeDescriptor> descriptor;
            if (builtinAttributeProvider) {
                descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(key);
            }

            if (descriptor) {
                if (!descriptor->Writable) {
                    ThrowCannotSetBuiltinAttribute(key);
                }

                permissionValidator.Validate(descriptor->WritePermission);

                if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                    if (!GuardedSetBuiltinAttribute(key, newYson)) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }
                } else {
                    auto oldWholeYson = builtinAttributeProvider->FindBuiltinAttribute(key);
                    if (!oldWholeYson) {
                        ThrowNoSuchBuiltinAttribute(key);
                    }

                    auto oldWholeNode = ConvertToNode(oldWholeYson);
                    SyncYPathSet(oldWholeNode, TYPath(tokenizer.GetInput()), newYson);
                    auto newWholeYson = ConvertToYsonStringStable(oldWholeNode);

                    if (!GuardedSetBuiltinAttribute(key, newWholeYson)) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }
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

                    auto wholeNode = ConvertToNode(oldWholeYson);
                    SyncYPathSet(wholeNode, TYPath(tokenizer.GetInput()), newYson);
                    auto newWholeYson = ConvertToYsonStringStable(wholeNode);

                    customAttributes->SetYson(key, newWholeYson);
                }
            }

            break;
        }

        default:
            tokenizer.ThrowUnexpected();
    }
}

void TSupportsAttributes::SetAttribute(
    const TYPath& path,
    TReqSet* request,
    TRspSet* /*response*/,
    const TCtxSetPtr& context)
{
    context->SetRequestInfo();

    // Request instances are pooled, and thus are request->values.
    // Check if this pooled string has a small overhead (<= 25%).
    // Otherwise make a deep copy.
    const auto& requestValue = request->value();
    const auto& safeValue = requestValue.capacity() <= requestValue.length() * 5 / 4
        ? requestValue
        : TString(TStringBuf(requestValue));
    DoSetAttribute(path, TYsonString(safeValue));
    context->Reply();
}

void TSupportsAttributes::DoRemoveAttribute(const TYPath& path, bool force)
{
    TCachingPermissionValidator permissionValidator(this, EPermissionCheckScope::This);

    auto* customAttributes = GetCustomAttributes();
    auto* builtinAttributeProvider = GetBuiltinAttributeProvider();

    NYPath::TTokenizer tokenizer(path);
    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::Asterisk: {
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
            auto customYson = customAttributes ? customAttributes->FindYson(key) : TYsonString();
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                if (customYson) {
                    permissionValidator.Validate(EPermission::Write);

                    YCHECK(customAttributes->Remove(key));
                } else {
                    if (!builtinAttributeProvider) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchCustomAttribute(key);
                    }

                    auto descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(key);
                    if (!descriptor) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }
                    if (!descriptor->Removable) {
                        ThrowCannotRemoveAttribute(key);
                    }

                    permissionValidator.Validate(descriptor->WritePermission);

                    if (!GuardedRemoveBuiltinAttribute(key)) {
                        ThrowNoSuchBuiltinAttribute(key);
                    }
                }
            } else {
                if (customYson) {
                    permissionValidator.Validate(EPermission::Write);

                    auto customNode = ConvertToNode(customYson);
                    SyncYPathRemove(customNode, TYPath(tokenizer.GetInput()), /*recursive*/ true, force);
                    auto updatedCustomYson = ConvertToYsonStringStable(customNode);

                    customAttributes->SetYson(key, updatedCustomYson);
                } else {
                    if (!builtinAttributeProvider) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }

                    auto descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(key);
                    if (!descriptor) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }

                    if (!descriptor->Writable) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }

                    permissionValidator.Validate(descriptor->WritePermission);

                    auto builtinYson = builtinAttributeProvider->FindBuiltinAttribute(key);
                    if (!builtinYson) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }

                    auto builtinNode = ConvertToNode(builtinYson);
                    SyncYPathRemove(builtinNode, TYPath(tokenizer.GetInput()));
                    auto updatedSystemYson = ConvertToYsonStringStable(builtinNode);

                    if (!GuardedSetBuiltinAttribute(key, updatedSystemYson)) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }
                }
            }
            break;
        }

        default:
            tokenizer.ThrowUnexpected();
            break;
    }
}

void TSupportsAttributes::RemoveAttribute(
    const TYPath& path,
    TReqRemove* request,
    TRspRemove* /*response*/,
    const TCtxRemovePtr& context)
{
    context->SetRequestInfo();

    bool force = request->force();
    DoRemoveAttribute(path, force);
    context->Reply();
}

IAttributeDictionary* TSupportsAttributes::GetCombinedAttributes()
{
    return &CombinedAttributes_;
}

IAttributeDictionary* TSupportsAttributes::GetCustomAttributes()
{
    return nullptr;
}

ISystemAttributeProvider* TSupportsAttributes::GetBuiltinAttributeProvider()
{
    return nullptr;
}

bool TSupportsAttributes::GuardedSetBuiltinAttribute(const TString& key, const TYsonString& yson)
{
    auto* provider = GetBuiltinAttributeProvider();

    try {
        return provider->SetBuiltinAttribute(key, yson);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error setting builtin attribute %Qv",
            ToYPathLiteral(key))
            << ex;
    }
}

bool TSupportsAttributes::GuardedRemoveBuiltinAttribute(const TString& key)
{
    auto* provider = GetBuiltinAttributeProvider();

    try {
        return provider->RemoveBuiltinAttribute(key);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error removing builtin attribute %Qv",
            ToYPathLiteral(key))
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

const THashSet<const char*>& TBuiltinAttributeKeysCache::GetBuiltinAttributeKeys(
    ISystemAttributeProvider* provider)
{
    if (!Initialized_) {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ListSystemAttributes(&descriptors);
        BuiltinKeys_.reserve(descriptors.size());
        for (const auto& descriptor : descriptors) {
            if (!descriptor.Custom) {
                YCHECK(BuiltinKeys_.insert(descriptor.Key).second);
            }
        }
        Initialized_ = true;
    }
    return BuiltinKeys_;
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
    std::unique_ptr<TBufferedBinaryYsonWriter> AttributeWriter_;


    virtual void OnMyKeyedItem(const TStringBuf& key) override
    {
        AttributeWriter_.reset(new TBufferedBinaryYsonWriter(&AttributeStream_));
        Forward(
            AttributeWriter_.get(),
            [this, key = TString(key)] {
                AttributeWriter_->Flush();
                AttributeWriter_.reset();
                Attributes_->SetYson(key, TYsonString(AttributeStream_.Str()));
                AttributeStream_.clear();
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INode* node, ITreeBuilder* builder)
    : Node_(node)
    , TreeBuilder_(builder)
    , NodeFactory_(node->CreateFactory())
{ }

TNodeSetterBase::~TNodeSetterBase() = default;

void TNodeSetterBase::ThrowInvalidType(ENodeType actualType)
{
    THROW_ERROR_EXCEPTION("Cannot update %Qlv node with %Qlv value; types must match",
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
    Forward(AttributesSetter_.get(), nullptr, EYsonType::MapFragment);
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
        NLogging::TLogger logger,
        NLogging::ELogLevel logLevel,
        TString loggingInfo)
        : TServiceContextBase(
            std::move(requestMessage),
            std::move(logger),
            logLevel)
        , LoggingInfo_(std::move(loggingInfo))
    { }

    TYPathServiceContext(
        std::unique_ptr<TRequestHeader> requestHeader,
        TSharedRefArray requestMessage,
        NLogging::TLogger logger,
        NLogging::ELogLevel logLevel,
        TString loggingInfo)
        : TServiceContextBase(
            std::move(requestHeader),
            std::move(requestMessage),
            std::move(logger),
            logLevel)
        , LoggingInfo_(std::move(loggingInfo))
    { }

    virtual TTcpDispatcherStatistics GetBusStatistics() const override
    {
        return {};
    }

protected:
    const TString LoggingInfo_;

    TNullable<NProfiling::TWallTimer> Timer_;


    virtual void DoReply() override
    { }

    virtual void LogRequest() override
    {
        TStringBuilder builder;
        builder.AppendFormat("%v:%v %v <- ",
            GetService(),
            GetMethod(),
            GetRequestYPath(*RequestHeader_));

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
        if (LoggingInfo_) {
            delimitedBuilder->AppendString(LoggingInfo_);
        }

        auto mutationId = GetMutationId();
        if (mutationId) {
            delimitedBuilder->AppendFormat("MutationId: %v", mutationId);
        }

        delimitedBuilder->AppendFormat("Retry: %v", IsRetry());

        if (RequestInfo_) {
            delimitedBuilder->AppendString(RequestInfo_);
        }

        LOG_DEBUG(builder.Flush());

        Timer_.Emplace();
    }

    virtual void LogResponse() override
    {
        TStringBuilder builder;
        builder.AppendFormat("%v:%v %v -> ",
            GetService(),
            GetMethod(),
            GetRequestYPath(*RequestHeader_));

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
        if (LoggingInfo_) {
            delimitedBuilder->AppendString(LoggingInfo_);
        }

        if (ResponseInfo_) {
            delimitedBuilder->AppendString(ResponseInfo_);
        }

        if (Timer_) {
            delimitedBuilder->AppendFormat("WallTime: %v", Timer_->GetElapsedTime().MilliSeconds());
        }

        delimitedBuilder->AppendFormat("Error: %v", Error_);

        LOG_DEBUG(builder.Flush());
    }
};

IServiceContextPtr CreateYPathContext(
    TSharedRefArray requestMessage,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel,
    TString loggingInfo)
{
    Y_ASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestMessage),
        std::move(logger),
        logLevel,
        std::move(loggingInfo));
}

IServiceContextPtr CreateYPathContext(
    std::unique_ptr<TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel,
    TString loggingInfo)
{
    Y_ASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestHeader),
        std::move(requestMessage),
        std::move(logger),
        logLevel,
        std::move(loggingInfo));
}

////////////////////////////////////////////////////////////////////////////////

class TRootService
    : public IYPathService
{
public:
    explicit TRootService(IYPathServicePtr underlyingService)
        : UnderlyingService_(std::move(underlyingService))
    { }

    virtual void Invoke(const IServiceContextPtr& /*context*/) override
    {
        Y_UNREACHABLE();
    }

    virtual TResolveResult Resolve(
        const TYPath& path,
        const IServiceContextPtr& /*context*/) override
    {
        NYPath::TTokenizer tokenizer(path);
        if (tokenizer.Advance() != NYPath::ETokenType::Slash) {
            THROW_ERROR_EXCEPTION("YPath must start with \"/\"");
        }

        return TResolveResultThere{UnderlyingService_, TYPath(tokenizer.GetSuffix())};
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* consumer,
        const TNullable<std::vector<TString>>& attributeKeys,
        bool stable) override
    {
        UnderlyingService_->WriteAttributesFragment(consumer, attributeKeys, stable);
    }

    virtual bool ShouldHideAttributes() override
    {
        return false;
    }

private:
    const IYPathServicePtr UnderlyingService_;

};

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService)
{
    return New<TRootService>(std::move(underlyingService));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
