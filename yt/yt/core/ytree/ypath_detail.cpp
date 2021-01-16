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

#include <yt/core/net/address.h>

#include <yt/core/profiling/timing.h>

namespace NYT::NYTree {

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
    const std::optional<std::vector<TString>>& /*attributeKeys*/,
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
        NYPath::TTokenizer tokenizer(GetRequestTargetYPath(context->RequestHeader())); \
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
            YT_ABORT(); \
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

DEFINE_RPC_SERVICE_METHOD(TSupportsMultisetAttributes, Multiset)
{
    context->SetRequestInfo("KeyCount: %v", request->subrequests_size());

    TReqMultisetAttributes req;
    for (const auto& subrequest : request->subrequests()) {
        auto* subreq = req.add_subrequests();
        subreq->set_attribute(subrequest.key());
        subreq->set_value(subrequest.value());
    }

    TRspMultisetAttributes rsp;
    DoSetAttributes(GetRequestTargetYPath(context->RequestHeader()), &req, &rsp);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TSupportsMultisetAttributes, MultisetAttributes)
{
    context->SetRequestInfo("KeyCount: %v", request->subrequests_size());

    DoSetAttributes(GetRequestTargetYPath(context->RequestHeader()), request, response);

    context->Reply();
}

void TSupportsMultisetAttributes::DoSetAttributes(
    const TYPath& path,
    TReqMultisetAttributes* request,
    TRspMultisetAttributes* response)
{
    NYPath::TTokenizer tokenizer(path);

    tokenizer.Advance();
    tokenizer.Skip(NYPath::ETokenType::Ampersand);
    tokenizer.Expect(NYPath::ETokenType::Slash);
    if (tokenizer.Advance() != NYPath::ETokenType::At) {
        tokenizer.ThrowUnexpected();
    }

    SetAttributes(TYPath(tokenizer.GetSuffix()), request, response);
}

void TSupportsMultisetAttributes::SetAttributes(
    const TYPath& path,
    TReqMultisetAttributes* request,
    TRspMultisetAttributes* response)
{
    Y_UNUSED(path);
    Y_UNUSED(request);
    Y_UNUSED(response);
    ThrowMethodNotSupported("MultisetAttributes", TString("attributes"));
}

////////////////////////////////////////////////////////////////////////////////

void TSupportsPermissions::ValidatePermission(
    EPermissionCheckScope /*scope*/,
    EPermission /*permission*/,
    const TString& /*user*/)
{ }

////////////////////////////////////////////////////////////////////////////////

TSupportsPermissions::TCachingPermissionValidator::TCachingPermissionValidator(
    TSupportsPermissions* owner,
    EPermissionCheckScope scope)
    : Owner_(owner)
    , Scope_(scope)
{ }

void TSupportsPermissions::TCachingPermissionValidator::Validate(EPermission permission, const TString& user)
{
    auto& validatedPermissions = ValidatedPermissions_[user];
    if (None(validatedPermissions & permission)) {
        Owner_->ValidatePermission(Scope_, permission, user);
        validatedPermissions |= permission;
    }
}

////////////////////////////////////////////////////////////////////////////////

TSupportsAttributes::TCombinedAttributeDictionary::TCombinedAttributeDictionary(TSupportsAttributes* owner)
    : Owner_(owner)
{ }

std::vector<TString> TSupportsAttributes::TCombinedAttributeDictionary::ListKeys() const
{
    std::vector<TString> keys;

    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ReserveAndListSystemAttributes(&descriptors);
        for (const auto& descriptor : descriptors) {
            if (descriptor.Present && !descriptor.Custom && !descriptor.Opaque) {
                keys.push_back(descriptor.InternedKey.Unintern());
            }
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (customAttributes) {
        auto customKeys = customAttributes->ListKeys();
        for (auto&& key : customKeys) {
            keys.push_back(std::move(key));
        }
    }
    return keys;
}

std::vector<IAttributeDictionary::TKeyValuePair> TSupportsAttributes::TCombinedAttributeDictionary::ListPairs() const
{
    std::vector<TKeyValuePair> pairs;

    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ReserveAndListSystemAttributes(&descriptors);
        for (const auto& descriptor : descriptors) {
            if (descriptor.Present && !descriptor.Custom && !descriptor.Opaque) {
                auto value = provider->FindBuiltinAttribute(descriptor.InternedKey);
                if (value) {
                    auto key = descriptor.InternedKey.Unintern();
                    pairs.push_back(std::make_pair(std::move(key), std::move(value)));
                }
            }
        }
    }

    auto* customAttributes = Owner_->GetCustomAttributes();
    if (customAttributes) {
        for (const auto& pair : customAttributes->ListPairs()) {
            pairs.push_back(pair);
        }
    }

    return pairs;
}

TYsonString TSupportsAttributes::TCombinedAttributeDictionary::FindYson(TStringBuf key) const
{
    auto* provider = Owner_->GetBuiltinAttributeProvider();
    if (provider) {
        auto internedKey = TInternedAttributeKey::Lookup(key);
        if (internedKey != InvalidInternedAttribute) {
            const auto& builtinKeys = provider->GetBuiltinAttributeKeys();
            if (builtinKeys.find(internedKey) != builtinKeys.end()) {
                return provider->FindBuiltinAttribute(internedKey);
            }
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
        auto internedKey = TInternedAttributeKey::Lookup(key);
        if (internedKey != InvalidInternedAttribute) {
            const auto& builtinKeys = provider->GetBuiltinAttributeKeys();
            if (builtinKeys.find(internedKey) != builtinKeys.end()) {
                if (!provider->SetBuiltinAttribute(internedKey, value)) {
                    ThrowCannotSetBuiltinAttribute(key);
                }
                return;
            }
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
        auto internedKey = TInternedAttributeKey::Lookup(key);
        if (internedKey != InvalidInternedAttribute) {
            const auto& builtinKeys = provider->GetBuiltinAttributeKeys();
            if (builtinKeys.find(internedKey) != builtinKeys.end()) {
                return provider->RemoveBuiltinAttribute(internedKey);
            }
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
    : CombinedAttributes_(New<TSupportsAttributes::TCombinedAttributeDictionary>(this))
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
        method != "Exists" &&
        method != "Multiset" &&
        method != "MultisetAttributes")
    {
        ThrowMethodNotSupported(method);
    }

    return TResolveResultHere{"/@" + path};
}

TFuture<TYsonString> TSupportsAttributes::DoFindAttribute(TStringBuf key)
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
        auto internedKey = TInternedAttributeKey::Lookup(key);
        if (internedKey != InvalidInternedAttribute) {
            if (auto builtinYson = builtinAttributeProvider->FindBuiltinAttribute(internedKey)) {
                return MakeFuture(builtinYson);
            }
        }

        auto asyncResult = builtinAttributeProvider->GetBuiltinAttributeAsync(internedKey);
        if (asyncResult) {
            return asyncResult;
        }
    }

    return std::nullopt;
}

TYsonString TSupportsAttributes::DoGetAttributeFragment(
    TStringBuf key,
    const TYPath& path,
    const TYsonString& wholeYson)
{
    if (!wholeYson) {
        ThrowNoSuchAttribute(key);
    }
    if (path.empty()) {
        return wholeYson;
    }
    auto node = ConvertToNode<TYsonString>(wholeYson);
    return SyncYPathGet(node, path, std::nullopt);
}

TFuture<TYsonString> TSupportsAttributes::DoGetAttribute(
    const TYPath& path,
    const std::optional<std::vector<TString>>& attributeKeys)
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

                    auto key = descriptor.InternedKey.Unintern();
                    TAttributeValueConsumer attributeValueConsumer(&writer, key);

                    if (descriptor.Opaque) {
                        attributeValueConsumer.OnEntity();
                        continue;
                    }

                    if (GuardedGetBuiltinAttribute(descriptor.InternedKey, &attributeValueConsumer)) {
                        continue;
                    }

                    auto asyncValue = builtinAttributeProvider->GetBuiltinAttributeAsync(descriptor.InternedKey);
                    if (asyncValue) {
                        attributeValueConsumer.OnRaw(std::move(asyncValue));
                    }
                }
            }

            auto* customAttributes = GetCustomAttributes();
            if (customAttributes) {
                for (const auto& [key, value] : customAttributes->ListPairs()) {
                    writer.OnKeyedItem(key);
                    Serialize(value, &writer);
                }
            }
        }

        writer.OnEndMap();

        return writer.Finish(NRpc::TDispatcher::Get()->GetHeavyInvoker());
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
                TYPath(tokenizer.GetInput()))
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
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
        ? std::make_optional(FromProto<std::vector<TString>>(request->attributes().keys()))
        : std::nullopt;

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
    TStringBuf key,
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
            auto userKeys = customAttributes->ListKeys();
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
                    writer.OnStringScalar(descriptor.InternedKey.Unintern());
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
    TStringBuf /*key*/,
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
            auto internedKey = TInternedAttributeKey::Lookup(key);
            if (internedKey != InvalidInternedAttribute) {
                auto optionalDescriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(internedKey);
                if (optionalDescriptor) {
                    const auto& descriptor = *optionalDescriptor;
                    return descriptor.Present ? TrueFuture : FalseFuture;
                }
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

            std::map<TInternedAttributeKey, ISystemAttributeProvider::TAttributeDescriptor> descriptorMap;
            if (builtinAttributeProvider) {
                builtinAttributeProvider->ListSystemAttributes(&descriptorMap);
            }

            // Set custom attributes.
            if (customAttributes) {

                auto modifyPermission = builtinAttributeProvider
                    ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                    : EPermission::Write;

                auto customKeys = customAttributes->ListKeys();
                std::sort(customKeys.begin(), customKeys.end());
                for (const auto& key : customKeys) {
                    if (!newAttributes->Contains(key)) {
                        permissionValidator.Validate(modifyPermission);

                        YT_VERIFY(customAttributes->Remove(key));
                    }
                }

                auto newPairs = newAttributes->ListPairs();
                std::sort(newPairs.begin(), newPairs.end(), [] (const auto& lhs, const auto& rhs) {
                    return lhs.first < rhs.first;
                });
                for (const auto& [key, value] : newPairs) {
                    auto internedKey = TInternedAttributeKey::Lookup(key);
                    auto it = (internedKey != InvalidInternedAttribute)
                        ? descriptorMap.find(internedKey)
                        : descriptorMap.end();
                    if (it == descriptorMap.end() || it->second.Custom) {
                        permissionValidator.Validate(modifyPermission);

                        customAttributes->SetYson(key, value);

                        YT_VERIFY(newAttributes->Remove(key));
                    }
                }
            }

            // Set builtin attributes.
            if (builtinAttributeProvider) {
                for (const auto& [internedKey, descriptor] : descriptorMap) {
                    const auto& key = internedKey.Unintern();

                    if (descriptor.Custom) {
                        continue;
                    }

                    auto newAttributeYson = newAttributes->FindYson(key);
                    if (newAttributeYson) {
                        if (!descriptor.Writable) {
                            ThrowCannotSetBuiltinAttribute(key);
                        }

                        permissionValidator.Validate(descriptor.ModifyPermission);

                        if (!GuardedSetBuiltinAttribute(internedKey, newAttributeYson)) {
                            ThrowCannotSetBuiltinAttribute(key);
                        }

                        YT_VERIFY(newAttributes->Remove(key));
                    } else if (descriptor.Removable) {
                        permissionValidator.Validate(descriptor.ModifyPermission);

                        if (!GuardedRemoveBuiltinAttribute(internedKey)) {
                            ThrowCannotRemoveAttribute(key);
                        }
                    }
                }
            }

            auto remainingNewKeys = newAttributes->ListKeys();
            if (!remainingNewKeys.empty()) {
                ThrowCannotSetBuiltinAttribute(remainingNewKeys[0]);
            }

            break;
        }

        case NYPath::ETokenType::Literal: {
            auto key = tokenizer.GetLiteralValue();
            ValidateAttributeKey(key);
            auto internedKey = TInternedAttributeKey::Lookup(key);

            std::optional<ISystemAttributeProvider::TAttributeDescriptor> descriptor;
            if (builtinAttributeProvider && internedKey != InvalidInternedAttribute) {
                descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(internedKey);
            }

            if (descriptor) {
                if (!descriptor->Writable) {
                    ThrowCannotSetBuiltinAttribute(key);
                }

                permissionValidator.Validate(descriptor->ModifyPermission);

                if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                    if (!GuardedSetBuiltinAttribute(internedKey, newYson)) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }
                } else {
                    auto oldWholeYson = builtinAttributeProvider->FindBuiltinAttribute(internedKey);
                    if (!oldWholeYson) {
                        ThrowNoSuchBuiltinAttribute(key);
                    }

                    auto oldWholeNode = ConvertToNode(oldWholeYson);
                    SyncYPathSet(oldWholeNode, TYPath(tokenizer.GetInput()), newYson);
                    auto newWholeYson = ConvertToYsonStringStable(oldWholeNode);

                    if (!GuardedSetBuiltinAttribute(internedKey, newWholeYson)) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }
                }
            } else {
                if (!customAttributes) {
                    THROW_ERROR_EXCEPTION("Custom attributes are not supported");
                }
                auto modifyPermission = builtinAttributeProvider
                    ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                    : EPermission::Write;
                permissionValidator.Validate(modifyPermission);

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
                auto modifyPermission = builtinAttributeProvider
                    ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                    : EPermission::Write;
                permissionValidator.Validate(modifyPermission);

                auto customKeys = customAttributes->ListKeys();
                std::sort(customKeys.begin(), customKeys.end());
                for (const auto& key : customKeys) {
                    YT_VERIFY(customAttributes->Remove(key));
                }
            }
            break;
        }

        case NYPath::ETokenType::Literal: {
            auto key = tokenizer.GetLiteralValue();
            auto internedKey = TInternedAttributeKey::Lookup(key);
            auto customYson = customAttributes ? customAttributes->FindYson(key) : TYsonString();
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                if (customYson) {
                    auto modifyPermission = builtinAttributeProvider
                        ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                        : EPermission::Write;
                    permissionValidator.Validate(modifyPermission);

                    YT_VERIFY(customAttributes->Remove(key));
                } else {
                    if (!builtinAttributeProvider) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchCustomAttribute(key);
                    }

                    auto descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(internedKey);
                    if (!descriptor) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }
                    if (!descriptor->Removable) {
                        ThrowCannotRemoveAttribute(key);
                    }

                    permissionValidator.Validate(descriptor->ModifyPermission);

                    if (!GuardedRemoveBuiltinAttribute(internedKey)) {
                        ThrowNoSuchBuiltinAttribute(key);
                    }
                }
            } else {
                if (customYson) {
                    auto modifyPermission = builtinAttributeProvider
                        ? builtinAttributeProvider->GetCustomAttributeModifyPermission()
                        : EPermission::Write;
                    permissionValidator.Validate(modifyPermission);

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

                    auto descriptor = builtinAttributeProvider->FindBuiltinAttributeDescriptor(internedKey);
                    if (!descriptor) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }

                    if (!descriptor->Writable) {
                        ThrowCannotSetBuiltinAttribute(key);
                    }

                    permissionValidator.Validate(descriptor->ModifyPermission);

                    auto builtinYson = builtinAttributeProvider->FindBuiltinAttribute(internedKey);
                    if (!builtinYson) {
                        if (force) {
                            return;
                        }
                        ThrowNoSuchAttribute(key);
                    }

                    auto builtinNode = ConvertToNode(builtinYson);
                    SyncYPathRemove(builtinNode, TYPath(tokenizer.GetInput()));
                    auto updatedSystemYson = ConvertToYsonStringStable(builtinNode);

                    if (!GuardedSetBuiltinAttribute(internedKey, updatedSystemYson)) {
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

void TSupportsAttributes::SetAttributes(const TYPath& path, TReqMultisetAttributes* request, TRspMultisetAttributes* /* response */)
{
    for (const auto& subrequest : request->subrequests()) {
        const auto& attribute = subrequest.attribute();
        const auto& value = subrequest.value();
        if (attribute.empty()) {
            THROW_ERROR_EXCEPTION("Empty attribute names are not allowed");
        }

        TYPath attributePath;
        if (path.empty()) {
            attributePath = attribute;
        } else {
            attributePath = path + "/" + attribute;
        }

        DoSetAttribute(attributePath, TYsonString(value));
    }
}

IAttributeDictionary* TSupportsAttributes::GetCombinedAttributes()
{
    return CombinedAttributes_.Get();
}

IAttributeDictionary* TSupportsAttributes::GetCustomAttributes()
{
    return nullptr;
}

ISystemAttributeProvider* TSupportsAttributes::GetBuiltinAttributeProvider()
{
    return nullptr;
}

bool TSupportsAttributes::GuardedGetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer)
{
    auto* provider = GetBuiltinAttributeProvider();

    try {
        return provider->GetBuiltinAttribute(key, consumer);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error getting builtin attribute %Qv",
            ToYPathLiteral(key.Unintern()))
            << ex;
    }
}

bool TSupportsAttributes::GuardedSetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& yson)
{
    auto* provider = GetBuiltinAttributeProvider();

    try {
        return provider->SetBuiltinAttribute(key, yson);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error setting builtin attribute %Qv",
            ToYPathLiteral(key.Unintern()))
            << ex;
    }
}

bool TSupportsAttributes::GuardedRemoveBuiltinAttribute(TInternedAttributeKey key)
{
    auto* provider = GetBuiltinAttributeProvider();

    try {
        return provider->RemoveBuiltinAttribute(key);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error removing builtin attribute %Qv",
            ToYPathLiteral(key.Unintern()))
            << ex;
    }
}

void TSupportsAttributes::ValidateAttributeKey(TStringBuf key) const
{
    if (key.empty()) {
        THROW_ERROR_EXCEPTION("Attribute key cannot be empty");
    }
}

////////////////////////////////////////////////////////////////////////////////

const THashSet<TInternedAttributeKey>& TSystemBuiltinAttributeKeysCache::GetBuiltinAttributeKeys(
    ISystemAttributeProvider* provider)
{
    if (!Initialized_) {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ListSystemAttributes(&descriptors);
        BuiltinKeys_.reserve(descriptors.size());
        for (const auto& descriptor : descriptors) {
            if (!descriptor.Custom) {
                YT_VERIFY(BuiltinKeys_.insert(descriptor.InternedKey).second);
            }
        }
        Initialized_ = true;
    }
    return BuiltinKeys_;
}

////////////////////////////////////////////////////////////////////////////////

const THashSet<TString>& TSystemCustomAttributeKeysCache::GetCustomAttributeKeys(
    ISystemAttributeProvider* provider)
{
    if (!Initialized_) {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ListSystemAttributes(&descriptors);
        CustomKeys_.reserve(descriptors.size());
        for (const auto& descriptor : descriptors) {
            if (descriptor.Custom) {
                YT_VERIFY(CustomKeys_.insert(descriptor.InternedKey.Unintern()).second);
            }
        }
        Initialized_ = true;
    }
    return CustomKeys_;
}

////////////////////////////////////////////////////////////////////////////////

const THashSet<TString>& TOpaqueAttributeKeysCache::GetOpaqueAttributeKeys(
    ISystemAttributeProvider* provider)
{
    if (!Initialized_) {
        std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
        provider->ListSystemAttributes(&descriptors);
        OpaqueKeys_.reserve(descriptors.size());
        for (const auto& descriptor : descriptors) {
            if (descriptor.Opaque) {
                YT_VERIFY(OpaqueKeys_.insert(descriptor.InternedKey.Unintern()).second);
            }
        }
        Initialized_ = true;
    }
    return OpaqueKeys_;
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


    virtual void OnMyKeyedItem(TStringBuf key) override
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

void TNodeSetterBase::OnMyStringScalar(TStringBuf /*value*/)
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
        NLogging::ELogLevel logLevel)
        : TServiceContextBase(
            std::move(requestMessage),
            std::move(logger),
            logLevel)
    { }

    TYPathServiceContext(
        std::unique_ptr<TRequestHeader> requestHeader,
        TSharedRefArray requestMessage,
        NLogging::TLogger logger,
        NLogging::ELogLevel logLevel)
        : TServiceContextBase(
            std::move(requestHeader),
            std::move(requestMessage),
            std::move(logger),
            logLevel)
    { }

protected:
    std::optional<NProfiling::TWallTimer> Timer_;
    const NProto::TYPathHeaderExt* YPathExt_ = nullptr;


    const NProto::TYPathHeaderExt& GetYPathExt()
    {
        if (!YPathExt_) {
            YPathExt_ = &RequestHeader_->GetExtension(NProto::TYPathHeaderExt::ypath_header_ext);
        }
        return *YPathExt_;
    }


    virtual void DoReply() override
    { }

    virtual void LogRequest() override
    {
        const auto& ypathExt = GetYPathExt();

        TStringBuilder builder;
        builder.AppendFormat("%v.%v %v <- ",
            GetService(),
            GetMethod(),
            ypathExt.target_path());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        auto requestId = GetRequestId();
        if (requestId) {
            delimitedBuilder->AppendFormat("RequestId: %v", requestId);
        }

        delimitedBuilder->AppendFormat("Mutating: %v", ypathExt.mutating());

        auto mutationId = GetMutationId();
        if (mutationId) {
            delimitedBuilder->AppendFormat("MutationId: %v", mutationId);
        }

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        delimitedBuilder->AppendFormat("Retry: %v", IsRetry());

        for (const auto& info : RequestInfos_){
            delimitedBuilder->AppendString(info);
        }

        auto logMessage = builder.Flush();
        NTracing::AddTag(RequestInfoAnnotation, logMessage);
        YT_LOG_DEBUG(logMessage);

        Timer_.emplace();
    }

    virtual void LogResponse() override
    {
        const auto& ypathExt = GetYPathExt();

        TStringBuilder builder;
        builder.AppendFormat("%v.%v %v -> ",
            GetService(),
            GetMethod(),
            ypathExt.target_path());

        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

        auto requestId = GetRequestId();
        if (requestId) {
            delimitedBuilder->AppendFormat("RequestId: %v", requestId);
        }

        delimitedBuilder->AppendFormat("Mutating: %v", ypathExt.mutating());

        if (RequestHeader_->has_user()) {
            delimitedBuilder->AppendFormat("User: %v", RequestHeader_->user());
        }

        for (const auto& info : ResponseInfos_) {
            delimitedBuilder->AppendString(info);
        }

        if (Timer_) {
            delimitedBuilder->AppendFormat("WallTime: %v", Timer_->GetElapsedTime());
        }

        delimitedBuilder->AppendFormat("Error: %v", Error_);

        auto logMessage = builder.Flush();
        NTracing::AddTag(ResponseInfoAnnotation, logMessage);
        YT_LOG_DEBUG(logMessage);
    }
};

IServiceContextPtr CreateYPathContext(
    TSharedRefArray requestMessage,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
{
    YT_ASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestMessage),
        std::move(logger),
        logLevel);
}

IServiceContextPtr CreateYPathContext(
    std::unique_ptr<TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
{
    YT_ASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestHeader),
        std::move(requestMessage),
        std::move(logger),
        logLevel);
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
        YT_ABORT();
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
        const std::optional<std::vector<TString>>& attributeKeys,
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

} // namespace NYT::NYTree
