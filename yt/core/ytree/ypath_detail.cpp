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

void TYPathServiceBase::Invoke(IServiceContextPtr context)
{
    GuardedInvoke(context);
}

void TYPathServiceBase::GuardedInvoke(IServiceContextPtr context)
{
    try {
        if (!DoInvoke(context)) {
            ThrowVerbNotSuppored(context->GetVerb());
        }
    } catch (const std::exception& ex) {
        context->Reply(ex);
    }
}

bool TYPathServiceBase::DoInvoke(IServiceContextPtr /*context*/)
{
    return false;
}

Stroka TYPathServiceBase::GetLoggingCategory() const
{
    return Logger.GetCategory();
}

void TYPathServiceBase::SerializeAttributes(
    NYson::IYsonConsumer* /*consumer*/,
    const TAttributeFilter& /*filter*/,
    bool /*sortKeys*/)
{ }

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_SUPPORTS_VERB_RESOLVE(verb, onPathError) \
    DEFINE_RPC_SERVICE_METHOD(TSupports##verb, verb) \
    { \
        NYPath::TTokenizer tokenizer(GetRequestYPath(context)); \
        switch (tokenizer.Advance()) { \
            case NYPath::ETokenType::EndOfStream: \
                verb##Self(request, response, context); \
                break; \
            \
            case NYPath::ETokenType::Slash: \
                if (tokenizer.Advance() == NYPath::ETokenType::At) { \
                    verb##Attribute(tokenizer.GetSuffix(), request, response, context); \
                } else { \
                    verb##Recursive(tokenizer.GetInput(), request, response, context); \
                } \
                break; \
            \
            default: \
                onPathError \
        } \
    }

#define IMPLEMENT_SUPPORTS_VERB(verb) \
    IMPLEMENT_SUPPORTS_VERB_RESOLVE( \
        verb, \
        { \
            tokenizer.ThrowUnexpected(); \
            YUNREACHABLE(); \
        } \
    ) \
    \
    void TSupports##verb::verb##Attribute(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb##Ptr context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        ThrowVerbNotSuppored(context->GetVerb(), Stroka("attribute")); \
    } \
    \
    void TSupports##verb::verb##Self(TReq##verb* request, TRsp##verb* response, TCtx##verb##Ptr context) \
    { \
        UNUSED(request); \
        UNUSED(response); \
        ThrowVerbNotSuppored(context->GetVerb(), Stroka("self")); \
    } \
    \
    void TSupports##verb::verb##Recursive(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb##Ptr context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        ThrowVerbNotSuppored(context->GetVerb(), Stroka("recursive")); \
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
    context->SetResponseInfo("Result: %s", ~FormatBool(value));
    context->Reply();
}

void TSupportsExists::ExistsAttribute(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    TCtxExistsPtr context)
{
    context->SetRequestInfo("");

    Reply(context, false);
}

void TSupportsExists::ExistsSelf(
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    TCtxExistsPtr context)
{
    context->SetRequestInfo("");

    Reply(context, true);
}

void TSupportsExists::ExistsRecursive(
    const TYPath& /*path*/,
    TReqExists* /*request*/,
    TRspExists* /*response*/,
    TCtxExistsPtr context)
{
    context->SetRequestInfo("");

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

static TFuture<bool> TrueFuture = MakeFuture(true);
static TFuture<bool> FalseFuture = MakeFuture(false);

IYPathService::TResolveResult TSupportsAttributes::ResolveAttributes(
    const TYPath& path,
    IServiceContextPtr context)
{
    const auto& verb = context->GetVerb();
    if (verb != "Get" &&
        verb != "Set" &&
        verb != "List" &&
        verb != "Remove" &&
        verb != "Exists")
    {
        ThrowVerbNotSuppored(verb);
    }

    return TResolveResult::Here("/@" + path);
}

TFuture< TErrorOr<TYsonString> > TSupportsAttributes::DoFindAttribute(const Stroka& key)
{
    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();

    if (userAttributes) {
        auto userYson = userAttributes->FindYson(key);
        if (userYson) {
            return MakeFuture(TErrorOr<TYsonString>(userYson.Get()));
        }
    }

    if (systemAttributeProvider) {
        TStringStream syncStream;
        NYson::TYsonWriter syncWriter(&syncStream);
        if (systemAttributeProvider->GetSystemAttribute(key, &syncWriter)) {
            TYsonString systemYson(syncStream.Str());
            return MakeFuture(TErrorOr<TYsonString>(systemYson));
        }

        auto onAsyncAttribute = [] (
            TStringStream* stream,
            NYson::TYsonWriter* writer,
            TError error) ->
            TErrorOr<TYsonString>
        {
            if (error.IsOK()) {
                return TYsonString(stream->Str());
            } else {
                return error;
            }
        };

        std::unique_ptr<TStringStream> asyncStream(new TStringStream());
        std::unique_ptr<NYson::TYsonWriter> asyncWriter(new NYson::TYsonWriter(asyncStream.get()));
        auto asyncResult = systemAttributeProvider->GetSystemAttributeAsync(key, asyncWriter.get());
        if (asyncResult) {
            return asyncResult.Apply(BIND(
                onAsyncAttribute,
                Owned(asyncStream.release()),
                Owned(asyncWriter.release())));
        }
    }

    return Null;
}

TErrorOr<TYsonString> TSupportsAttributes::DoGetAttributeFragment(
    const TYPath& path,
    TErrorOr<TYsonString> wholeYsonOrError)
{
    if (!wholeYsonOrError.IsOK()) {
        return wholeYsonOrError;
    }
    auto node = ConvertToNode<TYsonString>(wholeYsonOrError.Value());
    try {
        return SyncYPathGet(node, path, TAttributeFilter::All);
    } catch (const std::exception& ex) {
        return ex;
    }
}

TFuture< TErrorOr<TYsonString> > TSupportsAttributes::DoGetAttribute(const TYPath& path)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        TStringStream stream;
        NYson::TYsonWriter writer(&stream);

        writer.OnBeginMap();

        if (systemAttributeProvider) {
            std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
            systemAttributeProvider->ListSystemAttributes(&systemAttributes);
            for (const auto& attribute : systemAttributes) {
                if (attribute.IsPresent) {
                    writer.OnKeyedItem(attribute.Key);
                    if (attribute.IsOpaque) {
                        writer.OnEntity();
                    } else {
                        YCHECK(systemAttributeProvider->GetSystemAttribute(attribute.Key, &writer));
                    }
                }
            }
        }

        if (userAttributes) {
            for (const auto& key : userAttributes->List()) {
                writer.OnKeyedItem(key);
                Consume(userAttributes->GetYson(key), &writer);
            }
        }

        writer.OnEndMap();
        TYsonString yson(stream.Str());
        return MakeFuture(TErrorOr<TYsonString>(yson));
    } else {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        auto ysonOrError = DoFindAttribute(key);
        if (!ysonOrError) {
            return MakeFuture(TErrorOr<TYsonString>(TError(
                NYTree::EErrorCode::ResolveError,
                "Attribute %s is not found",
                ~ToYPathLiteral(key).Quote())));
        }

        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            return ysonOrError;
        }

        auto suffixPath = tokenizer.GetInput();
        return ysonOrError.Apply(BIND(&TSupportsAttributes::DoGetAttributeFragment, suffixPath));
   }
}

void TSupportsAttributes::GetAttribute(
    const TYPath& path,
    TReqGet* request,
    TRspGet* response,
    TCtxGetPtr context)
{
    DoGetAttribute(path).Subscribe(BIND([=] (TErrorOr<TYsonString> ysonOrError) {
        if (ysonOrError.IsOK()) {
            response->set_value(ysonOrError.Value().Data());
            context->Reply();
        } else {
            context->Reply(ysonOrError);
        }
    }));
}

TErrorOr<TYsonString> TSupportsAttributes::DoListAttributeFragment(
    const TYPath& path,
    TErrorOr<TYsonString> wholeYsonOrError)
{
    if (!wholeYsonOrError.IsOK()) {
        return wholeYsonOrError;
    }

    auto node = ConvertToNode(wholeYsonOrError.Value());

    std::vector<Stroka> listedKeys;
    try {
        listedKeys = SyncYPathList(node, path);
    } catch (const std::exception& ex) {
        return ex;
    }

    TStringStream stream;
    NYson::TYsonWriter writer(&stream);
    writer.OnBeginList();
    for (const auto& listedKey : listedKeys) {
        writer.OnListItem();
        writer.OnStringScalar(listedKey);
    }
    writer.OnEndList();

    return TYsonString(stream.Str());
}

TFuture< TErrorOr<TYsonString> > TSupportsAttributes::DoListAttribute(const TYPath& path)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Read);

    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        TStringStream stream;
        NYson::TYsonWriter writer(&stream);
        writer.OnBeginList();

        if (userAttributes) {
            auto userKeys = userAttributes->List();
            for (const auto& key : userKeys) {
                writer.OnListItem();
                writer.OnStringScalar(key);
            }
        }

        if (systemAttributeProvider) {
            std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
            systemAttributeProvider->ListSystemAttributes(&systemAttributes);
            for (const auto& attribute : systemAttributes) {
                if (attribute.IsPresent) {
                    writer.OnListItem();
                    writer.OnStringScalar(attribute.Key);
                }
            }
        }

        writer.OnEndList();

        TYsonString yson(stream.Str());
        return MakeFuture(TErrorOr<TYsonString>(yson));
    } else  {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        auto ysonOrError = DoFindAttribute(key);
        if (!ysonOrError) {
            return MakeFuture(TErrorOr<TYsonString>(TError(
                NYTree::EErrorCode::ResolveError,
                "Attribute %s is not found",
                ~ToYPathLiteral(key))));
        }

        auto pathSuffix = tokenizer.GetSuffix();
        return ysonOrError.Apply(BIND(&TSupportsAttributes::DoListAttributeFragment, pathSuffix));
    }
}

void TSupportsAttributes::ListAttribute(
    const TYPath& path,
    TReqList* /*request*/,
    TRspList* response,
    TCtxListPtr context)
{
    DoListAttribute(path).Subscribe(BIND([=] (TErrorOr<TYsonString> ysonOrError) {
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
    TErrorOr<TYsonString> wholeYsonOrError)
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

    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();

    NYPath::TTokenizer tokenizer(path);
    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        return TrueFuture;
    }

    tokenizer.Expect(NYPath::ETokenType::Literal);
    auto key = tokenizer.GetLiteralValue();

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        if (userAttributes) {
            auto userYson = userAttributes->FindYson(key);
            if (userYson) {
                return TrueFuture;
            }
        }

        if (systemAttributeProvider) {
            std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
            systemAttributeProvider->ListSystemAttributes(&systemAttributes);
            for (const auto& attribute : systemAttributes) {
                if (attribute.Key == key && attribute.IsPresent) {
                    return TrueFuture;
                }
            }
        }

        return FalseFuture;
    } else {
        auto ysonOrError = DoFindAttribute(key);
        if (!ysonOrError) {
            return FalseFuture;
        }

        auto pathSuffix = tokenizer.GetInput();
        return ysonOrError.Apply(BIND(&TSupportsAttributes::DoExistsAttributeFragment, pathSuffix));
    }
}

void TSupportsAttributes::ExistsAttribute(
    const TYPath& path,
    TReqExists* /*request*/,
    TRspExists* response,
    TCtxExistsPtr context)
{
    context->SetRequestInfo("");

    DoExistsAttribute(path).Subscribe(BIND([=] (bool result) {
        response->set_value(result);
        context->SetResponseInfo("Result: %s", ~FormatBool(result));
        context->Reply();
    }));
}

void TSupportsAttributes::DoSetAttribute(const TYPath& path, const TYsonString& newYson)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();

    NYPath::TTokenizer tokenizer(path);

    if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
        auto newAttributes = ConvertToAttributes(newYson);

        if (systemAttributeProvider) {
            std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
            systemAttributeProvider->ListSystemAttributes(&systemAttributes);

            for (const auto& attribute : systemAttributes) {
                Stroka key(attribute.Key);
                auto newAttributeYson = newAttributes->FindYson(key);
                if (newAttributeYson) {
                    if (!attribute.IsPresent) {
                        ThrowCannotSetSystemAttribute(key);
                    }
                    GuardedSetSystemAttribute(key, newAttributeYson.Get());
                    YCHECK(newAttributes->Remove(key));
                }
            }
        }

        auto newUserKeys = newAttributes->List();
        std::sort(newUserKeys.begin(), newUserKeys.end());

        if (!userAttributes) {
             if (!newUserKeys.empty()) {
                 THROW_ERROR_EXCEPTION("User attributes are not supported");
             }
             return;
        }

        auto oldUserKeys = userAttributes->List();
        std::sort(oldUserKeys.begin(), oldUserKeys.end());

        for (const auto& key : newUserKeys) {
            auto value = newAttributes->GetYson(key);
            userAttributes->SetYson(key, value);
        }

        for (const auto& key : oldUserKeys) {
            if (!newAttributes->FindYson(key)) {
                userAttributes->Remove(key);
            }
        }
    } else {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        if (key.Empty()) {
            THROW_ERROR_EXCEPTION("Attribute key cannot be empty");
        }

        const ISystemAttributeProvider::TAttributeInfo* attribute = nullptr;
        std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
        if (systemAttributeProvider) {
            systemAttributeProvider->ListSystemAttributes(&systemAttributes);
            for (const auto& currentAttribute : systemAttributes) {
                if (currentAttribute.Key == key) {
                    attribute = &currentAttribute;
                    break;
                }
            }
        }

        if (attribute) {
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                GuardedSetSystemAttribute(key, newYson);
            } else {
                TStringStream stream;
                NYson::TYsonWriter writer(&stream);
                if (!systemAttributeProvider->GetSystemAttribute(key, &writer)) {
                    ThrowNoSuchSystemAttribute(key);
                }

                TYsonString oldWholeYson(stream.Str());
                auto wholeNode = ConvertToNode(oldWholeYson);
                SyncYPathSet(wholeNode, tokenizer.GetInput(), newYson);
                auto newWholeYson = ConvertToYsonStringStable(wholeNode);

                GuardedSetSystemAttribute(key, newWholeYson);
            }
        } else {
            if (!userAttributes) {
                THROW_ERROR_EXCEPTION("User attributes are not supported");
            }
            
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                userAttributes->SetYson(key, newYson);
            } else {
                auto oldWholeYson = userAttributes->FindYson(key);
                if (!oldWholeYson) {
                    ThrowNoSuchUserAttribute(key);
                }

                auto wholeNode = ConvertToNode(oldWholeYson.Get());
                SyncYPathSet(wholeNode, tokenizer.GetInput(), newYson);
                auto newWholeYson = ConvertToYsonStringStable(wholeNode);

                userAttributes->SetYson(key, newWholeYson);
            }
        }
    }

    OnUserAttributesUpdated();
}

void TSupportsAttributes::SetAttribute(
    const TYPath& path,
    TReqSet* request,
    TRspSet* /*response*/,
    TCtxSetPtr context)
{
    context->SetRequestInfo("");

    DoSetAttribute(path, TYsonString(request->value()));
    
    context->Reply();
}

void TSupportsAttributes::DoRemoveAttribute(const TYPath& path)
{
    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();

    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);

    if (tokenizer.GetToken() == WildcardToken) {
        if (userAttributes) {
            auto userKeys = userAttributes->List();
            std::sort(userKeys.begin(), userKeys.end());
            for (const auto& key : userKeys) {
                YCHECK(userAttributes->Remove(key));
            }
        }
    } else {
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = tokenizer.GetLiteralValue();

        auto userYson = userAttributes ? userAttributes->FindYson(key) : TNullable<TYsonString>(Null);
        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            if (!userYson) {
                if (systemAttributeProvider) {
                    auto* attributeInfo = systemAttributeProvider->FindSystemAttributeInfo(key);
                    if (attributeInfo) {
                        ThrowCannotRemoveAttribute(key);
                    } else {
                        ThrowNoSuchUserAttribute(key);
                    }
                } else {
                    ThrowNoSuchUserAttribute(key);
                }
            }

            YCHECK(userAttributes->Remove(key));
        } else {
            if (userYson) {
                auto userNode = ConvertToNode(userYson);
                SyncYPathRemove(userNode, tokenizer.GetInput());
                auto updatedUserYson = ConvertToYsonStringStable(userNode);
                userAttributes->SetYson(key, updatedUserYson);
            } else {
                TStringStream stream;
                NYson::TYsonWriter writer(&stream);
                if (!systemAttributeProvider || !systemAttributeProvider->GetSystemAttribute(key, &writer)) {
                    ThrowNoSuchSystemAttribute(key);
                }

                TYsonString systemYson(stream.Str());
                auto systemNode = ConvertToNode(systemYson);
                SyncYPathRemove(systemNode, tokenizer.GetInput());
                auto updatedSystemYson = ConvertToYsonStringStable(systemNode);

                GuardedSetSystemAttribute(key, updatedSystemYson);
            }
        }
    }

    OnUserAttributesUpdated();
}

void TSupportsAttributes::RemoveAttribute(
    const TYPath& path,
    TReqRemove* /*request*/,
    TRspRemove* /*response*/,
    TCtxRemovePtr context)
{
    context->SetRequestInfo("");

    DoRemoveAttribute(path);

    context->Reply();
}

void TSupportsAttributes::ValidateUserAttributeUpdate(
    const Stroka& /*key*/,
    const TNullable<TYsonString>& /*oldValue*/,
    const TNullable<TYsonString>& /*newValue*/)
{ }

void TSupportsAttributes::OnUserAttributesUpdated()
{ }

IAttributeDictionary* TSupportsAttributes::GetUserAttributes()
{
    return nullptr;
}

ISystemAttributeProvider* TSupportsAttributes::GetSystemAttributeProvider()
{
    return nullptr;
}

void TSupportsAttributes::GuardedSetSystemAttribute(const Stroka& key, const TYsonString& yson)
{
    bool result;
    try {
        result = GetSystemAttributeProvider()->SetSystemAttribute(key, yson);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error setting system attribute %s",
            ~ToYPathLiteral(key).Quote())
            << ex;
    }

    if (!result) {
        ThrowCannotSetSystemAttribute(key);
    }
}

void TSupportsAttributes::GuardedValidateUserAttributeUpdate(
    const Stroka& key,
    const TNullable<TYsonString>& oldValue,
    const TNullable<TYsonString>& newValue)
{
    try {
        ValidateUserAttributeUpdate(key, oldValue, newValue);
    } catch (const std::exception& ex) {
        if (newValue) {
            THROW_ERROR_EXCEPTION("Error setting user attribute %s",
                ~ToYPathLiteral(key).Quote())
                << ex;
        } else {
            THROW_ERROR_EXCEPTION("Error removing user attribute %s",
                ~ToYPathLiteral(key).Quote())
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
        : Attributes(attributes)
    { }

private:
    IAttributeDictionary* Attributes;

    TStringStream AttributeStream;
    std::unique_ptr<NYson::TYsonWriter> AttributeWriter;

    virtual void OnMyKeyedItem(const TStringBuf& key) override
    {
        Stroka localKey(key);
        AttributeWriter.reset(new NYson::TYsonWriter(&AttributeStream));
        Forward(
            AttributeWriter.get(),
            BIND ([=] () {
                AttributeWriter.reset();
                Attributes->SetYson(localKey, TYsonString(AttributeStream.Str()));
                AttributeStream.clear();
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INode* node, ITreeBuilder* builder)
    : Node(node)
    , TreeBuilder(builder)
    , NodeFactory(node->CreateFactory())
{
    Node->MutableAttributes()->Clear();
}

TNodeSetterBase::~TNodeSetterBase()
{ }

void TNodeSetterBase::ThrowInvalidType(ENodeType actualType)
{
    THROW_ERROR_EXCEPTION("Invalid node type: expected %s, actual %s",
        ~FormatEnum(GetExpectedType()).Quote(),
        ~FormatEnum(actualType).Quote());
}

void TNodeSetterBase::OnMyStringScalar(const TStringBuf& /*value*/)
{
    ThrowInvalidType(ENodeType::String);
}

void TNodeSetterBase::OnMyIntegerScalar(i64 /*value*/)
{
    ThrowInvalidType(ENodeType::Integer);
}

void TNodeSetterBase::OnMyDoubleScalar(double /*value*/)
{
    ThrowInvalidType(ENodeType::Double);
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
    AttributesSetter.reset(new TAttributesSetter(Node->MutableAttributes()));
    Forward(AttributesSetter.get(), TClosure(), NYson::EYsonType::MapFragment);
}

void TNodeSetterBase::OnMyEndAttributes()
{
    AttributesSetter.reset();
}

void TNodeSetterBase::Commit()
{
    NodeFactory->Commit();
}

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceContext
    : public TServiceContextBase
{
public:
    TYPathServiceContext(
        TSharedRefArray requestMessage,
        TYPathResponseHandler responseHandler,
        const Stroka& loggingCategory)
        : TServiceContextBase(std::move(requestMessage))
        , ResponseHandler(std::move(responseHandler))
        , Logger(loggingCategory)
    { }

    TYPathServiceContext(
        std::unique_ptr<TRequestHeader> requestHeader,
        TSharedRefArray requestMessage,
        TYPathResponseHandler responseHandler,
        const Stroka& loggingCategory)
        : TServiceContextBase(
            std::move(requestHeader),
            std::move(requestMessage))
        , ResponseHandler(std::move(responseHandler))
        , Logger(loggingCategory)
    { }

protected:
    TYPathResponseHandler ResponseHandler;
    NLog::TLogger Logger;

    virtual void DoReply() override
    {
        if (ResponseHandler) {
            ResponseHandler.Run(GetResponseMessage());
        }
    }

    virtual void LogRequest() override
    {
        Stroka str;
        AppendInfo(str, RequestInfo_);
        LOG_DEBUG("%s:%s %s <- %s",
            ~GetService(),
            ~GetVerb(),
            ~GetRequestYPath(this),
            ~str);
    }

    virtual void LogResponse(const TError& error) override
    {
        Stroka str;
        AppendInfo(str, Sprintf("Error: %s", ~ToString(error)));
        AppendInfo(str, ResponseInfo_);
        LOG_DEBUG("%s:%s %s -> %s",
            ~GetService(),
            ~GetVerb(),
            ~GetRequestYPath(this),
            ~str);
    }

};

IServiceContextPtr CreateYPathContext(
    TSharedRefArray requestMessage,
    const Stroka& loggingCategory,
    TYPathResponseHandler responseHandler)
{
    YASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestMessage),
        std::move(responseHandler),
        loggingCategory);
}

IServiceContextPtr CreateYPathContext(
    std::unique_ptr<TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    const Stroka& loggingCategory,
    TYPathResponseHandler responseHandler)
{
    YASSERT(requestMessage);

    return New<TYPathServiceContext>(
        std::move(requestHeader),
        std::move(requestMessage),
        std::move(responseHandler),
        loggingCategory);
}

////////////////////////////////////////////////////////////////////////////////

class TRootService
    : public IYPathService
{
public:
    explicit TRootService(IYPathServicePtr underlyingService)
        : UnderlyingService(underlyingService)
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

        return TResolveResult::There(UnderlyingService, tokenizer.GetSuffix());
    }

    virtual Stroka GetLoggingCategory() const override
    {
        return UnderlyingService->GetLoggingCategory();
    }

    // TODO(panin): remove this when getting rid of IAttributeProvider
    virtual void SerializeAttributes(
        NYson::IYsonConsumer* consumer,
        const TAttributeFilter& filter,
        bool sortKeys) override
    {
        UnderlyingService->SerializeAttributes(consumer, filter, sortKeys);
    }

private:
    IYPathServicePtr UnderlyingService;

};

IYPathServicePtr CreateRootService(IYPathServicePtr underlyingService)
{
    return New<TRootService>(underlyingService);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
