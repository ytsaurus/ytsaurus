#include "stdafx.h"
#include "ypath_detail.h"

#include "ypath_client.h"
#include "serialize.h"
#include "tokenizer.h"
#include "ypath_format.h"

#include <ytlib/rpc/rpc.pb.h>

#include <ytlib/bus/message.h>
#include <ytlib/rpc/server_detail.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NYTree {

using namespace NProto;
using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TYPathServiceBase::Resolve(const TYPath& path, const Stroka& verb)
{
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    switch (tokenizer.GetCurrentType()) {
        case ETokenType::EndOfStream:
            return ResolveSelf(TYPath(tokenizer.GetCurrentSuffix()), verb);

        case PathSeparatorToken:
            tokenizer.ParseNext();
            if (tokenizer.GetCurrentType() == GoToAttributesToken) {
                return ResolveAttributes(TYPath(tokenizer.GetCurrentSuffix()), verb);
            } else {
                return ResolveRecursive(TYPath(tokenizer.CurrentInput()), verb);
            }

        default:
            ThrowUnexpectedToken(tokenizer.CurrentToken());
            YUNREACHABLE();
    }
}

IYPathService::TResolveResult TYPathServiceBase::ResolveSelf(const TYPath& path, const Stroka& verb)
{
    UNUSED(verb);
    return TResolveResult::Here(path);
}

IYPathService::TResolveResult TYPathServiceBase::ResolveAttributes(const TYPath& path, const Stroka& verb)
{
    UNUSED(path);
    UNUSED(verb);
    ythrow yexception() << "Object cannot have attributes";
}

IYPathService::TResolveResult TYPathServiceBase::ResolveRecursive(const TYPath& path, const Stroka& verb)
{
    UNUSED(path);
    UNUSED(verb);
    ythrow yexception() << "Object cannot have children";
}

void TYPathServiceBase::Invoke(IServiceContextPtr context)
{
    GuardedInvoke(context);
}

void TYPathServiceBase::GuardedInvoke(IServiceContextPtr context)
{
    try {
        DoInvoke(context);
    } catch (const TServiceException& ex) {
        context->Reply(ex.GetError());
    }
    catch (const std::exception& ex) {
        context->Reply(TError(ex.what()));
    }
}

void TYPathServiceBase::DoInvoke(IServiceContextPtr context)
{
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) <<
        "Verb is not supported";
}

Stroka TYPathServiceBase::GetLoggingCategory() const
{
    return Logger.GetCategory();
}

bool TYPathServiceBase::IsWriteRequest(IServiceContextPtr context) const
{
    UNUSED(context);
    return false;
}

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_SUPPORTS_VERB(verb) \
    DEFINE_RPC_SERVICE_METHOD(TSupports##verb, verb) \
    { \
        TTokenizer tokenizer(context->GetPath()); \
        tokenizer.ParseNext(); \
        switch (tokenizer.GetCurrentType()) { \
            case ETokenType::EndOfStream: \
                verb##Self(request, response, ~context); \
                break; \
            \
            case PathSeparatorToken: \
                tokenizer.ParseNext(); \
                if (tokenizer.GetCurrentType() == GoToAttributesToken) { \
                    verb##Attribute(TYPath(tokenizer.GetCurrentSuffix()), request, response, ~context); \
                } else { \
                    verb##Recursive(TYPath(tokenizer.CurrentInput()), request, response, ~context); \
                } \
                break; \
            \
            default: \
                ThrowUnexpectedToken(tokenizer.CurrentToken()); \
                YUNREACHABLE(); \
        } \
    } \
    \
    void TSupports##verb::verb##Self(TReq##verb* request, TRsp##verb* response, TCtx##verb* context) \
    { \
        UNUSED(request); \
        UNUSED(response); \
        UNUSED(context); \
        ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported"; \
    } \
    \
    void TSupports##verb::verb##Recursive(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb* context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        UNUSED(context); \
        ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported"; \
    } \
    \
    void TSupports##verb::verb##Attribute(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb* context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        UNUSED(context); \
        ythrow TServiceException(EErrorCode::NoSuchVerb) << "Verb is not supported"; \
    }

IMPLEMENT_SUPPORTS_VERB(Get)
IMPLEMENT_SUPPORTS_VERB(Set)
IMPLEMENT_SUPPORTS_VERB(List)
IMPLEMENT_SUPPORTS_VERB(Remove)

#undef IMPLEMENT_SUPPORTS_VERB

////////////////////////////////////////////////////////////////////////////////

namespace {

TYson DoGetAttribute(
    IAttributeDictionary* userAttributes,
    ISystemAttributeProvider* systemAttributeProvider,
    const Stroka& key,
    bool* isSystem = NULL)
{
    if (systemAttributeProvider) {
        TStringStream stream;
        TYsonWriter writer(&stream);
        if (systemAttributeProvider->GetSystemAttribute(key, &writer)) {
            if (isSystem) {
                *isSystem = true;
            }
            return stream.Str();
        }
    }

    if (!userAttributes) {
        ythrow yexception() << "User attributes are not supported";
    }

    if (isSystem) {
        *isSystem = false;
    }

    return userAttributes->GetYson(key);
}

TNullable<TYson> DoFindAttribute(
    IAttributeDictionary* userAttributes,
    ISystemAttributeProvider* systemAttributeProvider,
    const Stroka& key,
    bool* isSystem = NULL)
{
    if (systemAttributeProvider) {
        TStringStream stream;
        TYsonWriter writer(&stream);
        if (systemAttributeProvider->GetSystemAttribute(key, &writer)) {
            if (isSystem) {
                *isSystem = true;
            }
            return stream.Str();
        }
    }

    if (!userAttributes) {
        return Null;
    }

    if (isSystem) {
        *isSystem = false;
    }

    return userAttributes->FindYson(key);
}

void DoSetAttribute(
    IAttributeDictionary* userAttributes,
    ISystemAttributeProvider* systemAttributeProvider,
    const Stroka& key,
    const TYson& value,
    bool isSystem)
{
    if (isSystem) {
        YASSERT(systemAttributeProvider);
        if (!systemAttributeProvider->SetSystemAttribute(key, value)) {
            ythrow yexception() << Sprintf("System attribute %s cannot be set", ~key.Quote());
        }
    } else {
        if (!userAttributes) {
            ythrow yexception() << "User attributes are not supported";
        }
        userAttributes->SetYson(key, value);
    }
}

void DoSetAttribute(
    IAttributeDictionary* userAttributes,
    ISystemAttributeProvider* systemAttributeProvider,
    const Stroka& key,
    const TYson& value)
{
    if (systemAttributeProvider) {
        if (systemAttributeProvider->SetSystemAttribute(key, value)) {
            return;
        }

        // Check for system attributes
        yvector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
        systemAttributeProvider->GetSystemAttributes(&systemAttributes);
                
        FOREACH (const auto& attribute, systemAttributes) {
            if (attribute.Key == key) {
                ythrow yexception() << Sprintf("System attribute %s cannot be set", ~key.Quote());
            }
        }
    }

    if (!userAttributes) {
        ythrow yexception() << "User attributes are not supported";
    }

    userAttributes->SetYson(key, value);
}

std::vector<Stroka> DoListAttributes(
    IAttributeDictionary* userAttributes,
    ISystemAttributeProvider* systemAttributeProvider)
{
    std::vector<Stroka> keys;

    if (systemAttributeProvider) {
        yvector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
        systemAttributeProvider->GetSystemAttributes(&systemAttributes);
        FOREACH (const auto& attribute, systemAttributes) {
            if (attribute.IsPresent) {
                keys.push_back(attribute.Key);
            }
        }
    }

    if (userAttributes) {
        const auto& userKeys = userAttributes->List();
        keys.insert(keys.end(), userKeys.begin(), userKeys.end());
    }

    return keys;
}

bool DoRemoveAttribute(
    IAttributeDictionary* userAttributes,
    ISystemAttributeProvider* systemAttributeProvider,
    const Stroka& key)
{
    // System attributes do not support removal.
    UNUSED(systemAttributeProvider);

    if (!userAttributes) {
        ythrow yexception() << "User attributes are not supported";
    }

    return userAttributes->Remove(key);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSupportsAttributes::TCombinedAttributeDictionary
    : public IAttributeDictionary
{
public:
    TCombinedAttributeDictionary(
        IAttributeDictionary* userAttributes,
        ISystemAttributeProvider* systemAttributeProvider)
        : UserAttributes(userAttributes)
        , SystemAttributeProvider(systemAttributeProvider)
    { }

    virtual yhash_set<Stroka> List() const
    {
        auto keys = DoListAttributes(UserAttributes, SystemAttributeProvider);
        return yhash_set<Stroka>(keys.begin(), keys.end());
    }

    virtual TNullable<TYson> FindYson(const Stroka& key) const
    {
        return DoFindAttribute(UserAttributes, SystemAttributeProvider, key);
    }

    virtual void SetYson(const Stroka& key, const TYson& value)
    {
        DoSetAttribute(UserAttributes, SystemAttributeProvider, key, value);
    }

    virtual bool Remove(const Stroka& key)
    {
        return DoRemoveAttribute(UserAttributes, SystemAttributeProvider, key);
    }

private:
    IAttributeDictionary* UserAttributes;
    ISystemAttributeProvider* SystemAttributeProvider;

};

////////////////////////////////////////////////////////////////////////////////

IYPathService::TResolveResult TSupportsAttributes::ResolveAttributes(
    const NYTree::TYPath& path,
    const Stroka& verb)
{
    UNUSED(path);
    if (verb != "Get" &&
        verb != "Set" &&
        verb != "List" &&
        verb != "Remove")
    {
        ythrow TServiceException(EErrorCode::NoSuchVerb) <<
            "Verb is not supported";
    }
    return TResolveResult::Here("/@" + path);
}

void TSupportsAttributes::GetAttribute(
    const TYPath& path,
    TReqGet* request,
    TRspGet* response,
    TCtxGet* context)
{
    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();
    
    TTokenizer tokenizer(path);

    if (!tokenizer.ParseNext()) {
        TStringStream stream;
        TYsonWriter writer(&stream);
        
        writer.OnBeginMap();

        if (systemAttributeProvider) {
            yvector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
            systemAttributeProvider->GetSystemAttributes(&systemAttributes);
            FOREACH (const auto& attribute, systemAttributes) {
                if (attribute.IsPresent) {
                    writer.OnKeyedItem(attribute.Key);
                    if (attribute.IsOpaque) {
                        writer.OnEntity();
                    } else {
                        YVERIFY(systemAttributeProvider->GetSystemAttribute(attribute.Key, &writer));
                    }
                }
            }
        }

        if (userAttributes) {
            const auto& userAttributeSet = userAttributes->List();
            std::vector<Stroka> userAttributeList(userAttributeSet.begin(), userAttributeSet.end());
            std::sort(userAttributeList.begin(), userAttributeList.end());
            FOREACH (const auto& key, userAttributeList) {
                writer.OnKeyedItem(key);
                writer.OnRaw(userAttributes->GetYson(key));
            }
        }
        
        writer.OnEndMap();

        response->set_value(stream.Str());
    } else {
        auto yson =
            DoGetAttribute(
                userAttributes,
                systemAttributeProvider,
                Stroka(tokenizer.CurrentToken().GetStringValue()));

        if (!tokenizer.ParseNext()) {
            response->set_value(yson);
        } else {
            auto wholeValue = DeserializeFromYson(yson);
            auto value = SyncYPathGet(wholeValue, TYPath(tokenizer.CurrentInput()));
            response->set_value(value);
        }
    }

    context->Reply();
}

void TSupportsAttributes::ListAttribute(
    const TYPath& path,
    TReqList* request,
    TRspList* response,
    TCtxList* context)
{
    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();

    TTokenizer tokenizer(path);

    std::vector<Stroka> keys;

    if (!tokenizer.ParseNext()) {
        keys = DoListAttributes(userAttributes, systemAttributeProvider);
    } else  {
        auto wholeValue = DeserializeFromYson(
            DoGetAttribute(
                userAttributes,
                systemAttributeProvider,
                Stroka(tokenizer.CurrentToken().GetStringValue())));
        keys = SyncYPathList(wholeValue, TYPath(tokenizer.GetCurrentSuffix()));
    }

    std::sort(keys.begin(), keys.end());

    NYT::ToProto(response->mutable_keys(), keys);
    context->Reply();
}

void TSupportsAttributes::SetAttribute(
    const TYPath& path,
    TReqSet* request,
    TRspSet* response,
    TCtxSet* context)
{
    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();

    TTokenizer tokenizer(path);

    if (!tokenizer.ParseNext()) {
        auto newAttributes = DeserializeAttributesFromYson(request->value());
        auto newKeys = newAttributes->List();
        yhash_set<Stroka> userKeys;
        if (userAttributes) {
            auto temp = userAttributes->List();
            userKeys.swap(temp);
        }

        // Checking
        FOREACH (const auto& key, newKeys) {
            YASSERT(!key.empty());
            OnUpdateAttribute(
                key,
                DoFindAttribute(userAttributes, systemAttributeProvider, key),
                newAttributes->GetYson(key));
        }
        FOREACH (const auto& key, userKeys) {
            if (newKeys.find(key) == newKeys.end()) {
                OnUpdateAttribute(
                    key,
                    userAttributes->GetYson(key),
                    Null);
            }
        }

        // Removing
        FOREACH (const auto& key, userKeys) {
            userAttributes->Remove(key);
        }

        // Adding
        FOREACH (const auto& key, newKeys) {
            DoSetAttribute(
                userAttributes,
                systemAttributeProvider,
                key,
                newAttributes->GetYson(key));
        }
    } else {
        auto key = Stroka(tokenizer.CurrentToken().GetStringValue());
        if (!tokenizer.ParseNext()) {
            if (key.Empty()) {
                ythrow yexception() << "Attribute key cannot be empty";
            }
            OnUpdateAttribute(
                key,
                DoFindAttribute(userAttributes, systemAttributeProvider, key),
                request->value());
            DoSetAttribute(
                userAttributes,
                systemAttributeProvider,
                key,
                request->value());
        } else {
            bool isSystem;
            auto yson =
                DoGetAttribute(
                    userAttributes,
                    systemAttributeProvider,
                    key,
                    &isSystem);
            auto wholeValue = DeserializeFromYson(yson);
            SyncYPathSet(wholeValue, TYPath(tokenizer.CurrentInput()), request->value());
            auto updatedYson = SerializeToYson(wholeValue);
            OnUpdateAttribute(
                key,
                DoFindAttribute(userAttributes, systemAttributeProvider, key),
                updatedYson);
            DoSetAttribute(
                userAttributes,
                systemAttributeProvider,
                key,
                updatedYson,
                isSystem);
        }
    }

    context->Reply();
}

void TSupportsAttributes::RemoveAttribute(
    const TYPath& path,
    TReqRemove* request,
    TRspRemove* response,
    TCtxRemove* context)
{
    auto userAttributes = GetUserAttributes();
    auto systemAttributeProvider = GetSystemAttributeProvider();
    
    TTokenizer tokenizer(path);

    if (!tokenizer.ParseNext()) {
        if (userAttributes) {
            auto userKeys = userAttributes->List();
            FOREACH (const auto& key, userKeys) {
                OnUpdateAttribute(key, userAttributes->GetYson(key), Null);
            }
            FOREACH (const auto& key, userKeys) {
                YVERIFY(userAttributes->Remove(key));
            }
        }
    } else {
        auto key = Stroka(tokenizer.CurrentToken().GetStringValue());
        if (!tokenizer.ParseNext()) {
            OnUpdateAttribute(
                key,
                DoFindAttribute(userAttributes, systemAttributeProvider, key),
                Null);
            if (!DoRemoveAttribute(userAttributes, systemAttributeProvider, key)) {
                ythrow yexception() << Sprintf("User attribute %s is not found",
                    ~Stroka(key).Quote());
            }
        } else {
            bool isSystem;
            auto yson = DoGetAttribute(
                userAttributes,
                systemAttributeProvider,
                key,
                &isSystem);
            auto wholeValue = DeserializeFromYson(yson);
            SyncYPathRemove(wholeValue, TYPath(tokenizer.CurrentInput()));
            auto updatedYson = SerializeToYson(wholeValue);
            OnUpdateAttribute(
                key,
                DoFindAttribute(userAttributes, systemAttributeProvider, key),
                updatedYson);
            DoSetAttribute(
                userAttributes,
                systemAttributeProvider,
                key,
                updatedYson,
                isSystem);
        }
    }

    context->Reply();
}

void TSupportsAttributes::OnUpdateAttribute(
    const Stroka& key,
    const TNullable<NYTree::TYson>& oldValue,
    const TNullable<NYTree::TYson>& newValue)
{
    UNUSED(key);
    UNUSED(oldValue);
    UNUSED(newValue);
}

IAttributeDictionary& TSupportsAttributes::CombinedAttributes()
{
    return GetOrCreateCombinedAttributes();
}

const IAttributeDictionary& TSupportsAttributes::CombinedAttributes() const
{
    return const_cast<TSupportsAttributes*>(this)->CombinedAttributes();
}

IAttributeDictionary& TSupportsAttributes::GetOrCreateCombinedAttributes()
{
    auto* provider = GetSystemAttributeProvider();

    // Ephemeral nodes typically don't have system attributes.
    // This quick check eliminates creation of an additional |TCombinedAttributeDictionary| wrapper.
    if (!provider) {
        return *GetUserAttributes();
    }

    if (!CombinedAttributes_) {
        CombinedAttributes_.Reset(new TCombinedAttributeDictionary(
            GetUserAttributes(),
            GetSystemAttributeProvider()));
    }
    return *CombinedAttributes_;
}

IAttributeDictionary* TSupportsAttributes::GetUserAttributes()
{
    return NULL;
}

ISystemAttributeProvider* TSupportsAttributes::GetSystemAttributeProvider()
{
    return NULL;
}

////////////////////////////////////////////////////////////////////////////////

class TNodeSetterBase::TAttributesSetter
    : public TForwardingYsonConsumer
{
public:
    explicit TAttributesSetter(IAttributeDictionary* attributes)
        : Attributes(attributes)
        , AttributeStream(AttributeValue)
        , AttributeWriter(&AttributeStream)
    { }

private:
    IAttributeDictionary* Attributes;

    Stroka AttributeKey;
    TYson AttributeValue;
    TStringOutput AttributeStream;
    TYsonWriter AttributeWriter;

    virtual void OnMyKeyedItem(const TStringBuf& key)
    {
        AttributeKey = key;
        Forward(&AttributeWriter, BIND(&TAttributesSetter::OnAttributeFinished, this));
    }

    void OnAttributeFinished()
    {
        Attributes->SetYson(AttributeKey, AttributeValue);
        AttributeKey.clear();
        AttributeValue.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INode* node, ITreeBuilder* builder)
    : Node(node)
    , TreeBuilder(builder)
    , NodeFactory(node->CreateFactory())
{
    Node->Attributes().Clear();
}

TNodeSetterBase::~TNodeSetterBase()
{ }

void TNodeSetterBase::ThrowInvalidType(ENodeType actualType)
{
    ythrow yexception() << Sprintf("Invalid node type (Expected: %s, Actual: %s)",
        ~GetExpectedType().ToString().Quote(),
        ~actualType.ToString().Quote());
}

void TNodeSetterBase::OnMyStringScalar(const TStringBuf& value)
{
    UNUSED(value);

    ThrowInvalidType(ENodeType::String);
}

void TNodeSetterBase::OnMyIntegerScalar(i64 value)
{
    UNUSED(value);

    ThrowInvalidType(ENodeType::Integer);
}

void TNodeSetterBase::OnMyDoubleScalar(double value)
{
    UNUSED(value);

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
    AttributesSetter.Reset(new TAttributesSetter(&Node->Attributes()));
    Forward(~AttributesSetter, TClosure(), EYsonType::KeyedFragment);
}

void TNodeSetterBase::OnMyEndAttributes()
{
    AttributesSetter.Destroy();
}

////////////////////////////////////////////////////////////////////////////////

class TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        const TRequestHeader& header,
        NBus::IMessagePtr requestMessage,
        TYPathResponseHandler responseHandler,
        const Stroka& loggingCategory)
        : TServiceContextBase(header, requestMessage)
        , ResponseHandler(responseHandler)
        , Logger(loggingCategory)
    { }

protected:
    TYPathResponseHandler ResponseHandler;
    NLog::TLogger Logger;

    virtual void DoReply(const TError& error, IMessagePtr responseMessage)
    {
        UNUSED(error);

        if (!ResponseHandler.IsNull()) {
            ResponseHandler.Run(responseMessage);
        }
    }

    virtual void LogRequest()
    {
        Stroka str;
        AppendInfo(str, RequestInfo);
        LOG_DEBUG("%s %s <- %s",
            ~Verb,
            ~Path,
            ~str);
    }

    virtual void LogResponse(const TError& error)
    {
        Stroka str;
        AppendInfo(str, Sprintf("Error: %s", ~error.ToString()));
        AppendInfo(str, ResponseInfo);
        LOG_DEBUG("%s %s -> %s",
            ~Verb,
            ~Path,
            ~str);
    }

};

IServiceContextPtr CreateYPathContext(
    NBus::IMessagePtr requestMessage,
    const TYPath& path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler responseHandler)
{
    YASSERT(requestMessage);

    auto header = GetRequestHeader(requestMessage);
    return New<TServiceContext>(
        header,
        requestMessage,
        responseHandler,
        loggingCategory);
}

////////////////////////////////////////////////////////////////////////////////

class TRootService
    : public IYPathService
{
public:
    TRootService(IYPathServicePtr underlyingService)
        : UnderlyingService(underlyingService)
    { }

    virtual void Invoke(NRpc::IServiceContextPtr context)
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        UNUSED(verb);

        TTokenizer tokenizer(path);
        tokenizer.ParseNext();
        if (tokenizer.GetCurrentType() != RootToken) {
            ythrow yexception() << Sprintf("YPath must start with '/'");
        }

        return TResolveResult::There(~UnderlyingService, TYPath(tokenizer.GetCurrentSuffix()));
    }

    virtual Stroka GetLoggingCategory() const
    {
        return UnderlyingService->GetLoggingCategory();
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const
    {
        UNUSED(context);
        YUNREACHABLE();
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
