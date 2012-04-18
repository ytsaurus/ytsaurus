#include "stdafx.h"
#include "ypath_detail.h"

#include "ypath_client.h"
#include "serialize.h"
#include "tokenizer.h"

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

TYPathServiceBase::TYPathServiceBase(const Stroka& loggingCategory)
    : Logger(loggingCategory)
{ }

IYPathService::TResolveResult TYPathServiceBase::Resolve(const TYPath& path, const Stroka& verb)
{
    TTokenizer tokens(path);
    switch (tokens[0].GetType()) {
        case ETokenType::None:
            return ResolveSelf(TYPath(tokens.GetSuffix(0)), verb);

        case ETokenType::Slash:
            if (tokens[1].GetType() == ETokenType::At) {
                return ResolveAttributes(TYPath(tokens.GetSuffix(1)), verb);
            } else {
                return ResolveRecursive(TYPath(tokens.GetSuffix(0)), verb);
            }

        default:
            ThrowUnexpectedToken(tokens[0]);
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

void TYPathServiceBase::Invoke(IServiceContext* context)
{
    GuardedInvoke(context);
}

void TYPathServiceBase::GuardedInvoke(IServiceContext* context)
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

void TYPathServiceBase::DoInvoke(IServiceContext* context)
{
    UNUSED(context);
    ythrow TServiceException(EErrorCode::NoSuchVerb) <<
        "Verb is not supported";
}

Stroka TYPathServiceBase::GetLoggingCategory() const
{
    return Logger.GetCategory();
}

bool TYPathServiceBase::IsWriteRequest(IServiceContext* context) const
{
    UNUSED(context);
    return false;
}

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_SUPPORTS_VERB(verb) \
    DEFINE_RPC_SERVICE_METHOD(TSupports##verb, verb) \
    { \
        TTokenizer tokens(context->GetPath()); \
        switch (tokens[0].GetType()) { \
            case ETokenType::None: \
                verb##Self(request, response, ~context); \
                break; \
            \
            case ETokenType::Slash: \
                if (tokens[1].GetType() == ETokenType::At) { \
                    verb##Attribute(TYPath(tokens.GetSuffix(1)), request, response, ~context); \
                } else { \
                    verb##Recursive(TYPath(tokens.GetSuffix(0)), request, response, ~context); \
                } \
                break; \
            \
            default: \
                ThrowUnexpectedToken(tokens[0]); \
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
IMPLEMENT_SUPPORTS_VERB(GetNode)
IMPLEMENT_SUPPORTS_VERB(Set)
IMPLEMENT_SUPPORTS_VERB(SetNode)
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
    INode* value,
    bool isSystem)
{
    if (isSystem) {
        YASSERT(systemAttributeProvider);
        if (!systemAttributeProvider->SetSystemAttribute(key, ProducerFromNode(value))) {
            ythrow yexception() << Sprintf("System attribute %s cannot be set", ~key.Quote());
        }
    } else {
        if (!userAttributes) {
            ythrow yexception() << "User attributes are not supported";
        }
        userAttributes->SetYson(key, SerializeToYson(value));
    }
}

void DoSetAttribute(
    IAttributeDictionary* userAttributes,
    ISystemAttributeProvider* systemAttributeProvider,
    const Stroka& key,
    const TYson& value)
{
    if (systemAttributeProvider) {
        if (systemAttributeProvider->SetSystemAttribute(key, ProducerFromYson(value))) {
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

} // namespace <anonymous>

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
    
    TYPath suffixPath;
    auto token = ChopToken(path, &suffixPath);

    if (token.GetType() == ETokenType::None) {
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
    } else if (token.GetType() == ETokenType::String) {
        const auto& yson = DoGetAttribute(userAttributes, systemAttributeProvider, token.GetStringValue());

        if (IsEmpty(suffixPath)) {
            response->set_value(yson);
        } else {
            auto wholeValue = DeserializeFromYson(yson);
            auto value = SyncYPathGet(~wholeValue, suffixPath);
            response->set_value(value);
        }
    } else {
        ythrow yexception() << Sprintf("Unexpected token %s of type %s",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
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

    TYPath suffixPath;
    auto token = ChopToken(path, &suffixPath);
    
    std::vector<Stroka> keys;

    if (token.GetType() == ETokenType::None) {
        keys = DoListAttributes(userAttributes, systemAttributeProvider);
    } else if (token.GetType() == ETokenType::String) {
        auto wholeValue = DeserializeFromYson(
            DoGetAttribute(
                userAttributes,
                systemAttributeProvider,
                token.GetStringValue()));
        keys = SyncYPathList(~wholeValue, suffixPath);
    } else {
        ythrow yexception() << Sprintf("Unexpected token %s of type %s",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
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

    TYPath suffixPath;
    auto token = ChopToken(path, &suffixPath);

    if (token.GetType() == ETokenType::None) {
        auto value = DeserializeFromYson(request->value());
        if (value->GetType() != ENodeType::Map) {
            ythrow yexception() << "Map value expected";
        }

        if (userAttributes) {
            const auto& userKeys = userAttributes->List();
            FOREACH (const auto& key, userKeys) {
                YVERIFY(userAttributes->Remove(key));
            }
        }

        auto mapValue = value->AsMap();
        FOREACH (const auto& pair, mapValue->GetChildren()) {
            auto key = pair.first;
            YASSERT(!key.empty());
            auto value = SerializeToYson(~pair.second);
            DoSetAttribute(userAttributes, systemAttributeProvider, key, value);
        }
    } else if (token.GetType() == ETokenType::String) {
        if (IsEmpty(suffixPath)) {
            if (token.GetStringValue().empty()) {
                ythrow yexception() << "Attribute key cannot be empty";
            }
            DoSetAttribute(
                userAttributes,
                systemAttributeProvider,
                token.GetStringValue(),
                request->value());
        } else {
            bool isSystem;
            auto yson =
                DoGetAttribute(
                    userAttributes,
                    systemAttributeProvider,
                    token.GetStringValue(),
                    &isSystem);
            auto wholeValue = DeserializeFromYson(yson);
            SyncYPathSet(~wholeValue, suffixPath, request->value());
            DoSetAttribute(
                userAttributes,
                systemAttributeProvider,
                token.GetStringValue(),
                ~wholeValue,
                isSystem);
        }
    } else {
        ythrow yexception() << Sprintf("Unexpected token %s of type %s",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
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
    
    TYPath suffixPath;
    auto token = ChopToken(path, &suffixPath);

    if (token.GetType() == ETokenType::None) {
        auto userKeys = userAttributes->List();
        FOREACH (const auto& key, userKeys) {
            YVERIFY(userAttributes->Remove(key));
        }
    } else if (token.GetType() == ETokenType::String) {
        if (IsEmpty(suffixPath)) {
            auto key = token.GetStringValue();
            if (!DoRemoveAttribute(userAttributes, systemAttributeProvider, key)) {
                ythrow yexception() << Sprintf("User attribute %s is not found",
                    ~token.ToString().Quote());
            }
        } else {
            bool isSystem;
            auto yson = DoGetAttribute(
                userAttributes,
                systemAttributeProvider,
                token.GetStringValue(),
                &isSystem);
            auto wholeValue = DeserializeFromYson(yson);
            SyncYPathRemove(~wholeValue, suffixPath);
            DoSetAttribute(
                userAttributes,
                systemAttributeProvider,
                token.GetStringValue(),
                ~wholeValue,
                isSystem);
        }
    } else {
        ythrow yexception() << Sprintf("Unexpected token %s of type %s",
            ~token.ToString().Quote(),
            ~token.GetType().ToString());
    }

    context->Reply();
}

IAttributeDictionary& TSupportsAttributes::CombinedAttributes()
{
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

TNodeSetterBase::TNodeSetterBase(INode* node, ITreeBuilder* builder)
    : Node(node)
    , TreeBuilder(builder)
    , NodeFactory(node->CreateFactory())
    , AttributeStream(AttributeValue)
    , AttributeWriter(&AttributeStream)
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
    Node->Attributes().Clear();
}

void TNodeSetterBase::OnMyAttributesItem(const TStringBuf& key)
{
    AttributeKey = key;
    ForwardNode(&AttributeWriter, BIND(&TThis::OnForwardingFinished, this));
}

void TNodeSetterBase::OnForwardingFinished()
{
    Node->Attributes().Set(AttributeKey, AttributeValue);
    AttributeKey.clear();
    AttributeValue.clear();
}

void TNodeSetterBase::OnMyEndAttributes()
{ }

////////////////////////////////////////////////////////////////////////////////

class TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        const TRequestHeader& header,
        NBus::IMessage* requestMessage,
        TYPathResponseHandler responseHandler,
        const Stroka& loggingCategory)
        : TServiceContextBase(header, requestMessage)
        , ResponseHandler(responseHandler)
        , Logger(loggingCategory)
    { }

protected:
    TYPathResponseHandler ResponseHandler;
    NLog::TLogger Logger;

    virtual void DoReply(const TError& error, IMessage::TPtr responseMessage)
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

NRpc::IServiceContext::TPtr CreateYPathContext(
    NBus::IMessage* requestMessage,
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

    virtual void Invoke(NRpc::IServiceContext* context)
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb)
    {
        UNUSED(verb);

        TYPath currentPath;
        auto token = ChopToken(path, &currentPath);
        if (token.GetType() != ETokenType::Slash) {
            ythrow yexception() << Sprintf("YPath must start with '/'");
        }

        return TResolveResult::There(~UnderlyingService, currentPath);
    }

    virtual Stroka GetLoggingCategory() const
    {
        return UnderlyingService->GetLoggingCategory();
    }

    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const
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
