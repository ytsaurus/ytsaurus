#include "stdafx.h"
#include "ypath_detail.h"
#include "ypath_client.h"
#include "tokenizer.h"
#include "ypath_format.h"

#include <ytlib/ytree/convert.h>
#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/bus/message.h>

#include <ytlib/rpc/rpc.pb.h>
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
    THROW_ERROR_EXCEPTION("Object cannot have attributes");
}

IYPathService::TResolveResult TYPathServiceBase::ResolveRecursive(const TYPath& path, const Stroka& verb)
{
    UNUSED(path);
    UNUSED(verb);
    THROW_ERROR_EXCEPTION("Object cannot have children");
}

void TYPathServiceBase::Invoke(IServiceContextPtr context)
{
    GuardedInvoke(context);
}

void TYPathServiceBase::GuardedInvoke(IServiceContextPtr context)
{
    try {
        DoInvoke(context);
    } catch (const std::exception& ex) {
        context->Reply(ex);
    }
}

void TYPathServiceBase::DoInvoke(IServiceContextPtr context)
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::NoSuchVerb,
        "Verb %s is not supported",
        ~context->GetVerb());
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

#define THROW_VERB_NOT_SUPPORTED() \
    THROW_ERROR TError( \
        EErrorCode::NoSuchVerb, \
        "Verb %s is not supported", \
        ~context->GetVerb())

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
        THROW_VERB_NOT_SUPPORTED(); \
    } \
    \
    void TSupports##verb::verb##Recursive(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb* context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        THROW_VERB_NOT_SUPPORTED(); \
    } \
    \
    void TSupports##verb::verb##Attribute(const TYPath& path, TReq##verb* request, TRsp##verb* response, TCtx##verb* context) \
    { \
        UNUSED(path); \
        UNUSED(request); \
        UNUSED(response); \
        THROW_VERB_NOT_SUPPORTED(); \
    }

IMPLEMENT_SUPPORTS_VERB(Get)
IMPLEMENT_SUPPORTS_VERB(Set)
IMPLEMENT_SUPPORTS_VERB(List)
IMPLEMENT_SUPPORTS_VERB(Remove)

#undef THROW_VERB_NOT_SUPPORTED
#undef IMPLEMENT_SUPPORTS_VERB

////////////////////////////////////////////////////////////////////////////////

namespace {

TYsonString DoGetAttribute(
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
            return TYsonString(stream.Str());
        }
    }

    if (!userAttributes) {
        THROW_ERROR_EXCEPTION("User attributes are not supported");
    }

    if (isSystem) {
        *isSystem = false;
    }

    return userAttributes->GetYson(key);
}

TNullable<TYsonString> DoFindAttribute(
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
            return TYsonString(stream.Str());
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
    const TYsonString& value,
    bool isSystem)
{
    if (isSystem) {
        YASSERT(systemAttributeProvider);
        if (!systemAttributeProvider->SetSystemAttribute(key, value)) {
            THROW_ERROR_EXCEPTION("System attribute %s cannot be set", ~key.Quote());
        }
    } else {
        if (!userAttributes) {
            THROW_ERROR_EXCEPTION("User attributes are not supported");
        }
        userAttributes->SetYson(key, value);
    }
}

void DoSetAttribute(
    IAttributeDictionary* userAttributes,
    ISystemAttributeProvider* systemAttributeProvider,
    const Stroka& key,
    const TYsonString& value)
{
    if (systemAttributeProvider) {
        if (systemAttributeProvider->SetSystemAttribute(key, value)) {
            return;
        }

        // Check for system attributes
        std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
        systemAttributeProvider->GetSystemAttributes(&systemAttributes);
                
        FOREACH (const auto& attribute, systemAttributes) {
            if (attribute.Key == key) {
                THROW_ERROR_EXCEPTION("System attribute %s cannot be set", ~key.Quote());
            }
        }
    }

    if (!userAttributes) {
        THROW_ERROR_EXCEPTION("User attributes are not supported");
    }

    userAttributes->SetYson(key, value);
}

std::vector<Stroka> DoListAttributes(
    IAttributeDictionary* userAttributes,
    ISystemAttributeProvider* systemAttributeProvider)
{
    std::vector<Stroka> keys;

    if (systemAttributeProvider) {
        std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
        systemAttributeProvider->GetSystemAttributes(&systemAttributes);
        FOREACH (const auto& attribute, systemAttributes) {
            if (attribute.IsPresent) {
                keys.push_back(attribute.Key);
            }
        }
    }

    if (userAttributes) {
        auto userKeys = userAttributes->List();
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
        THROW_ERROR_EXCEPTION("User attributes are not supported");
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

    virtual std::vector<Stroka> List() const override
    {
        return DoListAttributes(UserAttributes, SystemAttributeProvider);
    }

    virtual TNullable<TYsonString> FindYson(const Stroka& key) const override
    {
        return DoFindAttribute(UserAttributes, SystemAttributeProvider, key);
    }

    virtual void SetYson(const Stroka& key, const TYsonString& value) override
    {
        DoSetAttribute(UserAttributes, SystemAttributeProvider, key, value);
    }

    virtual bool Remove(const Stroka& key) override
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
        THROW_ERROR_EXCEPTION(
            EErrorCode::NoSuchVerb,
            "Verb %s is not supported",
            ~verb);
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
            std::vector<ISystemAttributeProvider::TAttributeInfo> systemAttributes;
            systemAttributeProvider->GetSystemAttributes(&systemAttributes);
            FOREACH (const auto& attribute, systemAttributes) {
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
            FOREACH (const auto& key, userAttributes->List()) {
                writer.OnKeyedItem(key);
                Consume(userAttributes->GetYson(key), &writer);
            }
        }
        
        writer.OnEndMap();

        response->set_value(stream.Str());
    } else {
        TYsonString yson =
            DoGetAttribute(
                userAttributes,
                systemAttributeProvider,
                Stroka(tokenizer.CurrentToken().GetStringValue()));

        if (!tokenizer.ParseNext()) {
            response->set_value(yson.Data());
        } else {
            INodePtr node = ConvertToNode(yson);
            TYsonString value = SyncYPathGet(node, TYPath(tokenizer.CurrentInput()));
            response->set_value(value.Data());
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
        INodePtr node = ConvertToNode(
            DoGetAttribute(
                userAttributes,
                systemAttributeProvider,
                Stroka(tokenizer.CurrentToken().GetStringValue())));
        keys = SyncYPathList(node, TYPath(tokenizer.GetCurrentSuffix()));
    }

    response->set_keys(ConvertToYsonString(keys).Data());
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
        auto newAttributes = ConvertToAttributes(TYsonString(request->value()));
        auto newKeys_ = newAttributes->List();
        yhash_set<Stroka> newKeys(newKeys_.begin(), newKeys_.end());
        auto oldKeys = userAttributes ? userAttributes->List() : std::vector<Stroka>();

        // Call OnUpdateAttribute to check the changes for feasibility.
        FOREACH (const auto& key, newKeys) {
            YASSERT(!key.empty());
            OnUpdateAttribute(
                key,
                DoFindAttribute(userAttributes, systemAttributeProvider, key),
                newAttributes->GetYson(key));
        }
        FOREACH (const auto& key, oldKeys) {
            if (newKeys.find(key) == newKeys.end()) {
                OnUpdateAttribute(
                    key,
                    userAttributes->GetYson(key),
                    Null);
            }
        }

        // Remove deleted keys.
        FOREACH (const auto& key, oldKeys) {
            userAttributes->Remove(key);
        }

        // Add new keys.
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
                THROW_ERROR_EXCEPTION("Attribute key cannot be empty");
            }
            auto oldValue = DoFindAttribute(userAttributes, systemAttributeProvider, key);
            auto newValue = TYsonString(request->value());
            newValue.Validate();
            OnUpdateAttribute(
                key,
                oldValue,
                newValue);
            DoSetAttribute(
                userAttributes,
                systemAttributeProvider,
                key,
                newValue);
        } else {
            bool isSystem;
            TYsonString yson =
                DoGetAttribute(
                    userAttributes,
                    systemAttributeProvider,
                    key,
                    &isSystem);
            INodePtr node = ConvertToNode(yson);
            SyncYPathSet(node, TYPath(tokenizer.CurrentInput()), TYsonString(request->value()));
            TYsonString updatedYson = ConvertToYsonString(~node);
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

    if (!tokenizer.ParseNext() || tokenizer.CurrentToken().GetType() == WildcardToken) {
        if (userAttributes) {
            auto userKeys = userAttributes->List();
            FOREACH (const auto& key, userKeys) {
                OnUpdateAttribute(key, userAttributes->GetYson(key), Null);
            }
            FOREACH (const auto& key, userKeys) {
                YCHECK(userAttributes->Remove(key));
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
                THROW_ERROR_EXCEPTION("User attribute %s is not found",
                    ~Stroka(key).Quote());
            }
        } else {
            bool isSystem;
            TYsonString yson = DoGetAttribute(
                userAttributes,
                systemAttributeProvider,
                key,
                &isSystem);
            auto node = ConvertToNode(yson);
            SyncYPathRemove(node, TYPath(tokenizer.CurrentInput()));
            auto updatedYson = ConvertToYsonString(~node);
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
    const TNullable<NYTree::TYsonString>& oldValue,
    const TNullable<NYTree::TYsonString>& newValue)
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
    { }

private:
    IAttributeDictionary* Attributes;

    TStringStream AttributeStream;
    THolder<TYsonWriter> AttributeWriter;

    virtual void OnMyKeyedItem(const TStringBuf& key) override
    {
        Stroka localKey(key);
        AttributeWriter.Reset(new TYsonWriter(&AttributeStream));
        Forward(~AttributeWriter,
            BIND( [=] ()
            {
                AttributeWriter.Reset(NULL);
                Attributes->SetYson(localKey, TYsonString(AttributeStream.Str()));
                AttributeStream.clear();
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INodePtr node, ITreeBuilder* builder)
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
    THROW_ERROR_EXCEPTION("Invalid node type (Expected: %s, Actual: %s)",
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
    Forward(~AttributesSetter, TClosure(), EYsonType::MapFragment);
}

void TNodeSetterBase::OnMyEndAttributes()
{
    AttributesSetter.Destroy();
}

////////////////////////////////////////////////////////////////////////////////

class TYPathServiceContext
    : public TServiceContextBase
{
public:
    TYPathServiceContext(
        const TRequestHeader& header,
        IMessagePtr requestMessage,
        TYPathResponseHandler responseHandler,
        const Stroka& loggingCategory)
        : TServiceContextBase(header, requestMessage)
        , ResponseHandler(responseHandler)
        , Logger(loggingCategory)
    { }

protected:
    TYPathResponseHandler ResponseHandler;
    NLog::TLogger Logger;

    virtual void DoReply(IMessagePtr responseMessage) override
    {
        if (!ResponseHandler.IsNull()) {
            ResponseHandler.Run(responseMessage);
        }
    }

    virtual void LogRequest() override
    {
        Stroka str;
        AppendInfo(str, RequestInfo);
        LOG_DEBUG("%s %s <- %s",
            ~Verb,
            ~Path,
            ~str);
    }

    virtual void LogResponse(const TError& error) override
    {
        Stroka str;
        AppendInfo(str, Sprintf("Error: %s", ~ToString(error)));
        AppendInfo(str, ResponseInfo);
        LOG_DEBUG("%s %s -> %s",
            ~Verb,
            ~Path,
            ~str);
    }

};

IServiceContextPtr CreateYPathContext(
    IMessagePtr requestMessage,
    const TYPath& path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler responseHandler)
{
    YASSERT(requestMessage);

    NRpc::NProto::TRequestHeader requestHeader;
    YCHECK(ParseRequestHeader(requestMessage, &requestHeader));
    return New<TYPathServiceContext>(
        requestHeader,
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

    virtual void Invoke(IServiceContextPtr context) override
    {
        UNUSED(context);
        YUNREACHABLE();
    }

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb) override
    {
        UNUSED(verb);

        TTokenizer tokenizer(path);
        tokenizer.ParseNext();
        if (tokenizer.GetCurrentType() != RootToken) {
            THROW_ERROR_EXCEPTION("YPath must start with '/'");
        }

        return TResolveResult::There(UnderlyingService, TYPath(tokenizer.GetCurrentSuffix()));
    }

    virtual Stroka GetLoggingCategory() const override
    {
        return UnderlyingService->GetLoggingCategory();
    }

    virtual bool IsWriteRequest(IServiceContextPtr context) const override
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
