#include "stdafx.h"
#include "ypath_client.h"
#include "ypath_proxy.h"
#include "ypath_detail.h"
#include "tokenizer.h"
#include "ypath_format.h"
#include "yson_format.h"
#include "attribute_helpers.h"

#include <ytlib/misc/serialize.h>

#include <ytlib/rpc/rpc.pb.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYPathRequest::TYPathRequest(const Stroka& verb)
    : Verb_(verb)
{ }

bool TYPathRequest::IsOneWay() const
{
    return false;
}

bool TYPathRequest::IsHeavy() const
{
    return false;
}

const TRequestId& TYPathRequest::GetRequestId() const
{
    return NullRequestId;
}

const Stroka& TYPathRequest::GetVerb() const
{
    return Verb_;
}

const Stroka& TYPathRequest::GetPath() const
{
    return Path_;
}

void TYPathRequest::SetPath(const Stroka& path)
{
    Path_ = path;
}

IAttributeDictionary& TYPathRequest::Attributes()
{
    return TEphemeralAttributeProvider::Attributes();
}

const IAttributeDictionary& TYPathRequest::Attributes() const
{
    return TEphemeralAttributeProvider::Attributes();
}

IMessagePtr TYPathRequest::Serialize() const
{
    auto bodyData = SerializeBody();

    NRpc::NProto::TRequestHeader header;
    header.set_path(Path_);
    header.set_verb(Verb_);
    if (HasAttributes()) {
        ToProto(header.mutable_attributes(), Attributes());
    }

    return CreateRequestMessage(
        header,
        MoveRV(bodyData),
        Attachments_);
}

////////////////////////////////////////////////////////////////////////////////

void TYPathResponse::Deserialize(IMessagePtr message)
{
    YASSERT(message);

    NRpc::NProto::TResponseHeader header;
    if (!ParseResponseHeader(message, &header)) {
        Error_ = TError("Error parsing response header");
        return;
    }

    Error_ = NYT::FromProto(header.error());
    if (header.has_attributes()) {
        SetAttributes(FromProto(header.attributes()));
    }

    if (Error_.IsOK()) {
        // Deserialize body.
        const auto& parts = message->GetParts();
        YASSERT(parts.size() >=1 );
        DeserializeBody(parts[1]);

        // Load attachments.
        Attachments_ = std::vector<TSharedRef>(parts.begin() + 2, parts.end());
    }
}

bool TYPathResponse::IsOK() const
{
    return Error_.IsOK();
}

TYPathResponse::operator TError() const
{
    return Error_;
}

void TYPathResponse::DeserializeBody(const TRef& data)
{
    UNUSED(data);
}

////////////////////////////////////////////////////////////////////////////////

TYPath ComputeResolvedYPath(
    const TYPath& wholePath,
    const TYPath& unresolvedPath)
{
    int resolvedLength = static_cast<int>(wholePath.length()) - static_cast<int>(unresolvedPath.length());
    YASSERT(resolvedLength >= 0 && resolvedLength <= static_cast<int>(wholePath.length()));
    YASSERT(wholePath.substr(resolvedLength) == unresolvedPath);
    // Take care of trailing slash but don't reduce / to empty string.
    return
        resolvedLength > 1 && wholePath[resolvedLength - 1] == '/'
        ? wholePath.substr(0, resolvedLength - 1)
        : wholePath.substr(0, resolvedLength);
}

TYPath EscapeYPathToken(const Stroka& value)
{
    bool isIdentifer = false;

    // Checking if we can leave the value as is (i.e. value is an identifier)
    if (!value.empty() && value[0] != '-' && !isdigit(value[0])) {
        isIdentifer = true;
        FOREACH (char ch, value) {
            if (!isIdentifer)
                break;
            switch (ch) {
                case '_':
                case '-':
                case '%':
                    break;
                default:
                    if (!isalpha(ch) && !isdigit(ch)) {
                        isIdentifer = false;
                    }
                    break;
            }
        }
    }

    if (isIdentifer) {
        return value;
    } else {
        return YsonizeString(value, EYsonFormat::Text);
    }
}

TYPath EscapeYPathToken(i64 value)
{
    return ConvertToYsonString(value, EYsonFormat::Text).Data();
}

void ResolveYPath(
    IYPathServicePtr rootService,
    IServiceContextPtr context,
    IYPathServicePtr* suffixService,
    TYPath* suffixPath)
{
    YASSERT(rootService);
    YASSERT(suffixService);
    YASSERT(suffixPath);

    const auto& path = context->GetPath();
    const auto& verb = context->GetVerb();

    auto currentService = rootService;
    auto currentPath = path;

    while (true) {
        IYPathService::TResolveResult result;
        try {
            result = currentService->Resolve(currentPath, context);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                EYPathErrorCode::ResolveError,
                "Error resolving path %s for %s",
                ~path,
                ~verb)
                << TErrorAttribute("resolved_path", TRawString(ComputeResolvedYPath(path, currentPath)))
                << ex;
        }

        if (result.IsHere()) {
            *suffixService = currentService;
            *suffixPath = result.GetPath();
            break;
        }

        currentService = result.GetService();
        currentPath = result.GetPath();
    }
}

void OnYPathResponse(
    TPromise<IMessagePtr> asyncResponseMessage,
    IMessagePtr responseMessage)
{
    NRpc::NProto::TResponseHeader responseHeader;
    YCHECK(ParseResponseHeader(responseMessage, &responseHeader));

    auto error = NYT::FromProto(responseHeader.error());

    if (error.IsOK()) {
        asyncResponseMessage.Set(responseMessage);
    } else {
        ToProto(responseHeader.mutable_error(), error);
        auto updatedResponseMessage = SetResponseHeader(responseMessage, responseHeader);
        asyncResponseMessage.Set(updatedResponseMessage);
    }
}

TFuture<IMessagePtr>
ExecuteVerb(
    IYPathServicePtr service,
    IMessagePtr requestMessage)
{
    NLog::TLogger Logger(service->GetLoggingCategory());

    auto context = CreateYPathContext(
        requestMessage,
        "",
        TYPathResponseHandler());

    IYPathServicePtr suffixService;
    TYPath suffixPath;
    try {
        ResolveYPath(
            service,
            context,
            &suffixService,
            &suffixPath);
    } catch (const std::exception& ex) {
        return MakeFuture(CreateErrorResponseMessage(ex));
    }

    NRpc::NProto::TRequestHeader requestHeader;
    YCHECK(ParseRequestHeader(requestMessage, &requestHeader));
    requestHeader.set_path(suffixPath);
    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    auto asyncResponseMessage = NewPromise<IMessagePtr>();
    auto updatedContext = CreateYPathContext(
        updatedRequestMessage,
        suffixService->GetLoggingCategory(),
        BIND(&OnYPathResponse, asyncResponseMessage));

    // This should never throw.
    suffixService->Invoke(updatedContext);

    return asyncResponseMessage;
}

void ExecuteVerb(IYPathServicePtr service, IServiceContextPtr context)
{
    auto requestMessage = context->GetRequestMessage();
    ExecuteVerb(service, requestMessage)
        .Subscribe(BIND([=] (IMessagePtr responseMessage) {
            context->Reply(responseMessage);
        }));
}

TFuture< TValueOrError<TYsonString> > AsyncYPathGet(
    IYPathServicePtr service,
    const TYPath& path,
    bool allAttributes)
{
    auto request = TYPathProxy::Get(path);
    request->set_all_attributes(allAttributes);
    return
        ExecuteVerb(service, request)
            .Apply(BIND([] (TYPathProxy::TRspGetPtr response) {
                return
                    response->IsOK()
                    ? TValueOrError<TYsonString>(TYsonString(response->value()))
                    : TValueOrError<TYsonString>(response->GetError());
            }));
}

TYsonString SyncYPathGet(IYPathServicePtr service, const TYPath& path, bool allAttributes)
{
    auto result = AsyncYPathGet(service, path, allAttributes).Get();
    if (!result.IsOK()) {
        THROW_ERROR_EXCEPTION(result);
    }
    return result.Value();
}

void SyncYPathSet(IYPathServicePtr service, const TYPath& path, const TYsonString& value)
{
    auto request = TYPathProxy::Set(path);
    request->set_value(value.Data());
    auto response = ExecuteVerb(service, request).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*response);
}

void SyncYPathRemove(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::Remove(path);
    auto response = ExecuteVerb(service, request).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*response);
}

std::vector<Stroka> SyncYPathList(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::List(path);
    auto response = ExecuteVerb(service, request).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*response);
    return ConvertTo<std::vector<Stroka> >(TYsonString(response->keys()));
}

void ApplyYPathOverride(INodePtr root, const TStringBuf& overrideString)
{
    TTokenizer tokenizer(overrideString);

    TYPath path;
    while (true) {
        if (!tokenizer.ParseNext()) {
            THROW_ERROR_EXCEPTION("Unexpected end-of-stream while parsing YPath override");
        }
        if (tokenizer.GetCurrentType() == KeyValueSeparatorToken) {
            break;
        }
        path.append(tokenizer.CurrentToken().ToString());
    }

    auto value = TYsonString(Stroka(tokenizer.GetCurrentSuffix()));

    ForceYPath(root, path);
    SyncYPathSet(root, path, value);
}

INodePtr GetNodeByYPath(INodePtr root, const TYPath& path)
{
    INodePtr currentNode = root;
    TTokenizer tokenizer(path);
    while (tokenizer.ParseNext()) {
        tokenizer.CurrentToken().CheckType(PathSeparatorToken);
        tokenizer.ParseNext();
        switch (tokenizer.GetCurrentType()) {
            case ETokenType::String: {
                auto currentMap = currentNode->AsMap();
                Stroka key(tokenizer.CurrentToken().GetStringValue());
                currentNode = currentMap->GetChild(key);
                break;
            }

            case ETokenType::Integer: {
                auto currentList = currentNode->AsList();
                int index = currentList->AdjustAndValidateChildIndex(tokenizer.CurrentToken().GetIntegerValue());
                currentNode = currentList->GetChild(index);
                break;
            }

            default:
                ThrowUnexpectedToken(tokenizer.CurrentToken());
                YUNREACHABLE();
        }
    }
    return currentNode;
}

void SetNodeByYPath(INodePtr root, const TYPath& path, INodePtr value)
{
    auto currentNode = root;
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    tokenizer.CurrentToken().CheckType(PathSeparatorToken);
    tokenizer.ParseNext();
    Stroka currentTokenBuffer;
    auto currentToken = tokenizer.CurrentToken();
    if (currentToken.GetType() == ETokenType::String) {
        currentTokenBuffer.assign(currentToken.GetStringValue());
        currentToken = TToken(currentTokenBuffer);
    }
    while (tokenizer.ParseNext()) {
        // Move to currentToken.
        switch (currentToken.GetType()) {
            case ETokenType::String: {
                Stroka key(currentToken.GetStringValue());
                currentNode = currentNode->AsMap()->GetChild(key);
                break;
            }

            case ETokenType::Integer: {
                auto currentList = currentNode->AsList();
                int index = currentList->AdjustAndValidateChildIndex(currentToken.GetIntegerValue());
                currentNode = currentList->GetChild(index);
                break;
            }

            default:
                ThrowUnexpectedToken(currentToken);
                YUNREACHABLE();
        }
        // Update currentToken.
        tokenizer.CurrentToken().CheckType(PathSeparatorToken);
        tokenizer.ParseNext();
        currentToken = tokenizer.CurrentToken();
        if (currentToken.GetType() == ETokenType::String) {
            currentTokenBuffer.assign(currentToken.GetStringValue());
            currentToken = TToken(currentTokenBuffer);
        }
    }

    // Set value.
    switch (currentToken.GetType()) {
        case ETokenType::String: {
            Stroka key(currentToken.GetStringValue());
            auto currentMap = currentNode->AsMap();
            auto child = currentMap->FindChild(key);
            if (child) {
                currentMap->ReplaceChild(child, value);
            } else {
                currentMap->AddChild(value, key);
            }
            break;
        }

        case ETokenType::Integer: {
            auto currentList = currentNode->AsList();
            int index = currentList->AdjustAndValidateChildIndex(currentToken.GetIntegerValue());
            auto child = currentList->GetChild(index);
            currentList->ReplaceChild(child, value);
            break;
        }

        default:
            ThrowUnexpectedToken(currentToken);
            YUNREACHABLE();
    }
}

void ForceYPath(INodePtr root, const TYPath& path)
{
    auto currentNode = root;

    TTokenizer tokenizer(path);
    if (!tokenizer.ParseNext()) {
        // Hmm... empty path!
        return;
    }

    while (true) {
        tokenizer.CurrentToken().CheckType(PathSeparatorToken);

        tokenizer.ParseNext();
        auto token = tokenizer.CurrentToken();
        
        // Token holds a stringbuf that gets invalidated with each ParseNext call.
        // Thus we have to make a persistent copy before advancing the tokenizer.
        // For the sake of completeness, we also make copies of the type and the integer value.
        auto tokenType = token.GetType();
        Stroka key(token.GetType() == ETokenType::String ? token.GetStringValue() : "");
        i64 index(token.GetType() == ETokenType::Integer ? token.GetIntegerValue() : -1);

        if (!tokenizer.ParseNext()) {
            // The previous token was the last one.
            // Stop here -- we don't force the very last segment.
            return;
        }

        INodePtr child;
        switch (tokenType) {
            case ETokenType::String: {
                auto currentMap = currentNode->AsMap();
                child = currentMap->AsMap()->FindChild(key);
                if (!child) {
                    auto factory = currentMap->CreateFactory();
                    child = factory->CreateMap();
                    YCHECK(currentMap->AddChild(child, key));
                }
                break;
            }

            case ETokenType::Integer: {
                auto currentList = currentNode->AsList();
                index = currentList->AdjustAndValidateChildIndex(index);
                child = currentList->GetChild(index);
                break;
            }

            default:
                ThrowUnexpectedToken(tokenizer.CurrentToken());
                YUNREACHABLE();
        }

        currentNode = child;
    }
}

TYPath GetNodeYPath(INodePtr node, INodePtr* root)
{
    std::vector<TYPath> tokens;
    while (true) {
        auto parent = node->GetParent();
        if (!parent) {
            break;
        }
        TYPath token;
        switch (parent->GetType()) {
            case ENodeType::List: {
                auto index = parent->AsList()->GetChildIndex(node);
                token = EscapeYPathToken(index);
                break;
            }
            case ENodeType::Map: {
                auto key = parent->AsMap()->GetChildKey(node);
                token = EscapeYPathToken(key);
                break;
            }
            default:
                YUNREACHABLE();
        }
        tokens.push_back(token);
        node = parent;
    }
    if (root) {
        *root = node;
    }
    std::reverse(tokens.begin(), tokens.end());
    TYPath path;
    FOREACH (const auto& token, tokens) {
        path.append(TokenTypeToChar(PathSeparatorToken));
        path.append(token);
    }
    return path;
}

INodePtr CloneNode(INodePtr node)
{
    return ConvertToNode(node);
}

INodePtr UpdateNode(INodePtr base, INodePtr patch)
{
    if (base->GetType() == ENodeType::Map && patch->GetType() == ENodeType::Map) {
        auto result = CloneNode(base);
        auto resultMap = result->AsMap();
        auto patchMap = patch->AsMap();
        auto baseMap = base->AsMap();
        FOREACH (Stroka key, patchMap->GetKeys()) {
            if (baseMap->FindChild(key)) {
                resultMap->RemoveChild(key);
                resultMap->AddChild(UpdateNode(baseMap->GetChild(key), patchMap->GetChild(key)), key);
            }
            else {
                resultMap->AddChild(CloneNode(patchMap->GetChild(key)), key);
            }
        }
        result->Attributes().MergeFrom(patch->Attributes());
        return result;
    }
    else {
        auto result = CloneNode(patch);
        result->Attributes().Clear();
        if (base->GetType() == patch->GetType()) {
            result->Attributes().MergeFrom(base->Attributes());
        }
        result->Attributes().MergeFrom(patch->Attributes());
        return result;
    }
}

bool AreNodesEqual(INodePtr lhs, INodePtr rhs) {
    if (lhs->GetType() == ENodeType::Map && rhs->GetType() == ENodeType::Map) {
        auto lhsMap = lhs->AsMap();
        auto lhsKeys = lhsMap->GetKeys();
        sort(lhsKeys.begin(), lhsKeys.end());
        
        auto rhsMap = rhs->AsMap();
        auto rhsKeys = rhsMap->GetKeys();
        sort(rhsKeys.begin(), rhsKeys.end());
        if (rhsKeys != lhsKeys) {
            return false;
        }
        FOREACH (Stroka key, lhsKeys) {
            if (!AreNodesEqual(lhsMap->FindChild(key), rhsMap->FindChild(key))) {
                return false;
            }
        }
        return true;
    }
    else if (lhs->GetType() == ENodeType::List && rhs->GetType() == ENodeType::List) {
        auto lhsList = lhs->AsList();
        auto lhsChildren = lhsList->GetChildren();

        auto rhsList = rhs->AsList();
        auto rhsChildren = rhsList->GetChildren();

        if (lhsChildren.size() != rhsChildren.size()) {
            return false;
        }
        for (size_t i = 0; i < lhsChildren.size(); ++i) {
            if (!AreNodesEqual(lhsList->FindChild(i), rhsList->FindChild(i))) {
                return false;
            }
        }
        return true; 
    }
    else if (lhs->GetType() == rhs->GetType()) {
        return ConvertToYsonString(lhs) == ConvertToYsonString(rhs);
    }
    else {
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
