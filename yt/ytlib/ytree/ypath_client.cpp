#include "stdafx.h"
#include "ypath_client.h"
#include "ypath_proxy.h"
#include "ypath_detail.h"
#include "tokenizer.h"
#include "ypath_format.h"
#include "yson_format.h"

#include <ytlib/rpc/rpc.pb.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TYPathRequest::TYPathRequest(const Stroka& verb)
    : Verb_(verb)
{ }

IMessagePtr TYPathRequest::Serialize()
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

void TYPathResponse::Deserialize(NBus::IMessagePtr message)
{
    YASSERT(message);

    NRpc::NProto::TResponseHeader header;
    if (!ParseResponseHeader(message, &header)) {
        Error_ = TError("Error parsing response header");
        return;
    }

    Error_ = TError::FromProto(header.error());
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

int TYPathResponse::GetErrorCode() const
{
    return Error_.GetCode();
}

bool TYPathResponse::IsOK() const
{
    return Error_.IsOK();
}

void TYPathResponse::ThrowIfError() const
{
    if (!IsOK()) {
        ythrow yexception() << Error_.ToString();
    }
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
    const TYPath& path,
    const Stroka& verb,
    IYPathServicePtr* suffixService,
    TYPath* suffixPath)
{
    YASSERT(rootService);
    YASSERT(suffixService);
    YASSERT(suffixPath);

    IYPathServicePtr currentService = rootService;
    auto currentPath = path;

    while (true) {
        IYPathService::TResolveResult result;
        try {
            result = currentService->Resolve(currentPath, verb);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error during YPath resolution (Path: %s, Verb: %s, ResolvedPath: %s)\n%s",
                ~path,
                ~verb,
                ~ComputeResolvedYPath(path, currentPath),
                ex.what());
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
    const TYPath& path,
    const Stroka& verb,
    const TYPath& resolvedPath,
    IMessagePtr responseMessage)
{
    NRpc::NProto::TResponseHeader responseHeader;
    YCHECK(ParseResponseHeader(responseMessage, &responseHeader));

    auto error = TError::FromProto(responseHeader.error());

    if (error.IsOK()) {
        asyncResponseMessage.Set(responseMessage);
    } else {
        Stroka message = Sprintf("Error executing a YPath operation (Path: %s, Verb: %s, ResolvedPath: %s)\n%s",
            ~path,
            ~verb,
            ~resolvedPath,
            ~error.GetMessage());
        *responseHeader.mutable_error() = TError(error.GetCode(), message).ToProto();

        auto updatedResponseMessage = SetResponseHeader(responseMessage, responseHeader);
        asyncResponseMessage.Set(updatedResponseMessage);
    }
}

TFuture<IMessagePtr>
ExecuteVerb(
    IYPathServicePtr service,
    NBus::IMessagePtr requestMessage)
{
    NLog::TLogger Logger(service->GetLoggingCategory());

    NRpc::NProto::TRequestHeader requestHeader;
    YCHECK(ParseRequestHeader(requestMessage, &requestHeader));

    TYPath path = requestHeader.path();
    Stroka verb = requestHeader.verb();

    IYPathServicePtr suffixService;
    TYPath suffixPath;
    try {
        ResolveYPath(
            service,
            path,
            verb,
            &suffixService,
            &suffixPath);
    } catch (const std::exception& ex) {
        auto responseMessage = NRpc::CreateErrorResponseMessage(TError(
            EYPathErrorCode(EYPathErrorCode::ResolveError),
            ex.what()));
        return MakeFuture(responseMessage);
    }

    requestHeader.set_path(suffixPath);
    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    auto asyncResponseMessage = NewPromise<IMessagePtr>();
    auto context = CreateYPathContext(
        updatedRequestMessage,
        suffixPath,
        verb,
        suffixService->GetLoggingCategory(),
        BIND(
            &OnYPathResponse,
            asyncResponseMessage,
            path,
            verb,
            ComputeResolvedYPath(path, suffixPath)));

    // This should never throw.
    suffixService->Invoke(context);

    return asyncResponseMessage;
}

void ExecuteVerb(IYPathServicePtr service, IServiceContextPtr context)
{
    auto requestMessage = context->GetRequestMessage();
    ExecuteVerb(service, requestMessage)
        .Subscribe(BIND([=] (NBus::IMessagePtr responseMessage) {
            context->Reply(responseMessage);
        }));
}

TFuture< TValueOrError<TYsonString> > AsyncYPathGet(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::Get(path);
    return
        ExecuteVerb(service, request)
            .Apply(BIND([] (TYPathProxy::TRspGetPtr response) {
                return
                    response->IsOK()
                    ? TValueOrError<TYsonString>(TYsonString(response->value()))
                    : TValueOrError<TYsonString>(response->GetError());
            }));
}

TYsonString SyncYPathGet(IYPathServicePtr service, const TYPath& path)
{
    auto result = AsyncYPathGet(service, path).Get();
    if (!result.IsOK()) {
        ythrow yexception() << result.GetMessage();
    }
    return result.Value();
}

void SyncYPathSet(IYPathServicePtr service, const TYPath& path, const TYsonString& value)
{
    auto request = TYPathProxy::Set(path);
    request->set_value(value.Data());
    auto response = ExecuteVerb(service, request).Get();
    response->ThrowIfError();
}

void SyncYPathRemove(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::Remove(path);
    auto response = ExecuteVerb(service, request).Get();
    response->ThrowIfError();
}

std::vector<Stroka> SyncYPathList(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::List(path);
    auto response = ExecuteVerb(service, request).Get();
    response->ThrowIfError();
    return ConvertTo<std::vector<Stroka> >(TYsonString(response->keys()));
}

void ApplyYPathOverride(INodePtr root, const TStringBuf& overrideString)
{
    TTokenizer tokenizer(overrideString);

    TYPath path;
    while (true) {
        if (!tokenizer.ParseNext()) {
            ythrow yexception() << "Unexpected end-of-stream while parsing YPath override";
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
            std::cerr << key << std::endl;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
