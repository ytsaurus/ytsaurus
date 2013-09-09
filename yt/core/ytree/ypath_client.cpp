#include "stdafx.h"
#include "ypath_client.h"
#include "ypath_proxy.h"
#include "ypath_detail.h"
#include "attribute_helpers.h"
#include "exception_helpers.h"

#include <core/yson/format.h>
#include <core/yson/tokenizer.h>

#include <core/misc/serialize.h>

#include <core/rpc/rpc.pb.h>
#include <core/rpc/message.h>

#include <core/ypath/token.h>
#include <core/ypath/tokenizer.h>

#include <cmath>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TYPathRequest::TYPathRequest(const Stroka& verb, const TYPath& path)
{
    Header_.set_verb(verb);
    Header_.set_path(path);
}

bool TYPathRequest::IsOneWay() const
{
    return false;
}

bool TYPathRequest::IsHeavy() const
{
    return false;
}

TRequestId TYPathRequest::GetRequestId() const
{
    return NullRequestId;
}

const Stroka& TYPathRequest::GetVerb() const
{
    return Header_.verb();
}

const Stroka& TYPathRequest::GetPath() const
{
    return Header_.path();
}

void TYPathRequest::SetPath(const Stroka& path)
{
    Header_.set_path(path);
}

TInstant TYPathRequest::GetStartTime() const
{
    YUNREACHABLE();
}

void TYPathRequest::SetStartTime(TInstant /*value*/)
{
    YUNREACHABLE();
}

const IAttributeDictionary& TYPathRequest::Attributes() const
{
    return TEphemeralAttributeOwner::Attributes();
}

IAttributeDictionary* TYPathRequest::MutableAttributes()
{
    return TEphemeralAttributeOwner::MutableAttributes();
}

const NRpc::NProto::TRequestHeader& TYPathRequest::Header() const 
{
    return Header_;
}

NRpc::NProto::TRequestHeader& TYPathRequest::Header()
{
    return Header_;
}

IMessagePtr TYPathRequest::Serialize() const
{
    auto bodyData = SerializeBody();

    auto header = Header_;
    if (HasAttributes()) {
        ToProto(header.mutable_attributes(), Attributes());
    }

    return CreateRequestMessage(
        header,
        std::move(bodyData),
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
                NYTree::EErrorCode::ResolveError,
                "Error resolving path %s",
                ~path)
                << TErrorAttribute("verb", ~verb)
                << TErrorAttribute("resolved_path", ComputeResolvedYPath(path, currentPath))
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

TFuture< TErrorOr<TYsonString> > AsyncYPathGet(
    IYPathServicePtr service,
    const TYPath& path,
    const TAttributeFilter& attributeFilter)
{
    auto request = TYPathProxy::Get(path);
    ToProto(request->mutable_attribute_filter(), attributeFilter);
    return
        ExecuteVerb(service, request)
            .Apply(BIND([] (TYPathProxy::TRspGetPtr response) {
                return
                    response->IsOK()
                    ? TErrorOr<TYsonString>(TYsonString(response->value()))
                    : TErrorOr<TYsonString>(response->GetError());
            }));
}

Stroka SyncYPathGetKey(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::GetKey(path);
    return ExecuteVerb(service, request).Get()->value();
}

TYsonString SyncYPathGet(
    IYPathServicePtr service,
    const TYPath& path,
    const TAttributeFilter& attributeFilter)
{
    return AsyncYPathGet(service, path, attributeFilter).Get().GetValueOrThrow();
}

bool SyncYPathExists(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::Exists(path);
    return ExecuteVerb(service, request).Get()->value();
}

void SyncYPathSet(IYPathServicePtr service, const TYPath& path, const TYsonString& value)
{
    auto request = TYPathProxy::Set(path);
    request->set_value(value.Data());
    auto response = ExecuteVerb(service, request).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*response);
}

void SyncYPathRemove(
    IYPathServicePtr service,
    const TYPath& path,
    bool recursive,
    bool force)
{
    auto request = TYPathProxy::Remove(path);
    request->set_recursive(recursive);
    request->set_force(force);
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
    // TODO(babenko): this effectively forbids override path from containing "="
    int eqIndex = overrideString.find('=');
    if (eqIndex == TStringBuf::npos) {
        THROW_ERROR_EXCEPTION("Missing \"=\" in override string");
    }

    TYPath path(overrideString.begin(), overrideString.begin() + eqIndex);
    TYsonString value(Stroka(overrideString.begin() + eqIndex + 1, overrideString.end()));

    ForceYPath(root, path);
    SyncYPathSet(root, path, value);
}

INodePtr GetNodeByYPath(INodePtr root, const TYPath& path)
{
    auto currentNode = root;
    NYPath::TTokenizer tokenizer(path);
    while (tokenizer.Advance() != NYPath::ETokenType::EndOfStream) {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        switch (currentNode->GetType()) {
            case ENodeType::Map: {
                auto currentMap = currentNode->AsMap();
                auto key = tokenizer.GetLiteralValue();
                currentNode = currentMap->GetChild(key);
                break;
            }

            case ENodeType::List: {
                auto currentList = currentNode->AsList();
                const auto& token = tokenizer.GetToken();
                int index = ParseListIndex(token);
                int adjustedIndex = currentList->AdjustChildIndex(index);
                currentNode = currentList->GetChild(adjustedIndex);
                break;
            }

            default:
                ThrowCannotHaveChildren(currentNode);
                YUNREACHABLE();
        }
    }
    return currentNode;
}

void SetNodeByYPath(INodePtr root, const TYPath& path, INodePtr value)
{
    auto currentNode = root;

    NYPath::TTokenizer tokenizer(path);

    Stroka currentToken;
    Stroka currentLiteralValue;
    auto nextSegment = [&] () {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        currentToken = Stroka(tokenizer.GetToken());
        currentLiteralValue = tokenizer.GetLiteralValue();
    };

    tokenizer.Advance();
    nextSegment();

    while (tokenizer.Advance() != NYPath::ETokenType::EndOfStream) {
        switch (currentNode->GetType()) {
            case ENodeType::Map: {
                const auto& key = currentLiteralValue;
                auto currentMap = currentNode->AsMap();
                currentNode = currentMap->GetChild(key);
                break;
            }

            case ENodeType::List: {
                auto currentList = currentNode->AsList();
                int index = ParseListIndex(currentToken);
                int adjustedIndex = currentList->AdjustChildIndex(index);
                currentNode = currentList->GetChild(adjustedIndex);
                break;
            }

            default:
                ThrowCannotHaveChildren(currentNode);
                YUNREACHABLE();
        }
        nextSegment();
    }

    // Set value.
    switch (currentNode->GetType()) {
        case ENodeType::Map: {
            const auto& key = currentLiteralValue;
            auto currentMap = currentNode->AsMap();
            auto child = currentMap->FindChild(key);
            if (child) {
                currentMap->ReplaceChild(child, value);
            } else {
                YCHECK(currentMap->AddChild(value, key));
            }
            break;
        }

        case ENodeType::List: {
            auto currentList = currentNode->AsList();
            int index = ParseListIndex(currentToken);
            int adjustedIndex = currentList->AdjustChildIndex(index);
            auto child = currentList->GetChild(adjustedIndex);
            currentList->ReplaceChild(child, value);
            break;
        }

        default:
            ThrowCannotHaveChildren(currentNode);
            YUNREACHABLE();
    }
}

void ForceYPath(INodePtr root, const TYPath& path)
{
    auto currentNode = root;

    NYPath::TTokenizer tokenizer(path);

    Stroka currentToken;
    Stroka currentLiteralValue;
    auto nextSegment = [&] () {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        currentToken = Stroka(tokenizer.GetToken());
        currentLiteralValue = tokenizer.GetLiteralValue();
    };

    tokenizer.Advance();
    nextSegment();

    auto factory = root->CreateFactory();
    
    while (tokenizer.Advance() != NYPath::ETokenType::EndOfStream) {
        INodePtr child;
        switch (currentNode->GetType()) {
            case ENodeType::Map: {
                auto currentMap = currentNode->AsMap();
                const auto& key = currentLiteralValue;
                child = currentMap->AsMap()->FindChild(key);
                if (!child) {
                    child = factory->CreateMap();
                    YCHECK(currentMap->AddChild(child, key));
                }
                break;
            }

            case ENodeType::List: {
                auto currentList = currentNode->AsList();
                int index = ParseListIndex(currentToken);
                int adjustedIndex = currentList->AdjustChildIndex(index);
                child = currentList->GetChild(adjustedIndex);
                break;
            }

            default:
                ThrowCannotHaveChildren(currentNode);
                YUNREACHABLE();
        }

        nextSegment();
        currentNode = child;
    }

    factory->Commit();
}

TYPath GetNodeYPath(INodePtr node, INodePtr* root)
{
    std::vector<Stroka> tokens;
    while (true) {
        auto parent = node->GetParent();
        if (!parent) {
            break;
        }
        Stroka token;
        switch (parent->GetType()) {
            case ENodeType::List: {
                auto index = parent->AsList()->GetChildIndex(node);
                token = ToYPathLiteral(index);
                break;
            }
            case ENodeType::Map: {
                auto key = parent->AsMap()->GetChildKey(node);
                token = ToYPathLiteral(key);
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
        path.append('/');
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
        FOREACH (const auto& key, patchMap->GetKeys()) {
            if (baseMap->FindChild(key)) {
                resultMap->RemoveChild(key);
                YCHECK(resultMap->AddChild(UpdateNode(baseMap->GetChild(key), patchMap->GetChild(key)), key));
            } else {
                YCHECK(resultMap->AddChild(CloneNode(patchMap->GetChild(key)), key));
            }
        }
        result->MutableAttributes()->MergeFrom(patch->Attributes());
        return result;
    } else {
        auto result = CloneNode(patch);
        auto* resultAttributes = result->MutableAttributes();
        resultAttributes->Clear();
        if (base->GetType() == patch->GetType()) {
            resultAttributes->MergeFrom(base->Attributes());
        }
        resultAttributes->MergeFrom(patch->Attributes());
        return result;
    }
}

bool AreNodesEqual(INodePtr lhs, INodePtr rhs)
{
    // Check types.
    auto lhsType = lhs->GetType();
    auto rhsType = rhs->GetType();
    if (lhsType != rhsType) {
        return false;
    }

    // Check attributes.
    const auto& lhsAttributes = lhs->Attributes();
    const auto& rhsAttributes = rhs->Attributes();
    
    auto lhsAttributeKeys = lhsAttributes.List();
    auto rhsAttributeKeys = rhsAttributes.List();
    
    if (lhsAttributeKeys.size() != rhsAttributeKeys.size()) {
        return false;
    }

    std::sort(lhsAttributeKeys.begin(), lhsAttributeKeys.end());
    std::sort(rhsAttributeKeys.begin(), rhsAttributeKeys.end());

    for (size_t index = 0; index < lhsAttributeKeys.size(); ++index) {
        if (lhsAttributeKeys[index] != rhsAttributeKeys[index]) {
            return false;
        }
        auto lhsYson = lhsAttributes.GetYson(lhsAttributeKeys[index]);
        auto rhsYson = lhsAttributes.GetYson(rhsAttributeKeys[index]);
        auto lhsNode = ConvertToNode(lhsYson);
        auto rhsNode = ConvertToNode(rhsYson);
        if (!AreNodesEqual(lhsNode, rhsNode)) {
            return false;
        }
    }

    // Check content.
    switch (lhsType) {
        case ENodeType::Map: {
            auto lhsMap = lhs->AsMap();
            auto rhsMap = rhs->AsMap();

            auto lhsKeys = lhsMap->GetKeys();
            auto rhsKeys = rhsMap->GetKeys();

            if (lhsKeys.size() != rhsKeys.size()) {
                return false;
            }

            std::sort(lhsKeys.begin(), lhsKeys.end());
            std::sort(rhsKeys.begin(), rhsKeys.end());

            for (size_t index = 0; index < lhsKeys.size(); ++index) {
                const auto& lhsKey = lhsKeys[index];
                const auto& rhsKey = rhsKeys[index];
                if (lhsKey != rhsKey) {
                    return false;
                }
                if (!AreNodesEqual(lhsMap->FindChild(lhsKey), rhsMap->FindChild(rhsKey))) {
                    return false;
                }
            }

            return true;
        }

        case ENodeType::List: {
            auto lhsList = lhs->AsList();
            auto lhsChildren = lhsList->GetChildren();

            auto rhsList = rhs->AsList();
            auto rhsChildren = rhsList->GetChildren();

            if (lhsChildren.size() != rhsChildren.size()) {
                return false;
            }

            for (size_t index = 0; index < lhsChildren.size(); ++index) {
                if (!AreNodesEqual(lhsList->FindChild(index), rhsList->FindChild(index))) {
                    return false;
                }
            }

            return true;
        }

        case ENodeType::String:
            return lhs->GetValue<Stroka>() == rhs->GetValue<Stroka>();

        case ENodeType::Integer:
            return lhs->GetValue<i64>() == rhs->GetValue<i64>();

        case ENodeType::Double:
            return std::abs(lhs->GetValue<double>() - rhs->GetValue<double>()) < 1e-6;

        case ENodeType::Entity:
            return true;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
