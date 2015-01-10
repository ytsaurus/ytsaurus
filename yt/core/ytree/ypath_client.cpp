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

#include <core/ytree/ypath.pb.h>

#include <cmath>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TYPathRequest::TYPathRequest(const TRequestHeader& header)
    : Header_(header)
{ }

TYPathRequest::TYPathRequest(
    const Stroka& service,
    const Stroka& method,
    const TYPath& path,
    bool mutating)
{
    Header_.set_service(service);
    Header_.set_method(method);

    auto* headerExt = Header_.MutableExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    headerExt->set_mutating(mutating);
    headerExt->set_path(path);
}

bool TYPathRequest::IsOneWay() const
{
    return false;
}

bool TYPathRequest::IsRequestHeavy() const
{
    return false;
}

bool TYPathRequest::IsResponseHeavy() const
{
    return false;
}

TRequestId TYPathRequest::GetRequestId() const
{
    return NullRequestId;
}

TRealmId TYPathRequest::GetRealmId() const
{
    return NullRealmId;
}

const Stroka& TYPathRequest::GetMethod() const
{
    return Header_.method();
}

const Stroka& TYPathRequest::GetService() const
{
    return Header_.service();
}

TInstant TYPathRequest::GetStartTime() const
{
    YUNREACHABLE();
}

void TYPathRequest::SetStartTime(TInstant /*value*/)
{
    YUNREACHABLE();
}

const NRpc::NProto::TRequestHeader& TYPathRequest::Header() const
{
    return Header_;
}

NRpc::NProto::TRequestHeader& TYPathRequest::Header()
{
    return Header_;
}

TSharedRefArray TYPathRequest::Serialize()
{
    auto bodyData = SerializeBody();
    return CreateRequestMessage(
        Header_,
        std::move(bodyData),
        Attachments_);
}

////////////////////////////////////////////////////////////////////////////////

void TYPathResponse::Deserialize(TSharedRefArray message)
{
    YASSERT(message);

    NRpc::NProto::TResponseHeader header;
    if (!ParseResponseHeader(message, &header)) {
        THROW_ERROR_EXCEPTION("Error parsing response header");
    }

    if (header.has_error()) {
        auto error = NYT::FromProto<TError>(header.error());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    // Deserialize body.
    YASSERT(message.Size() >= 2);
    DeserializeBody(message[1]);

    // Load attachments.
    Attachments_ = std::vector<TSharedRef>(message.Begin() + 2, message.End());
}

void TYPathResponse::DeserializeBody(const TRef& data)
{
    UNUSED(data);
}

////////////////////////////////////////////////////////////////////////////////

const TYPath& GetRequestYPath(IServiceContextPtr context)
{
    return GetRequestYPath(context->RequestHeader());
}

const TYPath& GetRequestYPath(const NRpc::NProto::TRequestHeader& header)
{
    const auto& headerExt = header.GetExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    return headerExt.path();
}

void SetRequestYPath(NRpc::NProto::TRequestHeader* header, const TYPath& path)
{
    auto* headerExt = header->MutableExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    headerExt->set_path(path);
}

TYPath ComputeResolvedYPath(const TYPath& wholePath, const TYPath& unresolvedPath)
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

    auto currentService = rootService;

    const auto& path = GetRequestYPath(context);
    auto currentPath = path;

    while (true) {
        IYPathService::TResolveResult result;
        try {
            result = currentService->Resolve(currentPath, context);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "Error resolving path %v",
                path)
                << TErrorAttribute("method", context->GetMethod())
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

TFuture<TSharedRefArray>
ExecuteVerb(IYPathServicePtr service, TSharedRefArray requestMessage)
{
    IYPathServicePtr suffixService;
    TYPath suffixPath;
    try {
        auto resolveContext = CreateYPathContext(requestMessage);
        ResolveYPath(
            service,
            resolveContext,
            &suffixService,
            &suffixPath);
    } catch (const std::exception& ex) {
        return MakeFuture(CreateErrorResponseMessage(ex));
    }

    NRpc::NProto::TRequestHeader requestHeader;
    YCHECK(ParseRequestHeader(requestMessage, &requestHeader));
    SetRequestYPath(&requestHeader, suffixPath);

    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    auto invokeContext = CreateYPathContext(
        std::move(updatedRequestMessage),
        suffixService->GetLogger());

    // NB: Calling GetAsyncResponseMessage after Invoke is not allowed.
    auto asyncResponseMessage = invokeContext->GetAsyncResponseMessage();

    // This should never throw.
    suffixService->Invoke(invokeContext);

    return asyncResponseMessage;
}

void ExecuteVerb(IYPathServicePtr service, IServiceContextPtr context)
{
    auto requestMessage = context->GetRequestMessage();
    auto asyncResponseMessage = ExecuteVerb(service, requestMessage);
    context->ReplyFrom(asyncResponseMessage);
}

TFuture<TYsonString> AsyncYPathGet(
    IYPathServicePtr service,
    const TYPath& path,
    const TAttributeFilter& attributeFilter,
    bool ignoreOpaque)
{
    auto request = TYPathProxy::Get(path);
    ToProto(request->mutable_attribute_filter(), attributeFilter);
    request->set_ignore_opaque(ignoreOpaque);
    return ExecuteVerb(service, request)
        .Apply(BIND([] (TYPathProxy::TRspGetPtr response) {
            return TYsonString(response->value());
        }));
}

Stroka SyncYPathGetKey(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::GetKey(path);
    return ExecuteVerb(service, request).Get().ValueOrThrow()->value();
}

TYsonString SyncYPathGet(
    IYPathServicePtr service,
    const TYPath& path,
    const TAttributeFilter& attributeFilter,
    bool ignoreOpaque)
{
    return
        AsyncYPathGet(
            service,
            path,
            attributeFilter,
            ignoreOpaque)
        .Get()
        .ValueOrThrow();
}

TFuture<bool> AsyncYPathExists(
    IYPathServicePtr service,
    const TYPath& path)
{
    auto request = TYPathProxy::Exists(path);
    return ExecuteVerb(service, request)
        .Apply(BIND([] (TYPathProxy::TRspExistsPtr response) {
            return response->value();
        }));
}

bool SyncYPathExists(IYPathServicePtr service, const TYPath& path)
{
    return AsyncYPathExists(service, path)
        .Get()
        .ValueOrThrow();
}

void SyncYPathSet(IYPathServicePtr service, const TYPath& path, const TYsonString& value)
{
    auto request = TYPathProxy::Set(path);
    request->set_value(value.Data());
    ExecuteVerb(service, request).Get().ThrowOnError();
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
    ExecuteVerb(service, request).Get().ThrowOnError();
}

std::vector<Stroka> SyncYPathList(IYPathServicePtr service, const TYPath& path)
{
    auto request = TYPathProxy::List(path);
    auto response = ExecuteVerb(service, request).Get().ValueOrThrow();
    return ConvertTo<std::vector<Stroka>>(TYsonString(response->keys()));
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
    for (const auto& token : tokens) {
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
        for (const auto& key : patchMap->GetKeys()) {
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
    if (lhsAttributes != rhsAttributes) {
        return false;
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

        case ENodeType::Int64:
            return lhs->GetValue<i64>() == rhs->GetValue<i64>();

        case ENodeType::Uint64:
            return lhs->GetValue<ui64>() == rhs->GetValue<ui64>();

        case ENodeType::Double:
            return std::abs(lhs->GetValue<double>() - rhs->GetValue<double>()) < 1e-6;

        case ENodeType::Boolean:
            return lhs->GetValue<bool>() == rhs->GetValue<bool>();

        case ENodeType::Entity:
            return true;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
