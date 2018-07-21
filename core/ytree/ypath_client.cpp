#include "ypath_client.h"
#include "helpers.h"
#include "exception_helpers.h"
#include "ypath_detail.h"
#include "ypath_proxy.h"

#include <yt/core/misc/serialize.h>

#include <yt/core/net/address.h>

#include <yt/core/bus/bus.h>

#include <yt/core/rpc/message.h>
#include <yt/core/rpc/proto/rpc.pb.h>
#include <yt/core/rpc/server_detail.h>

#include <yt/core/ypath/token.h>
#include <yt/core/ypath/tokenizer.h>

#include <yt/core/yson/format.h>
#include <yt/core/yson/tokenizer.h>

#include <yt/core/ytree/proto/ypath.pb.h>

#include <cmath>

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NYPath;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TYPathRequest::TYPathRequest(const TRequestHeader& header)
    : Header_(header)
{ }

TYPathRequest::TYPathRequest(
    const TString& service,
    const TString& method,
    const TYPath& path,
    bool mutating)
{
    Header_.set_service(service);
    Header_.set_method(method);

    auto* ypathExt = Header_.MutableExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    ypathExt->set_mutating(mutating);
    ypathExt->set_path(path);
}

bool TYPathRequest::IsHeavy() const
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

const TString& TYPathRequest::GetMethod() const
{
    return Header_.method();
}

const TString& TYPathRequest::GetService() const
{
    return Header_.service();
}

void TYPathRequest::SetUser(const TString& /*user*/)
{
    Y_UNREACHABLE();
}

const TString& TYPathRequest::GetUser() const
{
    Y_UNREACHABLE();
}

void TYPathRequest::SetUserAgent(const TString& userAgent)
{
    Y_UNREACHABLE();
}

bool TYPathRequest::GetRetry() const
{
    return Header_.retry();
}

void TYPathRequest::SetRetry(bool value)
{
    Header_.set_retry(value);
}

TMutationId TYPathRequest::GetMutationId() const
{
    return FromProto<TMutationId>(Header_.mutation_id());
}

void TYPathRequest::SetMutationId(const TMutationId& id)
{
    if (id) {
        ToProto(Header_.mutable_mutation_id(), id);
    } else {
        Header_.clear_mutation_id();
    }
}

size_t TYPathRequest::GetHash() const
{
    return 0;
}

EMultiplexingBand TYPathRequest::GetMultiplexingBand() const
{
    return EMultiplexingBand::Default;
}

void TYPathRequest::SetMultiplexingBand(EMultiplexingBand /*band*/)
{
    Y_UNREACHABLE();
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
    Y_ASSERT(message);

    NRpc::NProto::TResponseHeader header;
    if (!ParseResponseHeader(message, &header)) {
        THROW_ERROR_EXCEPTION("Error parsing response header");
    }

    if (header.has_error()) {
        auto error = NYT::FromProto<TError>(header.error());
        error.ThrowOnError();
    }

    // Deserialize body.
    Y_ASSERT(message.Size() >= 2);
    DeserializeBody(message[1]);

    // Load attachments.
    Attachments_ = std::vector<TSharedRef>(message.Begin() + 2, message.End());
}

void TYPathResponse::DeserializeBody(const TRef& data)
{
    Y_UNUSED(data);
}

////////////////////////////////////////////////////////////////////////////////

const TYPath& GetRequestYPath(const NRpc::NProto::TRequestHeader& header)
{
    const auto& ext = header.GetExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    return ext.path();
}

void SetRequestYPath(NRpc::NProto::TRequestHeader* header, const TYPath& path)
{
    auto* ext = header->MutableExtension(NProto::TYPathHeaderExt::ypath_header_ext);
    ext->set_path(path);
}

void ResolveYPath(
    const IYPathServicePtr& rootService,
    const IServiceContextPtr& context,
    IYPathServicePtr* suffixService,
    TYPath* suffixPath)
{
    Y_ASSERT(rootService);
    Y_ASSERT(suffixService);
    Y_ASSERT(suffixPath);

    auto currentService = rootService;

    const auto& path = GetRequestYPath(context->RequestHeader());
    auto currentPath = path;

    int iteration = 0;
    while (true) {
        if (++iteration > MaxYPathResolveIterations) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "Path %v exceeds resolve depth limit",
                path)
                << TErrorAttribute("limit", MaxYPathResolveIterations);
        }

        try {
            auto result = currentService->Resolve(currentPath, context);
            if (auto* hereResult = result.TryAs<IYPathService::TResolveResultHere>()) {
                *suffixService = currentService;
                *suffixPath = std::move(hereResult->Path);
                break;
            }
            auto& thereResult = result.As<IYPathService::TResolveResultThere>();
            currentService = std::move(thereResult.Service);
            currentPath = std::move(thereResult.Path);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "Error resolving path %v",
                path)
                << TErrorAttribute("method", context->GetMethod())
                << ex;
        }
    }
}

TFuture<TSharedRefArray> ExecuteVerb(
    const IYPathServicePtr& service,
    const TSharedRefArray& requestMessage)
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

    auto invokeContext = CreateYPathContext(std::move(updatedRequestMessage));

    // NB: Calling GetAsyncResponseMessage after Invoke is not allowed.
    auto asyncResponseMessage = invokeContext->GetAsyncResponseMessage();

    // This should never throw.
    suffixService->Invoke(invokeContext);

    return asyncResponseMessage;
}

void ExecuteVerb(
    const IYPathServicePtr& service,
    const IServiceContextPtr& context)
{
    IYPathServicePtr suffixService;
    TYPath suffixPath;
    try {
        ResolveYPath(
            service,
            context,
            &suffixService,
            &suffixPath);
    } catch (const std::exception& ex) {
        context->Reply(ex);
        return;
    }

    auto requestMessage = context->GetRequestMessage();
    NRpc::NProto::TRequestHeader requestHeader;
    YCHECK(ParseRequestHeader(requestMessage, &requestHeader));
    SetRequestYPath(&requestHeader, suffixPath);

    auto updatedRequestMessage = SetRequestHeader(requestMessage, requestHeader);

    class TInvokeContext
        : public TServiceContextBase
    {
    public:
        TInvokeContext(
            TSharedRefArray requestMessage,
            IServiceContextPtr underlyingContext)
            : TServiceContextBase(
                std::move(requestMessage),
                underlyingContext->GetLogger(),
                underlyingContext->GetLogLevel())
            , UnderlyingContext_(std::move(underlyingContext))
        { }

        virtual TTcpDispatcherStatistics GetBusStatistics() const override
        {
            return UnderlyingContext_->GetBusStatistics();
        }

        virtual const IAttributeDictionary& GetEndpointAttributes() const override
        {
            return UnderlyingContext_->GetEndpointAttributes();
        }

        virtual void SetRawRequestInfo(const TString& info) override
        {
            UnderlyingContext_->SetRawRequestInfo(info);
        }

        virtual void SetRawResponseInfo(const TString& info) override
        {
            UnderlyingContext_->SetRawResponseInfo(info);
        }

    private:
        const IServiceContextPtr UnderlyingContext_;


        virtual void LogRequest() override
        { }

        virtual void LogResponse() override
        { }

        virtual void DoReply() override
        {
            UnderlyingContext_->Reply(GetResponseMessage());
        }
    };

    auto invokeContext = New<TInvokeContext>(
        std::move(updatedRequestMessage),
        context);

    // This should never throw.
    suffixService->Invoke(std::move(invokeContext));
}

TFuture<TYsonString> AsyncYPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TNullable<std::vector<TString>>& attributeKeys)
{
    auto request = TYPathProxy::Get(path);
    if (attributeKeys) {
        ToProto(request->mutable_attributes()->mutable_keys(), *attributeKeys);
    }
    return ExecuteVerb(service, request)
        .Apply(BIND([] (TYPathProxy::TRspGetPtr response) {
            return TYsonString(response->value());
        }));
}

TString SyncYPathGetKey(const IYPathServicePtr& service, const TYPath& path)
{
    auto request = TYPathProxy::GetKey(path);
    return ExecuteVerb(service, request)
        .Get()
        .ValueOrThrow()->value();
}

TYsonString SyncYPathGet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TNullable<std::vector<TString>>& attributeKeys)
{
    return
        AsyncYPathGet(
            service,
            path,
            attributeKeys)
        .Get()
        .ValueOrThrow();
}

TFuture<bool> AsyncYPathExists(
    const IYPathServicePtr& service,
    const TYPath& path)
{
    auto request = TYPathProxy::Exists(path);
    return ExecuteVerb(service, request)
        .Apply(BIND([] (TYPathProxy::TRspExistsPtr response) {
            return response->value();
        }));
}

bool SyncYPathExists(
    const IYPathServicePtr& service,
    const TYPath& path)
{
    return AsyncYPathExists(service, path)
        .Get()
        .ValueOrThrow();
}

void SyncYPathSet(
    const IYPathServicePtr& service,
    const TYPath& path,
    const TYsonString& value,
    bool recursive)
{
    auto request = TYPathProxy::Set(path);
    request->set_value(value.GetData());
    request->set_recursive(recursive);
    ExecuteVerb(service, request)
        .Get()
        .ThrowOnError();
}

void SyncYPathRemove(
    const IYPathServicePtr& service,
    const TYPath& path,
    bool recursive,
    bool force)
{
    auto request = TYPathProxy::Remove(path);
    request->set_recursive(recursive);
    request->set_force(force);
    ExecuteVerb(service, request)
        .Get()
        .ThrowOnError();
}

std::vector<TString> SyncYPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    TNullable<i64> limit)
{
    return AsyncYPathList(service, path, limit)
        .Get()
        .ValueOrThrow();
}

TFuture<std::vector<TString>> AsyncYPathList(
    const IYPathServicePtr& service,
    const TYPath& path,
    TNullable<i64> limit)
{
    auto request = TYPathProxy::List(path);
    if (limit) {
        request->set_limit(*limit);
    }
    return ExecuteVerb(service, request)
        .Apply(BIND([] (TYPathProxy::TRspListPtr response) {
            return ConvertTo<std::vector<TString>>(TYsonString(response->value()));
        }));
}

static INodePtr WalkNodeByYPath(
    const INodePtr& root,
    const TYPath& path,
    std::function<INodePtr(const TString&)> handleMissingAttribute,
    std::function<INodePtr(const IMapNodePtr&, const TString&)> handleMissingChildKey,
    std::function<INodePtr(const IListNodePtr&, int)> handleMissingChildIndex)
{
    auto currentNode = root;
    NYPath::TTokenizer tokenizer(path);
    while (true) {
        tokenizer.Skip(NYPath::ETokenType::Ampersand);
        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            break;
        }
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        if (tokenizer.GetType() == NYPath::ETokenType::At) {
            tokenizer.Advance();
            if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
                return currentNode->Attributes().ToMap();
            } else {
                tokenizer.Expect(NYPath::ETokenType::Literal);
                const auto key = tokenizer.GetLiteralValue();
                const auto& attributes = currentNode->Attributes();
                currentNode = attributes.Find<INodePtr>(key);
                if (!currentNode) {
                    return handleMissingAttribute(key);
                }
            }
        } else {
            tokenizer.Expect(NYPath::ETokenType::Literal);
            switch (currentNode->GetType()) {
                case ENodeType::Map: {
                    auto currentMap = currentNode->AsMap();
                    auto key = tokenizer.GetLiteralValue();
                    currentNode = currentMap->FindChild(key);
                    if (!currentNode) {
                        return handleMissingChildKey(currentMap, key);
                    }
                    break;
                }
                case ENodeType::List: {
                    auto currentList = currentNode->AsList();
                    const auto& token = tokenizer.GetToken();
                    int index = ParseListIndex(token);
                    int adjustedIndex = currentList->AdjustChildIndex(index);
                    currentNode = currentList->FindChild(adjustedIndex);
                    if (!currentNode) {
                        return handleMissingChildIndex(currentList, adjustedIndex);
                    }
                    break;
                }
                default:
                    ThrowCannotHaveChildren(currentNode);
                    Y_UNREACHABLE();
            }
        }
    }
    return currentNode;
}

INodePtr GetNodeByYPath(
    const INodePtr& root,
    const TYPath& path)
{
    return WalkNodeByYPath(
        root,
        path,
        [] (const TString& key) {
            ThrowNoSuchAttribute(key);
            return nullptr;
        },
        [] (const IMapNodePtr& node, const TString& key) {
            ThrowNoSuchChildKey(node, key);
            return nullptr;
        },
        [] (const IListNodePtr& node, int index) {
            ThrowNoSuchChildIndex(node, index);
            return nullptr;
        }
    );
}

INodePtr FindNodeByYPath(
    const INodePtr& root,
    const TYPath& path)
{
    return WalkNodeByYPath(
        root,
        path,
        [] (const TString& key) {
            return nullptr;
        },
        [] (const IMapNodePtr& node, const TString& key) {
            return nullptr;
        },
        [] (const IListNodePtr& node, int index) {
            return nullptr;
        }
    );
}

void SetNodeByYPath(
    const INodePtr& root,
    const TYPath& path,
    const INodePtr& value)
{
    auto currentNode = root;

    NYPath::TTokenizer tokenizer(path);

    TString currentToken;
    TString currentLiteralValue;
    auto nextSegment = [&] () {
        tokenizer.Skip(NYPath::ETokenType::Ampersand);
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        currentToken = TString(tokenizer.GetToken());
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
                Y_UNREACHABLE();
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
                YCHECK(currentMap->AddChild(key, value));
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
            Y_UNREACHABLE();
    }
}

void ForceYPath(
    const INodePtr& root,
    const TYPath& path)
{
    auto currentNode = root;

    NYPath::TTokenizer tokenizer(path);

    TString currentToken;
    TString currentLiteralValue;
    auto nextSegment = [&] () {
        tokenizer.Skip(NYPath::ETokenType::Ampersand);
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        currentToken = TString(tokenizer.GetToken());
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
                    YCHECK(currentMap->AddChild(key, child));
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
                Y_UNREACHABLE();
        }

        nextSegment();
        currentNode = child;
    }

    factory->Commit();
}

INodePtr CloneNode(const INodePtr& node)
{
    return ConvertToNode(node);
}

INodePtr PatchNode(const INodePtr& base, const INodePtr& patch)
{
    if (base->GetType() == ENodeType::Map && patch->GetType() == ENodeType::Map) {
        auto result = CloneNode(base);
        auto resultMap = result->AsMap();
        auto patchMap = patch->AsMap();
        auto baseMap = base->AsMap();
        for (const auto& key : patchMap->GetKeys()) {
            if (baseMap->FindChild(key)) {
                resultMap->RemoveChild(key);
                YCHECK(resultMap->AddChild(key, PatchNode(baseMap->GetChild(key), patchMap->GetChild(key))));
            } else {
                YCHECK(resultMap->AddChild(key, CloneNode(patchMap->GetChild(key))));
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

bool AreNodesEqual(const INodePtr& lhs, const INodePtr& rhs)
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
            return lhs->GetValue<TString>() == rhs->GetValue<TString>();

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
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
