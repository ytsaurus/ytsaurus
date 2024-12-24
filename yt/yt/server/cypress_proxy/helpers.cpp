#include "helpers.h"

#include "path_resolver.h"
#include "sequoia_service.h"

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/yson/attribute_consumer.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressClient::NProto;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NYPath;
using namespace NYTree;

using TYPath = NSequoiaClient::TYPath;
using TYPathBuf = NSequoiaClient::TYPathBuf;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void SetAccessTrackingOptions(
    const IClientRequestPtr& request,
    const TSuppressableAccessTrackingOptions& commandOptions)
{
    if (commandOptions.SuppressAccessTracking) {
        NCypressClient::SetSuppressAccessTracking(request, true);
    }
    if (commandOptions.SuppressModificationTracking) {
        NCypressClient::SetSuppressModificationTracking(request, true);
    }
    if (commandOptions.SuppressExpirationTimeoutRenewal) {
        NCypressClient::SetSuppressExpirationTimeoutRenewal(request, true);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TYPathBuf SkipAmpersand(TYPathBuf pathSuffix)
{
    TTokenizer tokenizer(pathSuffix.Underlying());
    tokenizer.Advance();
    tokenizer.Skip(ETokenType::Ampersand);
    return TYPathBuf(tokenizer.GetInput());
}

TAbsoluteYPath GetCanonicalYPath(const TResolveResult& resolveResult)
{
    return Visit(resolveResult,
        [] (const TCypressResolveResult& resolveResult) -> TAbsoluteYPath {
            // NB: Cypress resolve result doesn't contain unresolved links.
            return TAbsoluteYPath(resolveResult.Path);
        },
        [] (const TSequoiaResolveResult& resolveResult) -> TAbsoluteYPath {
            // We don't want to distinguish "//tmp/a&/my-link" from
            // "//tmp/a/my-link".
            return resolveResult.Path + SkipAmpersand(resolveResult.UnresolvedSuffix);
        });
}

} // namespace

void ValidateLinkNodeCreation(
    const TSequoiaSessionPtr& session,
    TRawYPath targetPath,
    const TResolveResult& resolveResult)
{
    // TODO(danilalexeev): In case of a master-object root designator the
    // following resolve will not produce a meaningful result. Such YPath has to
    // be resolved by master first.
    // TODO(kvk1920): probably works (since links are stored in both resolve
    // tables now), but has to be tested.
    auto linkPath = GetCanonicalYPath(resolveResult);

    auto checkAcyclicity = [&] (
        TRawYPath pathToResolve,
        const TAbsoluteYPath& forbiddenPrefix)
    {
        std::vector<TSequoiaResolveIterationResult> history;
        auto resolveResult = ResolvePath(session, std::move(pathToResolve), /*method*/ {}, &history);

        for (const auto& [id, path] : history) {
            if (IsLinkType(TypeFromId(id)) && path == forbiddenPrefix) {
                return false;
            }
        }

        return GetCanonicalYPath(resolveResult) != forbiddenPrefix;
    };

    if (!checkAcyclicity(targetPath, linkPath)) {
        THROW_ERROR_EXCEPTION("Failed to create link: link is cyclic")
            << TErrorAttribute("target_path", targetPath.Underlying())
            << TErrorAttribute("path", linkPath);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> TokenizeUnresolvedSuffix(TYPathBuf unresolvedSuffix)
{
    constexpr auto TypicalPathTokenCount = 3;
    std::vector<std::string> pathTokens;
    pathTokens.reserve(TypicalPathTokenCount);

    TTokenizer tokenizer(unresolvedSuffix.Underlying());
    tokenizer.Advance();

    while (tokenizer.GetType() != ETokenType::EndOfStream) {
        tokenizer.Expect(ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);
        pathTokens.push_back(tokenizer.GetLiteralValue());
        tokenizer.Advance();
    }

    return pathTokens;
}

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(EObjectType type)
{
    return IsSequoiaCompositeNodeType(type) ||
        IsScalarType(type) ||
        IsChunkOwnerType(type) ||
        type == EObjectType::SequoiaLink;
}

bool IsSequoiaCompositeNodeType(EObjectType type)
{
    return type == EObjectType::SequoiaMapNode || type == EObjectType::Scion;
}

void ValidateSupportedSequoiaType(EObjectType type)
{
    if (!IsSupportedSequoiaType(type)) {
        THROW_ERROR_EXCEPTION(
            "Object type %Qlv is not supported in Sequoia yet",
            type);
    }
}

void ThrowAlreadyExists(const TAbsoluteYPath& path)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::AlreadyExists,
        "Node %v already exists",
        path);
}

void ThrowNoSuchChild(const TAbsoluteYPath& existingPath, TStringBuf missingPath)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Node %v has no child with key %Qv",
        existingPath,
        missingPath);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TParsedReqCreate> TryParseReqCreate(ISequoiaServiceContextPtr context)
{
    YT_VERIFY(context->GetRequestHeader().method() == "Create");

    auto typedContext = New<TTypedSequoiaServiceContext<TReqCreate, TRspCreate>>(
        std::move(context),
        THandlerInvocationOptions{});

    // NB: this replies to underlying context on error.
    if (!typedContext->DeserializeRequest()) {
        return std::nullopt;
    }

    const auto& request = typedContext->Request();

    try {
        return TParsedReqCreate{
            .Type = FromProto<EObjectType>(request.type()),
            .ExplicitAttributes = request.has_node_attributes()
                ? NYTree::FromProto(request.node_attributes())
                : CreateEphemeralAttributes(),
        };
    } catch (const std::exception& ex) {
        typedContext->Reply(ex);
        return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

void ConsumeAttributes(NYson::IAsyncYsonConsumer* consumer, const IAttributeDictionaryPtr& attributes)
{
    NYson::TAttributeFragmentConsumer attributeConsumer(consumer);
    for (const auto& [key, value] : attributes->ListPairs()) {
        attributeConsumer.OnKeyedItem(key);
        attributeConsumer.OnRaw(value);
    }
    attributeConsumer.Finish();
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TCopyOptions* options, const TReqCopy& protoOptions)
{
    options->Mode = FromProto<ENodeCloneMode>(protoOptions.mode());
    options->PreserveAcl = protoOptions.preserve_acl();
    options->PreserveAccount = protoOptions.preserve_account();
    options->PreserveOwner = protoOptions.preserve_owner();
    options->PreserveCreationTime = protoOptions.preserve_creation_time();
    options->PreserveModificationTime = protoOptions.preserve_modification_time();
    options->PreserveExpirationTime = protoOptions.preserve_expiration_time();
    options->PreserveExpirationTimeout = protoOptions.preserve_expiration_timeout();
    options->PessimisticQuotaCheck = protoOptions.pessimistic_quota_check();
}

void FromProto(
    TMultisetAttributesSubrequest* subrequest,
    const NYTree::NProto::TReqMultisetAttributes::TSubrequest& protoSubrequest)
{
    subrequest->AttributeKey = protoSubrequest.attribute();
    subrequest->Value = NYson::TYsonString(protoSubrequest.value());
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NYTree::INodePtr> FetchSingleObject(
    const NNative::IClientPtr& client,
    TVersionedObjectId objectId,
    const TAttributeFilter& attributeFilter)
{
    auto request = TYPathProxy::Get();

    if (objectId.TransactionId) {
        SetTransactionId(request, objectId.TransactionId);
    }

    if (attributeFilter) {
        ToProto(request->mutable_attributes(), attributeFilter);
    }

    auto batcher = TMasterYPathProxy::CreateGetBatcher(client, request, {objectId.ObjectId});

    return batcher.Invoke().Apply(BIND([=] (const TMasterYPathProxy::TVectorizedGetBatcher::TVectorizedResponse& rsp) {
        return ConvertToNode(NYson::TYsonString(rsp.at(objectId.ObjectId).ValueOrThrow()->value()));
    }));
}

////////////////////////////////////////////////////////////////////////////////

std::string GetRequestQueueNameForKey(const std::pair<std::string, EUserWorkloadType>& userNameAndWorkloadType)
{
    return Format(
        "%v_%v",
        userNameAndWorkloadType.first,
        CamelCaseToUnderscoreCase(TEnumTraits<EUserWorkloadType>::ToString(userNameAndWorkloadType.second)));
}

std::string GetDistributedWeightThrottlerId(const std::string& prefix)
{
    return prefix + "_weight_throttler";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
