#include "private.h"
#include "helpers.h"

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NSequoiaClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

std::vector<TYPathBuf> TokenizeUnresolvedSuffix(const TYPath& unresolvedSuffix)
{
    constexpr auto TypicalPathTokenCount = 3;
    std::vector<TYPathBuf> pathTokens;
    pathTokens.reserve(TypicalPathTokenCount);

    TTokenizer tokenizer(unresolvedSuffix);
    tokenizer.Advance();

    while (tokenizer.GetType() != ETokenType::EndOfStream) {
        tokenizer.Expect(ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);
        pathTokens.push_back(tokenizer.GetToken());
        tokenizer.Advance();
    };

    return pathTokens;
}

////////////////////////////////////////////////////////////////////////////////

TYPath JoinNestedNodesToPath(
    const TYPath& parentPath,
    const std::vector<TYPathBuf>& childKeys)
{
    TStringBuilder builder;

    auto nestedLength = std::accumulate(
        childKeys.begin(),
        childKeys.end(),
        size_t(0),
        [] (size_t result, TYPathBuf childKey) {
            return result + childKey.size() + 1;
        });
    builder.Reserve(parentPath.size() + nestedLength);

    builder.AppendString(parentPath);
    for (auto childKey : childKeys) {
        builder.AppendChar('/');
        builder.AppendString(childKey);
    }
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(EObjectType type)
{
    return IsSequoiaCompositeNodeType(type) || IsScalarType(type);
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

////////////////////////////////////////////////////////////////////////////////

void ThrowAlreadyExists(const TYPath& path)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::AlreadyExists,
        "Node %v already exists",
        path);
}

void ThrowNoSuchChild(const TYPath& existingPath, const TYPathBuf& missingPath)
{
    THROW_ERROR_EXCEPTION(
        NYTree::EErrorCode::ResolveError,
        "Node %v has no child with key %Qv",
        existingPath,
        missingPath);
}

////////////////////////////////////////////////////////////////////////////////

// TODO(h0pless): Change this to TFuture<std::vector>.
std::vector<NRecords::TPathToNodeId> SelectSubtree(
    const TYPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    auto mangledPath = MangleSequoiaPath(path);
    return WaitFor(transaction->SelectRows<NRecords::TPathToNodeIdKey>(
        {
            Format("path >= %Qv", mangledPath),
            Format("path <= %Qv", MakeLexicographicallyMaximalMangledSequoiaPathForPrefix(mangledPath)),
        }))
        .ValueOrThrow();
}

TNodeId LookupNodeId(
    const TYPath& path,
    const ISequoiaTransactionPtr& transaction)
{
    NRecords::TPathToNodeIdKey nodeKey{
        .Path = MangleSequoiaPath(path),
    };
    auto rows = WaitFor(transaction->LookupRows<NRecords::TPathToNodeIdKey>({nodeKey}))
        .ValueOrThrow();
    if (rows.size() != 1) {
        YT_LOG_ALERT("Unexpected number of rows received while looking up a node by its path "
            "(Path: %v, RowCount: %v)",
            path,
            rows.size());
    } else if (!rows[0]) {
        YT_LOG_ALERT("Row with null value received while looking up a node by its path (Path: %v)",
            path);
    }

    return rows[0]->NodeId;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NCypressServer::NProto::TReqCloneNode::TCloneOptions* protoOptions, const TCopyOptions& options)
{
    protoOptions->set_mode(static_cast<int>(options.Mode));
    protoOptions->set_preserve_acl(options.PreserveAcl);
    protoOptions->set_preserve_account(options.PreserveAccount);
    protoOptions->set_preserve_owner(options.PreserveOwner);
    protoOptions->set_preserve_creation_time(options.PreserveCreationTime);
    protoOptions->set_preserve_modification_time(options.PreserveModificationTime);
    protoOptions->set_preserve_expiration_time(options.PreserveExpirationTime);
    protoOptions->set_preserve_expiration_timeout(options.PreserveExpirationTimeout);
    protoOptions->set_pessimistic_quota_check(options.PessimisticQuotaCheck);
}

void FromProto(TCopyOptions* options, const NCypressClient::NProto::TReqCopy& protoOptions)
{
    options->Mode = CheckedEnumCast<ENodeCloneMode>(protoOptions.mode());
    options->PreserveAcl = protoOptions.preserve_acl();
    options->PreserveAccount = protoOptions.preserve_account();
    options->PreserveOwner = protoOptions.preserve_owner();
    options->PreserveCreationTime = protoOptions.preserve_creation_time();
    options->PreserveModificationTime = protoOptions.preserve_modification_time();
    options->PreserveExpirationTime = protoOptions.preserve_expiration_time();
    options->PreserveExpirationTimeout = protoOptions.preserve_expiration_timeout();
    options->PessimisticQuotaCheck = protoOptions.pessimistic_quota_check();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
