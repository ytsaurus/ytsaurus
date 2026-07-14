#include "access_log.h"
#include "private.h"

#include <yt/yt/server/lib/sequoia/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/rpc/authentication_identity.h>
#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ypath/token.h>

#include <library/cpp/yt/string/raw_formatter.h>

namespace NYT::NSecurityServer {

using namespace NObjectClient;
using namespace NSequoiaServer;
using namespace NLogging;
using namespace NRpc;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AccessLogger;

static constexpr int AccessLogStackBufferSize = 1024;

////////////////////////////////////////////////////////////////////////////////

namespace {

TOneShotFluentLogEvent LogStructuredEventFluently(ELogLevel level)
{
    const auto& identity = GetCurrentAuthenticationIdentity();
    return NLogging::LogStructuredEventFluently(AccessLogger(), level)
        .Item("user").Value(identity.User)
        .DoIf(identity.UserTag != identity.User && !identity.UserTag.empty(), [&] (auto fluent) {
            fluent.Item("user_tag").Value(identity.UserTag);
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void LogAccess(
    const NRpc::IServiceContextPtr& context,
    TObjectId id,
    TYPathBuf path,
    const ITransactionAccessLogInfo& transactionInfo,
    NRpc::TMutationId mutationId,
    const TAccessLogAttributes& additionalAttributes,
    std::optional<std::string> methodOverride)
{
    const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);

    auto maybeAlert = [&] (const TErrorOr<TYPath>& ypathOrError, const auto& unparsedYPath, TStringBuf ypathType) {
        if (!ypathOrError.IsOK()) {
            YT_LOG_ALERT(ypathOrError,
                "Failed to parse YPath while logging access "
                "(NodeId: %v, Path: %v, TransactionId: %v, UnparsedYPathType: %v, UnparsedYPath: %v)",
                id,
                path,
                transactionInfo.GetTransactionId(),
                ypathType,
                unparsedYPath);
            return true;
        }
        return false;
    };

    const auto& unparsedTargetSuffix = GetRequestTargetYPath(context->RequestHeader());
    auto targetSuffixOrError = TryEscapeNonAsciiYPathLiterals(unparsedTargetSuffix);
    const auto* targetSuffix = maybeAlert(targetSuffixOrError, unparsedTargetSuffix, "target_suffix")
        ? nullptr
        : &targetSuffixOrError.Value();

    auto targetSuffixIsForDestinationPath =
        context->GetMethod() == "Move" ||
        context->GetMethod() == "Copy" ||
        context->GetMethod() == "LockCopyDestination" ||
        context->GetMethod() == "AssembleTreeCopy";

    auto doPath = [&] (auto fluent, TYPathBuf path, bool appendTargetSuffix) {
        if (!appendTargetSuffix || !targetSuffix || targetSuffix->empty()) {
            fluent.Value(path);
            return;
        }

        // NB: At Cypress proxy, leading ampersand currently is not skipped during resolve.
        if (*targetSuffix == "&") {
            fluent.Value(path);
            return;
        }

        // Try to avoid allocation.
        if (path.size() + targetSuffix->size() <= AccessLogStackBufferSize) {
            TRawFormatter<AccessLogStackBufferSize> fullPath;
            fullPath.AppendString(path);
            fullPath.AppendString(*targetSuffix);
            fluent.Value(TStringBuf(fullPath.GetData(), fullPath.GetBytesWritten()));
        } else {
            fluent.Value(path + *targetSuffix);
        }
    };

    LogStructuredEventFluently(ELogLevel::Info)
        .Item("method").Value(methodOverride.value_or(context->GetMethod()))
        .Item("type").Value(MaybeConvertToCypressType(TypeFromId(id)))
        .Item("id").Value(id)
        .Item("path").Do([&] (auto fluent) {
            doPath(fluent, path, !targetSuffixIsForDestinationPath);
        })
        .DoIf(mutationId.operator bool(), [&] (auto fluent) {
            fluent.Item("mutation_id").Value(mutationId);
        })
        .Do([&] (auto fluent) {
            const TProtobufString* originalPath = nullptr;
            if (targetSuffixIsForDestinationPath) {
                // COMPAT(shakurov)
                if (ypathExt.original_additional_paths_size() == 1) {
                    originalPath = &ypathExt.original_additional_paths(0);
                }
            } else if (ypathExt.has_original_target_path()) {
                originalPath = &ypathExt.original_target_path();
            }

            if (originalPath && !originalPath->empty()) {
                const auto& unparsedOriginalPath = ::NYT::FromProto<TYPath>(*originalPath);
                auto originalPathOrError = TryEscapeNonAsciiYPathLiterals(unparsedOriginalPath);
                if (!maybeAlert(originalPathOrError, unparsedOriginalPath, "original_path")) {
                    fluent.Item("original_path").Value(originalPathOrError.Value());
                }
            }
        })
        .Do([&] (auto fluent) {
            for (const auto& pair : additionalAttributes) {
                const auto& attrName = pair.first;
                const auto& attrValue = pair.second;
                if (attrName == "destination_path") {
                    fluent.Item(attrName).Do([&] (auto fluent) {
                        doPath(fluent, attrValue, targetSuffixIsForDestinationPath);
                    });
                    // COMPAT(shakurov)
                    if (targetSuffixIsForDestinationPath && ypathExt.has_original_target_path()) {
                        const auto& unparsedOriginalDestinationPath = ::NYT::FromProto<TYPath>(ypathExt.original_target_path());
                        auto originalDestinationPathOrError = TryEscapeNonAsciiYPathLiterals(unparsedOriginalDestinationPath);
                        if (!maybeAlert(originalDestinationPathOrError, unparsedOriginalDestinationPath, "original_destination_path")) {
                            fluent.Item("original_destination_path").Value(originalDestinationPathOrError.Value());
                        }
                    }
                } else {
                    fluent.Item(attrName).Value(attrValue);
                }
            }
        })
        .DoIf(transactionInfo.GetTransactionId().operator bool(), [&] (auto fluent) {
            fluent.Item("transaction_info").DoMap([&] (auto fluent) {
                transactionInfo.WriteTransactionInfo(fluent.GetConsumer());
            });
        });
}

void LogAccess(
    TObjectId id,
    TYPathBuf path,
    const std::string& method,
    const ITransactionAccessLogInfo& transactionInfo,
    NRpc::TMutationId mutationId,
    const TAccessLogAttributes& additionalAttributes)
{
    LogStructuredEventFluently(ELogLevel::Info)
        .Item("method").Value(method)
        .Item("type").Value(MaybeConvertToCypressType(TypeFromId(id)))
        .Item("id").Value(id)
        .Item("path").Value(path)
        .DoIf(mutationId.operator bool(), [&] (auto fluent) {
            fluent.Item("mutation_id").Value(mutationId);
        })
        .DoIf(transactionInfo.GetTransactionId().operator bool(), [&] (auto fluent) {
            fluent.Item("transaction_info").DoMap([&] (auto fluent) {
                transactionInfo.WriteTransactionInfo(fluent.GetConsumer());
            });
        })
        .Do([&] (auto fluent) {
            for (const auto& [key, value] : additionalAttributes) {
                fluent.Item(key).Value(value);
            }
        });
}

void LogAccess(
    const std::string& method,
    const ITransactionAccessLogInfo& transactionInfo)
{
    YT_ASSERT(transactionInfo.GetTransactionId());

    LogStructuredEventFluently(ELogLevel::Info)
        .Item("method").Value(method)
        .Item("path").Value("")
        .Item("transaction_info").DoMap([&] (auto fluent) {
            transactionInfo.WriteTransactionInfo(fluent.GetConsumer());
        });
}

bool IsAccessLoggedType(EObjectType type)
{
    static const THashSet<EObjectType> typesForAccessLog = {
        EObjectType::File,
        EObjectType::Journal,
        EObjectType::Table,
        EObjectType::Document,
        EObjectType::MapNode,
        EObjectType::Link,
    };
    return typesForAccessLog.contains(MaybeConvertToCypressType(type));
}

bool IsAccessLoggedMethod(TStringBuf method)
{
    static const THashSet<std::string> methodsForAccessLog = {
        "Lock",
        "Unlock",
        "Get",
        "GetKey",
        "Set",
        "Remove",
        "List",
        "Exists",
        "GetBasicAttributes",
        "CheckPermission",
        "LockCopyDestination",
        "LockCopySource",
    };
    return methodsForAccessLog.contains(method);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
