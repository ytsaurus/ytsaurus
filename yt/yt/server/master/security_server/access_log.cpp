#include "access_log.h"
#include "private.h"
#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/logging/public.h>
#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <library/cpp/yt/string/raw_formatter.h>

namespace NYT::NSecurityServer {

using namespace NCypressClient;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NLogging;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr int AccessLogStackBufferSize = 1024;

////////////////////////////////////////////////////////////////////////////////

void TraverseTransactionAncestors(
    const TTransaction* transaction,
    TFluentMap& fluent)
{
    const auto* attributes = transaction->GetAttributes();

    fluent
        .Item("transaction_id").Value(transaction->GetId())
        .DoIf(transaction->GetTitle().has_value(), [&] (auto fluent) {
            fluent
                .Item("transaction_title").Value(transaction->GetTitle());
        })
        .DoIf(attributes, [&] (auto fluent) {
            const auto& attributeMap = attributes->Attributes();
            static const std::vector<TString> Keys{
                TString("operation_id"),
                TString("operation_title"),
                TString("operation_type")
            };
            for (const auto& key : Keys) {
                if (auto it = attributeMap.find(key)) {
                    fluent.Item(key).Value(it->second);
                }
            }
        })
        .DoIf(transaction->GetParent(), [&] (auto fluent) {
            fluent
                .Item("parent").BeginMap()
                    .Do([&] (auto fluent) {
                        TraverseTransactionAncestors(transaction->GetParent(), fluent);
                    })
                .EndMap();
        });
}

namespace {

TOneShotFluentLogEvent LogStructuredEventFluently(ELogLevel level)
{
    const auto& identity = NRpc::GetCurrentAuthenticationIdentity();
    return NLogging::LogStructuredEventFluently(AccessLogger(), level)
        .Item("user").Value(identity.User)
        .DoIf(identity.UserTag != identity.User, [&] (auto fluent) {
            fluent
                .Item("user_tag").Value(identity.UserTag);
        });
}

} // namespace

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const NRpc::IServiceContextPtr& context,
    NCypressServer::TNodeId id,
    const std::optional<TString>& path,
    const NTransactionServer::TTransaction* transaction,
    const TAttributeVector& additionalAttributes,
    const std::optional<TStringBuf> methodOverride)
{
    // Seeing as it has come to actually logging something, surely everything
    // has been actually evaluated by YT_EVALUATE_FOR_ACCESS_LOG, right? (Because
    // it checks for the same conditions YT_LOG_ACCESS does.) Wrong. Setting the
    // "enable_access_log" flag changes those conditions in between. Let's skip
    // logging such a request, it's no great loss.
    if (Y_UNLIKELY(!path)) {
        return;
    }

    LogAccess(
        bootstrap,
        context,
        id,
        TStringBuf(*path),
        transaction,
        additionalAttributes,
        methodOverride);
}

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const NRpc::IServiceContextPtr& context,
    NCypressServer::TNodeId id,
    const TString& path,
    const NTransactionServer::TTransaction* transaction,
    const TAttributeVector& additionalAttributes,
    const std::optional<TStringBuf> methodOverride)
{
    LogAccess(
        bootstrap,
        context,
        id,
        TStringBuf(path),
        transaction,
        additionalAttributes,
        methodOverride);
}

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const NRpc::IServiceContextPtr& context,
    TNodeId id,
    const TStringBuf path,
    const TTransaction* transaction,
    const TAttributeVector& additionalAttributes,
    const std::optional<TStringBuf> methodOverride)
{
    YT_ASSERT(bootstrap->GetConfigManager()->GetConfig()->SecurityManager->EnableAccessLog);
    YT_ASSERT(!bootstrap->GetHydraFacade()->GetHydraManager()->IsLeader());
    YT_ASSERT(!bootstrap->GetHydraFacade()->GetHydraManager()->IsRecovery());

    const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);

    const auto& targetSuffix = GetRequestTargetYPath(context->RequestHeader());
    auto targetSuffixIsForDestinationPath = context->GetMethod() == "Move" || context->GetMethod() == "Copy";

    auto doPath = [&] (auto fluent, const TStringBuf path, bool appendTargetSuffix) {
        if (!appendTargetSuffix) {
            fluent.Value(path);
            return;
        }

        // Try to avoid allocation.
        if (path.size() + targetSuffix.size() <= AccessLogStackBufferSize) {
            TRawFormatter<AccessLogStackBufferSize> fullPath;
            fullPath.AppendString(path);
            fullPath.AppendString(targetSuffix);
            fluent.Value(TStringBuf(fullPath.GetData(), fullPath.GetBytesWritten()));
        } else {
            fluent.Value(path + targetSuffix);
        }
    };

    LogStructuredEventFluently(ELogLevel::Info)
        .Item("method").Value(methodOverride.value_or(context->GetMethod()))
        .Item("type").Value(TypeFromId(id))
        .Item("id").Value(id)
        .Item("path").Do([&] (auto fluent) {
            doPath(fluent, path, !targetSuffixIsForDestinationPath);
        })
        .DoIf(NHydra::HasMutationContext(), [&] (auto fluent) {
            fluent.Item("mutation_id").Value(NHydra::GetCurrentMutationContext()->Request().MutationId);
        })
        .Do([&] (auto fluent) {
            const TString* originalPath = nullptr;
            if (targetSuffixIsForDestinationPath) {
                // COMPAT(shakurov)
                if (ypathExt.original_additional_paths_size() == 1) {
                    originalPath = &ypathExt.original_additional_paths(0);
                }
            } else if (ypathExt.has_original_target_path()) {
                originalPath = &ypathExt.original_target_path();
            }

            if (originalPath && !originalPath->empty()) {
                fluent.Item("original_path").Value(*originalPath);
            }
        })
        .Do([&] (auto fluent) {
            for (const auto& pair: additionalAttributes) {
                const auto& attrName = pair.first;
                const auto& attrValue = pair.second;
                if (attrName == "destination_path") {
                    fluent.Item(attrName).Do([&] (auto fluent) {
                        doPath(fluent, attrValue, targetSuffixIsForDestinationPath);
                    });
                    // COMPAT(shakurov)
                    if (targetSuffixIsForDestinationPath && ypathExt.has_original_target_path()) {
                        fluent.Item("original_destination_path").Value(ypathExt.original_target_path());
                    }
                } else {
                    fluent.Item(attrName).Value(attrValue);
                }
            }
        })
        .DoIf(transaction, [&] (auto fluent) {
            fluent.Item("transaction_info").DoMap([&] (auto fluent) {
                TraverseTransactionAncestors(transaction, fluent);
            });
        });
}

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    TNodeId id,
    const TStringBuf path,
    const TTransaction* transaction,
    const TStringBuf method,
    const TAttributeVector& additionalAttributes)
{
    YT_ASSERT(bootstrap->GetConfigManager()->GetConfig()->SecurityManager->EnableAccessLog);
    YT_ASSERT(!bootstrap->GetHydraFacade()->GetHydraManager()->IsLeader());
    YT_ASSERT(!bootstrap->GetHydraFacade()->GetHydraManager()->IsRecovery());

    LogStructuredEventFluently(ELogLevel::Info)
        .Item("method").Value(method)
        .Item("type").Value(TypeFromId(id))
        .Item("id").Value(id)
        .Item("path").Value(path)
        .DoIf(NHydra::HasMutationContext(), [&] (auto fluent) {
            fluent.Item("mutation_id").Value(NHydra::GetCurrentMutationContext()->Request().MutationId);
        })
        .DoIf(transaction, [&] (auto fluent) {
            fluent.Item("transaction_info").DoMap([&] (auto fluent) {
                TraverseTransactionAncestors(transaction, fluent);
            });
        })
        .Do([&] (auto fluent) {
            for (const auto& [key, value]: additionalAttributes) {
                fluent.Item(key).Value(value);
            }
        });
}

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const TStringBuf method,
    const NTransactionServer::TTransaction* transaction)
{
    YT_ASSERT(bootstrap->GetConfigManager()->GetConfig()->SecurityManager->EnableAccessLog);
    YT_ASSERT(!bootstrap->GetHydraFacade()->GetHydraManager()->IsLeader());
    YT_ASSERT(!bootstrap->GetHydraFacade()->GetHydraManager()->IsRecovery());
    YT_ASSERT(transaction);

    LogStructuredEventFluently(ELogLevel::Info)
        .Item("method").Value(method)
        .Item("path").Value("")
        .Item("transaction_info").DoMap([&] (auto fluent) {
            TraverseTransactionAncestors(transaction, fluent);
        });
}

bool IsAccessLoggedType(const EObjectType type)
{
    static const THashSet<EObjectType> typesForAccessLog = {
        EObjectType::File,
        EObjectType::Journal,
        EObjectType::Table,
        EObjectType::Document,
        EObjectType::MapNode,
        EObjectType::Link
    };
    return typesForAccessLog.contains(type);
}

bool IsAccessLogEnabled(NCellMaster::TBootstrap* bootstrap)
{
    if (!bootstrap->GetConfigManager()->GetConfig()->SecurityManager->EnableAccessLog) {
        return false;
    }

    if (!bootstrap->GetHydraFacade()->GetHydraManager()->IsLeader() &&
        !bootstrap->GetHydraFacade()->GetHydraManager()->IsRecovery())
    {
        return true;
    }

    if (bootstrap->GetCellManager()->GetTotalPeerCount() <= 1) {
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
