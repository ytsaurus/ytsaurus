#include "access_log.h"
#include "private.h"
#include "public.h"

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/logging/public.h>
#include <yt/core/logging/fluent_log.h>

#include <yt/core/misc/raw_formatter.h>

#include <yt/client/object_client/public.h>

#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/config.h>

#include <yt/server/master/transaction_server/transaction.h>

namespace NYT::NSecurityServer {

using namespace NYTree;
using namespace NTransactionServer;
using namespace NLogging;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

constexpr int AccessLogStackBufferSize = 1024;

////////////////////////////////////////////////////////////////////////////////

void TraverseTransactionAncestors(
    const TTransaction* transaction,
    TFluentMap& fluent)
{
    const auto* attributes = transaction->GetAttributes();

    fluent
        .Item("transaction_id").Value(transaction->GetId())
        .DoIf(transaction->GetTitle().has_value(), [&] (auto fluent) {
            fluent.Item("transaction_title").Value(transaction->GetTitle());
        })
        .DoIf(attributes, [&] (auto fluent) {
            const auto& attributeMap = attributes->Attributes();
            for (const auto& attribute : {"operation_id", "operation_title"}) {
                auto it = attributeMap.find(attribute);
                fluent.DoIf(it != attributeMap.end(), [&] (auto fluent) {
                    fluent.Item(attribute).Value(*it);
                });
            }
        })
        .DoIf(transaction->GetParent(), [&] (auto fluent) {
            fluent.Item("parent")
                .BeginMap()
                .Do([&] (auto fluent) {
                    TraverseTransactionAncestors(transaction->GetParent(), fluent);
                })
                .EndMap();
        });
}

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const NRpc::IServiceContextPtr& context,
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

    LogStructuredEventFluently(AccessLogger, ELogLevel::Info)
        .Item("user").Value(context->GetUser())
        .Item("method").Value(methodOverride.value_or(context->GetMethod()))
        .Item("path").Do([&] (auto fluent) {
            doPath(fluent, path, !targetSuffixIsForDestinationPath);
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
    const TStringBuf path,
    const TTransaction* transaction,
    const TStringBuf method)
{
    YT_ASSERT(bootstrap->GetConfigManager()->GetConfig()->SecurityManager->EnableAccessLog);
    YT_ASSERT(!bootstrap->GetHydraFacade()->GetHydraManager()->IsLeader());
    YT_ASSERT(!bootstrap->GetHydraFacade()->GetHydraManager()->IsRecovery());

    LogStructuredEventFluently(AccessLogger, ELogLevel::Info)
        .Item("user").Value(bootstrap->GetSecurityManager()->GetAuthenticatedUserName())
        .Item("method").Value(method)
        .Item("path").Value(path)
        .DoIf(transaction, [&] (auto fluent) {
            fluent.Item("transaction_info").DoMap([&] (auto fluent) {
                TraverseTransactionAncestors(transaction, fluent);
            });
        });
}

bool IsAccessLoggedType(const EObjectType type) {
    static const THashSet<EObjectType> typesForAccessLog = {
        EObjectType::File,
        EObjectType::Journal,
        EObjectType::Table,
        EObjectType::Document
    };
    return typesForAccessLog.contains(type);
}

} // namespace NYT::NSecurityServer
