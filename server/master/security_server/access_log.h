#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

#include <yt/server/master/transaction_server/transaction.h>

#include <yt/client/object_client/public.h>

#define YT_LOG_ACCESS(...) \
    do { \
        if (Bootstrap_->GetConfigManager()->GetConfig()->SecurityManager->EnableAccessLog && \
            !Bootstrap_->GetHydraFacade()->GetHydraManager()->IsLeader() && \
            !Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery()) \
        { \
            LogAccess(Bootstrap_, __VA_ARGS__); \
        } \
    } while (false)

#define YT_LOG_ACCESS_IF(predicate, ...) \
    if (predicate) { \
        YT_LOG_ACCESS(__VA_ARGS__); \
    }

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const NRpc::IServiceContextPtr& context,
    const TStringBuf path,
    const NTransactionServer::TTransaction* transaction,
    const TAttributeVector& additionalAttributes = {},
    const std::optional<TStringBuf> methodOverride = std::nullopt);

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const TStringBuf path,
    const NTransactionServer::TTransaction* transaction,
    const TStringBuf method);

bool IsAccessLoggedType(const NCypressClient::EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
