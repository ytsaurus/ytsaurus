#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/client/object_client/public.h>

#define YT_LOG_ACCESS(...) \
    do { \
        if (IsAccessLogEnabled(Bootstrap_)) { \
            LogAccess(Bootstrap_, __VA_ARGS__); \
        } \
    } while (false)

#define YT_LOG_ACCESS_IF(predicate, ...) \
    if (predicate) { \
        YT_LOG_ACCESS(__VA_ARGS__); \
    }

//! Evaluates an expression the result of which is to be access-logged later.
//! Crucially, skips the evaluation if access logging is no-op.
#define YT_EVALUATE_FOR_ACCESS_LOG(...) \
    (IsAccessLogEnabled(Bootstrap_) \
        ? std::optional(__VA_ARGS__) \
        : std::nullopt)

#define YT_EVALUATE_FOR_ACCESS_LOG_IF(predicate, ...) \
    (predicate \
        ? YT_EVALUATE_FOR_ACCESS_LOG(__VA_ARGS__) \
        : std::nullopt)

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const NRpc::IServiceContextPtr& context,
    NCypressServer::TNodeId id,
    const std::optional<TString>& path,
    const NTransactionServer::TTransaction* transaction,
    const TAttributeVector& additionalAttributes = {},
    const std::optional<TStringBuf> methodOverride = std::nullopt);

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const NRpc::IServiceContextPtr& context,
    NCypressServer::TNodeId id,
    const TString& path,
    const NTransactionServer::TTransaction* transaction,
    const TAttributeVector& additionalAttributes = {},
    const std::optional<TStringBuf> methodOverride = std::nullopt);

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const NRpc::IServiceContextPtr& context,
    NCypressServer::TNodeId id,
    const TStringBuf path,
    const NTransactionServer::TTransaction* transaction,
    const TAttributeVector& additionalAttributes = {},
    const std::optional<TStringBuf> methodOverride = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    NCypressServer::TNodeId id,
    const TStringBuf path,
    const NTransactionServer::TTransaction* transaction,
    const TStringBuf method,
    const TAttributeVector& additionalAttributes = {});

////////////////////////////////////////////////////////////////////////////////

void LogAccess(
    NCellMaster::TBootstrap* bootstrap,
    const TStringBuf method,
    const NTransactionServer::TTransaction* transaction);

////////////////////////////////////////////////////////////////////////////////

bool IsAccessLoggedType(const NCypressClient::EObjectType type);

////////////////////////////////////////////////////////////////////////////////

bool IsAccessLogEnabled(NCellMaster::TBootstrap* bootstrap);

} // namespace NYT::NSecurityServer
