#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Abstracts the way of filling the "transaction_info" section of an access log record.
struct ITransactionAccessLogInfo
{
    virtual ~ITransactionAccessLogInfo() = default;

    virtual void WriteTransactionInfo(NYson::IYsonConsumer* consumer) const = 0;

    virtual NObjectClient::TTransactionId GetTransactionId() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

void LogAccess(
    const NRpc::IServiceContextPtr& context,
    NObjectClient::TObjectId id,
    NYPath::TYPathBuf path,
    const ITransactionAccessLogInfo& transactionInfo,
    NRpc::TMutationId mutationId = {},
    const TAccessLogAttributes& additionalAttributes = {},
    std::optional<std::string> methodOverride = {});

void LogAccess(
    NObjectClient::TObjectId id,
    NYPath::TYPathBuf path,
    const std::string& method,
    const ITransactionAccessLogInfo& transactionInfo,
    NRpc::TMutationId mutationId = {},
    const TAccessLogAttributes& additionalAttributes = {});

void LogAccess(
    const std::string& method,
    const ITransactionAccessLogInfo& transactionInfo);

////////////////////////////////////////////////////////////////////////////////

bool IsAccessLoggedType(NObjectClient::EObjectType type);

bool IsAccessLoggedMethod(TStringBuf method);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
