#pragma once

#include "private.h"

#include "engine.h"

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

class TDeclaredParametersInfoProvider
{
public:
    TDeclaredParametersInfoProvider(NApi::IClientPtr stateClient, NYPath::TYPath stateRoot);
    NYson::TYsonString GetDeclaredParametersInfo(const std::string& query, const NYson::TYsonString& settings);

private:
    const NApi::IClientPtr StateClient_;
    const NYPath::TYPath StateRoot_;
};

////////////////////////////////////////////////////////////////////////////////

template <bool IsActive>
NYson::TYsonString GetYqlProgressFromParts(
    TQueryId queryId,
    const NApi::IClientPtr& client,
    const NYPath::TYPath& root,
    NTableClient::TTimestamp timestamp,
    const NYTree::IMapNodePtr& progressOptions);

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateYqlEngine(
    const NApi::IClientPtr& stateClient,
    const NYPath::TYPath& stateRoot,
    std::optional<std::string> adminsGroup);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
