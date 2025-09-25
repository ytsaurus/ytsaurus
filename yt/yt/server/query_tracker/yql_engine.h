#pragma once

#include "private.h"

#include "engine.h"

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

IQueryEnginePtr CreateYqlEngine(const NApi::IClientPtr& stateClient, const NYPath::TYPath& stateRoot);

////////////////////////////////////////////////////////////////////////////////

IProxyEngineProviderPtr CreateProxyYqlEngineProvider(const NApi::IClientPtr& stateClient, const NYPath::TYPath& stateRoot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
