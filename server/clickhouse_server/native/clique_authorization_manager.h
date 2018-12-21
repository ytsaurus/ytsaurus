#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <string>

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

struct ICliqueAuthorizationManager
{
    virtual ~ICliqueAuthorizationManager() = default;

    //! Check if given user has access to a current clique.
    virtual bool HasAccess(const std::string& user) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICliqueAuthorizationManagerPtr CreateCliqueAuthorizationManager(
    NApi::IClientPtr client,
    TString cliqueId,
    bool validateOperationPermission);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
