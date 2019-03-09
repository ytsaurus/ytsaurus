#pragma once

#include "private.h"

#include <yt/ytlib/api/native/client.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct ICliqueAuthorizationManager
{
    virtual ~ICliqueAuthorizationManager() = default;

    //! Check if given user has access to a current clique.
    virtual bool HasAccess(const std::string& user) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICliqueAuthorizationManagerPtr CreateCliqueAuthorizationManager(
    NApi::NNative::IClientPtr client,
    TString cliqueId,
    bool validateOperationPermission);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
