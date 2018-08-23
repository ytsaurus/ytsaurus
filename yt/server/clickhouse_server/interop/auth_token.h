#pragma once

#include <util/generic/map.h>

#include <memory>

namespace NInterop {

using TStringMap = TMap<TString, TString>;

////////////////////////////////////////////////////////////////////////////////

class IAuthorizationToken
{
public:
    virtual ~IAuthorizationToken() = default;

    /// Opaque token used to represent authenticated client session
};

using IAuthorizationTokenPtr = std::shared_ptr<IAuthorizationToken>;

////////////////////////////////////////////////////////////////////////////////

class IAuthorizationTokenService
{
public:
    virtual ~IAuthorizationTokenService() = default;

    virtual IAuthorizationTokenPtr CreateToken(const TStringMap& attrs) = 0;
};

}   // namespace NInterop
