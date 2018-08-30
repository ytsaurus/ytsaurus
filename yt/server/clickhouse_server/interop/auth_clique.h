#pragma once

#include <string>

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

class ICliqueAuthorizationManager
{
public:
    virtual ~ICliqueAuthorizationManager() = default;

    //! Check if given user has access to a current clique.
    virtual bool HasAccess(const std::string& user) = 0;
};

using ICliqueAuthorizationManagerPtr = std::shared_ptr<ICliqueAuthorizationManager>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NInterop
