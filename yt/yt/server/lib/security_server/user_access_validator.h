#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Thread affinity: any.
struct IUserAccessValidator
    : public TRefCounted
{
    virtual void ValidateUser(const std::string& user) = 0;
    virtual void Reconfigure(const TUserAccessValidatorDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserAccessValidator)

////////////////////////////////////////////////////////////////////////////////

IUserAccessValidatorPtr CreateUserAccessValidator(
    TUserAccessValidatorDynamicConfigPtr config,
    NApi::IConnectionPtr connection,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
