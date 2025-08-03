#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Thread affinity: any.
struct IUserAccessValidator
    : public TRefCounted
{
    virtual void ValidateUser(const std::string& user, const std::optional<std::string>& cluster = {}) = 0;
    virtual void Reconfigure(const TUserAccessValidatorDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserAccessValidator)

////////////////////////////////////////////////////////////////////////////////

IUserAccessValidatorPtr CreateUserAccessValidator(
    TUserAccessValidatorDynamicConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
