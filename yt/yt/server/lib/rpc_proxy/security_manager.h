#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

//! Thread affinity: any.
struct ISecurityManager
    : public TRefCounted
{
    virtual void ValidateUser(const TString& user) = 0;
    virtual void Reconfigure(const TSecurityManagerDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISecurityManager)

////////////////////////////////////////////////////////////////////////////////

ISecurityManagerPtr CreateSecurityManager(
    TSecurityManagerDynamicConfigPtr config,
    NApi::IConnectionPtr connection,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
