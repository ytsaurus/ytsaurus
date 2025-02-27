#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TNativeAuthenticationManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TNativeAuthenticationManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TTvmBridgeConfig)

DECLARE_REFCOUNTED_STRUCT(ITvmBridge)

YT_DECLARE_RECONFIGURABLE_SINGLETON(TNativeAuthenticationManagerConfig, TNativeAuthenticationManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
