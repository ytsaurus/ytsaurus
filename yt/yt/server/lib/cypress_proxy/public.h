#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBanServiceDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressProxyBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressProxyDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressProxyProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TObjectServiceDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TSequoiaResponseKeeperDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TTestConfig)
DECLARE_REFCOUNTED_STRUCT(TUserDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
