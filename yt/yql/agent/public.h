#pragma once

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/guid.h>

namespace NYT::NYqlAgent {

struct IYqlAgent;
class TYqlAgentConfig;
class TYqlAgentDynamicConfig;
class TYqlAgentServerConfig;
class TYqlAgentServerDynamicConfig;
class TDynamicConfigManager;
class TVanillaJobFile;
class TDQYTBackend;
class TDQYTCoordinator;
class TDQManagerConfig;
struct TYqlRef;

DECLARE_REFCOUNTED_STRUCT(IYqlAgent)
DECLARE_REFCOUNTED_CLASS(TYqlAgentConfig)
DECLARE_REFCOUNTED_CLASS(TYqlAgentDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TYqlAgentServerConfig)
DECLARE_REFCOUNTED_CLASS(TYqlAgentServerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)
DECLARE_REFCOUNTED_CLASS(TVanillaJobFile)
DECLARE_REFCOUNTED_CLASS(TDQYTBackend)
DECLARE_REFCOUNTED_CLASS(TDQYTCoordinator)
DECLARE_REFCOUNTED_CLASS(TDQManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlRef);

////////////////////////////////////////////////////////////////////////////////

using TAgentId = TString;

using TQueryId = TGuid;

}
