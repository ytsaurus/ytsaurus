#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/guid.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

struct IYqlAgent;
class TDynamicConfigManager;
class TVanillaJobFile;
class TDQYTBackend;
class TDQYTCoordinator;
struct TYqlRef;

DECLARE_REFCOUNTED_STRUCT(IYqlAgent)
DECLARE_REFCOUNTED_STRUCT(TYqlAgentConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlAgentDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlAgentServerConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlAgentServerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)
DECLARE_REFCOUNTED_CLASS(TVanillaJobFile)
DECLARE_REFCOUNTED_CLASS(TDQYTBackend)
DECLARE_REFCOUNTED_CLASS(TDQYTCoordinator)
DECLARE_REFCOUNTED_STRUCT(TDQManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlRef);

////////////////////////////////////////////////////////////////////////////////

using TAgentId = TString;
using TQueryId = TGuid;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
