#include "private.h"

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger CellarAgentLogger("CellarAgent");
const NProfiling::TProfiler CellarAgentProfiler("/cellar_agent");

const TString TabletCellCypressPrefix("//sys/tablet_cells");
const TString ChaosCellCypressPrefix("//sys/chaos_cells");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
