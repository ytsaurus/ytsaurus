#include "stdafx.h"
#include "common.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger CypressLogger("Cypress");

TNodeId NullNodeId(0, 0, 0, 0);
TNodeId RootNodeId(0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

