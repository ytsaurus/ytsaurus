#pragma once

#include "common.h"

#include "../cypress/node.h"
#include "../cypress/cypress_manager.h"

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateOrchidTypeHandler(
    NCypress::TCypressManager* cypressManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
