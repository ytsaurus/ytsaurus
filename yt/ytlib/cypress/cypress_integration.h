#pragma once

#include "common.h"

#include "../cypress/cypress_manager.h"
#include "../cypress/node.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateNodeMapTypeHandler(
    NCypress::TCypressManager* cypressManager);

NCypress::INodeTypeHandler::TPtr CreateLockMapTypeHandler(
    NCypress::TCypressManager* cypressManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
