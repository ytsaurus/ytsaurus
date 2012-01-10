#pragma once

#include "common.h"

#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/cypress/node.h>

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
