#pragma once

#include "common.h"
#include "transaction_manager.h"

#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/cypress/node.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateTransactionMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TTransactionManager* transactionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
