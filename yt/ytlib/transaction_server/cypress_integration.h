#pragma once

#include "common.h"
#include "transaction_manager.h"

#include "../cypress/cypress_manager.h"
#include "../cypress/node.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandler::TPtr CreateTransactionMapTypeHandler(
    NCypress::TCypressManager* cypressManager,
    TTransactionManager* transactionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
