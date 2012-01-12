#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/transaction_server/id.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger CypressLogger;

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

