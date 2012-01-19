#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/transaction_server/id.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TransactionClientLogger;

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
