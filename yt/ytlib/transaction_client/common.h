#pragma once

#include <yt/ytlib/logging/log.h>
#include <yt/ytlib/transaction_server/id.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TransactionClientLogger;

using NTransactionServer::TTransactionId;
using NTransactionServer::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
