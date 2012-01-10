#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>
#include <ytlib/logging/log.h>

#include <ytlib/transaction_client/common.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TransactionServerLogger;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

