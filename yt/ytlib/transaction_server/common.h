#pragma once

#include "../misc/common.h"
#include "../misc/guid.h"
#include "../logging/log.h"

#include "../transaction_client/common.h"

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TransactionServerLogger;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

