#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

using NObjectServer::TTransactionId;

extern NLog::TLogger TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

