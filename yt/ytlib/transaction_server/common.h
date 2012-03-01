#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

using NObjectServer::TTransactionId;
using NObjectServer::NullTransactionId;

extern NLog::TLogger TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

