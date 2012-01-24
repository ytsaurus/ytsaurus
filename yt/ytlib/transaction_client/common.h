#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/object_server/id.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TransactionClientLogger;

using NObjectServer::TTransactionId;
using NObjectServer::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
