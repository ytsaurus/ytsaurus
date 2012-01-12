#pragma once

#include "../logging/log.h"
#include <yt/ytlib/transaction_server/id.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger CypressLogger;

using NTransactionServer::TTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

