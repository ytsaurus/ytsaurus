#pragma once

#include "../misc/common.h"
#include "../misc/new.h"
#include "../misc/intrusive_ptr.h"
#include "../misc/ref_counted_base.h"
#include "../misc/guid.h"

#include "../logging/log.h"
#include "../actions/action.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TransactionClientLogger;

//! Identifies a transaction.
typedef TGuid TTransactionId;

//! Means "no transaction".
extern TTransactionId NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
