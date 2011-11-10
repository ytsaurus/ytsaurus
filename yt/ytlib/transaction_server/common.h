#pragma once

#include "../misc/common.h"
#include "../misc/guid.h"
#include "../logging/log.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TransactionLogger;

////////////////////////////////////////////////////////////////////////////////

//! Identifies a transaction.
typedef TGuid TTransactionId;

//! Means "no transaction".
extern TTransactionId NullTransactionId;
//! Denotes a special "system" transaction
/*!
 *  This is the only transaction that may alter the state without
 *  branching nodes, taking locks etc.
 *  
 *  Used by TWorldInitializer.
 */
extern TTransactionId SysTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT

