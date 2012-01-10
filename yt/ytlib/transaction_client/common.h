#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/new.h>
#include <ytlib/misc/intrusive_ptr.h>
#include <ytlib/misc/ref_counted_base.h>
#include <ytlib/misc/guid.h>

#include <ytlib/logging/log.h>
#include <ytlib/actions/action.h>

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
