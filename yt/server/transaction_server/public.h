#pragma once

#include <core/misc/public.h>
#include <core/misc/small_vector.h>

#include <ytlib/object_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <server/hive/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTransactionManager)
DECLARE_REFCOUNTED_CLASS(TTimestampManager)

DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTimestampManagerConfig)

class TTransaction;
typedef SmallVector<TTransaction*, 4> TTransactionPath;

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

using NHive::ETransactionState;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
