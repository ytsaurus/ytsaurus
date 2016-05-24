#pragma once

#include <yt/server/hive/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/public.h>
#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTransactionManager)
DECLARE_REFCOUNTED_CLASS(TTimestampManager)

DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTimestampManagerConfig)

class TTransaction;

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

using NHive::ETransactionState;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
