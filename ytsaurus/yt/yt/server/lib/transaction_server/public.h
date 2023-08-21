#pragma once

#include <yt/yt/server/lib/transaction_supervisor/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;
using NTransactionClient::TTransactionActionData;

using NTransactionSupervisor::ETransactionState;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
