#pragma once

#include <yt/core/misc/public.h>

#include <yt/client/hydra/public.h>

#include <yt/client/transaction_client/public.h>

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTimestampMap;
class TClusterDirectory;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;


using NHydra::TCellId;
using NHydra::NullCellId;

struct TTimestampMap;

DECLARE_REFCOUNTED_STRUCT(ITransactionParticipant)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
