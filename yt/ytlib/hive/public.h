#pragma once

#include <core/misc/common.h>

#include <ytlib/hydra/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCellDirectory)

DECLARE_REFCOUNTED_STRUCT(ITimestampProvider)

DECLARE_REFCOUNTED_CLASS(TCellDirectoryConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

using NHydra::TCellGuid;
using NHydra::NullCellGuid;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
