#pragma once

#include <core/misc/common.h>

#include <ytlib/hydra/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

struct ITimestampProvider;
typedef TIntrusivePtr<ITimestampProvider> ITimestampProviderPtr;

class TRemoteTimestampProviderConfig;
typedef TIntrusivePtr<TRemoteTimestampProviderConfig> TRemoteTimestampProviderConfigPtr;

////////////////////////////////////////////////////////////////////////////////

using NHydra::TCellGuid;

using NVersionedTableClient::TTimestamp;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
