#pragma once

#include <core/misc/common.h>

#include <ytlib/hydra/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectory;
typedef TIntrusivePtr<TCellDirectory> TCellDirectoryPtr;

struct ITimestampProvider;
typedef TIntrusivePtr<ITimestampProvider> ITimestampProviderPtr;

class TRemoteTimestampProviderConfig;
typedef TIntrusivePtr<TRemoteTimestampProviderConfig> TRemoteTimestampProviderConfigPtr;

////////////////////////////////////////////////////////////////////////////////

using NHydra::TCellGuid;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
