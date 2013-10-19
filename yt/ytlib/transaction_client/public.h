#pragma once

#include <core/misc/common.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

class TTransactionManagerConfig;
typedef TIntrusivePtr<TTransactionManagerConfig> TTransactionManagerConfigPtr;

class TTransactionManager;
typedef TIntrusivePtr<TTransactionManager> TTransactionManagerPtr;

struct ITransaction;
typedef TIntrusivePtr<ITransaction> ITransactionPtr;

////////////////////////////////////////////////////////////////////////////////

typedef i64 TTimestamp;
const TTimestamp NullTimestamp = 0;
const TTimestamp LastCommittedTimestamp = -1;

// TODO(babenko): use this until Timestamp Oracle is implemented
inline TTimestamp GetCurrentTimestamp()
{
    return TInstant::Now().MicroSeconds();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
