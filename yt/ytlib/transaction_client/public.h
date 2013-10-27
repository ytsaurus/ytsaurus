#pragma once

#include <core/misc/common.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager;
typedef TIntrusivePtr<TTransactionManager> TTransactionManagerPtr;

struct ITransaction;
typedef TIntrusivePtr<ITransaction> ITransactionPtr;

class TTransactionManagerConfig;
typedef TIntrusivePtr<TTransactionManagerConfig> TTransactionManagerConfigPtr;

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

typedef i64 TTimestamp;
const TTimestamp NullTimestamp = 0;
const TTimestamp LastCommittedTimestamp = -1;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
