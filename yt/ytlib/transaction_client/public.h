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

typedef ui64 TTimestamp;

// Uninitialized/invalid timestamp.
const TTimestamp NullTimestamp = 0;

// Valid timestamps.
const TTimestamp MinTimestamp = 1;
const TTimestamp MaxTimestamp = 0x3fffffffffffffffULL;

// Sentinels.
const TTimestamp LastCommittedTimestamp = 0x4000000000000000ULL;
const TTimestamp UncommittedTimestamp   = 0x4000000000000001ULL;

// Masks.
const TTimestamp TimestampValueMask     = 0x7fffffffffffffffULL;
const TTimestamp TombstoneTimestampMask = 0x8000000000000000ULL;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
