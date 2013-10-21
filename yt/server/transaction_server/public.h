#pragma once

#include <core/misc/common.h>
#include <core/misc/small_vector.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager;
typedef TIntrusivePtr<TTransactionManager> TTransactionManagerPtr;

class TTimestampManager;
typedef TIntrusivePtr<TTimestampManager> TTimestampManagerPtr;

class TTransaction;
typedef TSmallVector<TTransaction*, 4> TTransactionPath;

class TTimestampManagerConfig;
typedef TIntrusivePtr<TTimestampManagerConfig> TTimestampManagerConfigPtr;

class TTransactionManagerConfig;
typedef TIntrusivePtr<TTransactionManagerConfig> TTransactionManagerConfigPtr;

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
