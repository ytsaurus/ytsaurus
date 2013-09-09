#pragma once

#include <core/misc/common.h>
#include <core/misc/small_vector.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager;
typedef TIntrusivePtr<TTransactionManager> TTransactionManagerPtr;

class TTransactionManagerConfig;
typedef TIntrusivePtr<TTransactionManagerConfig> TTransactionManagerConfigPtr;

class TTransaction;
typedef TSmallVector<TTransaction*, 4> TTransactionPath;

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
