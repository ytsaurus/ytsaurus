#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionManagerConfig;
typedef TIntrusivePtr<TTransactionManagerConfig> TTransactionManagerConfigPtr;

class TTransactionManager;
typedef TIntrusivePtr<TTransactionManager> TTransactionManagerPtr;

struct ITransaction;
typedef TIntrusivePtr<ITransaction> ITransactionPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
