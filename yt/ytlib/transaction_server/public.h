#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager;
typedef TIntrusivePtr<TTransactionManager> TTransactionManagerPtr;

class TTransaction;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NTransactionServer
} // namespace NYT
