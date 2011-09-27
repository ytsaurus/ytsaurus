#pragma once

#include "../misc/common.h"
#include "../misc/enum.h"
#include "../logging/log.h"
#include "../transaction/transaction_manager.h"

namespace NYT {
namespace NRegistry {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger RegistryLogger;

////////////////////////////////////////////////////////////////////////////////

using NTransaction::TTransactionId;
using NTransaction::TTransaction;
using NTransaction::TTransactionManager;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ELockMode,
    (SharedRead)
    (SharedWrite)
    (ExclusiveWrite)
);

struct TRegistryServiceConfig
{
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRegistry
} // namespace NYT

