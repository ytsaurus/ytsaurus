#pragma once

#include "../misc/common.h"
#include "../misc/enum.h"
#include "../logging/log.h"
#include "../transaction_manager/transaction_manager.h"
#include "../ytree/ytree.h"
#include "../ytree/ypath.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger CypressLogger;

////////////////////////////////////////////////////////////////////////////////

using NTransaction::TTransactionId;
using NTransaction::NullTransactionId;
using NTransaction::TTransaction;
using NTransaction::TTransactionManager;

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

//! Identifies a node.
typedef TGuid TNodeId;

//! Identifies a lock.
typedef TGuid TLockId;

DECLARE_ENUM(ELockMode,
    (SharedRead)
    (SharedWrite)
    (ExclusiveWrite)
);

extern TNodeId NullNodeId;
extern TNodeId RootNodeId;

////////////////////////////////////////////////////////////////////////////////

struct TCypressServiceConfig
{
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

