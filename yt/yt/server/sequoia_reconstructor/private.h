#pragma once

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/leaky_global.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, SequoiaReconstructorLogger, "SequoiaReconstructor");

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOutputTableType,
    (NodeIdToPath)
    (NodeForks)
    (NodeSnapshots)
    (ChildNodes)
    (ChildForks)
    (Acls)
    (Transactions)
    (TransactionDescendants)
    (DependentTransactions)
    (TransactionReplicas)
    (PathToNodeId)
    (PathForks)
    (PathToNodeChanges)
);

////////////////////////////////////////////////////////////////////////////////

struct TPathToNodeChangeRecord;

struct TRecordsConsumer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
