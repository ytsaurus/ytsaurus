#pragma once

#include "helpers.h"
#include "config.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

template <class TRecord>
struct IRecordConsumer
{
    virtual ~IRecordConsumer() = default;

    virtual void Consume(const TRecord& record) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRecordsConsumer
{
    TRecordsConsumer(TSequoiaReconstructorConfigPtr config);

    // The records up to TransactionReplicas are written only in the map stage.
    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TNodeIdToPath>> NodeIdToPath;
    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TNodeFork>> NodeForks;
    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TNodeSnapshot>> NodeSnapshots;

    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TChildNode>> ChildNode;
    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TChildFork>> ChildForks;

    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TAcls>> Acls;

    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TTransaction>> Transactions;
    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TTransactionDescendant>> TransactionDescendants;
    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TDependentTransaction>> DependentTransactions;
    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TTransactionReplica>> TransactionReplicas;

    // PathToNodeId records are written both in map and reduce stages.
    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TPathToNodeId>> PathToNodeId;
    // PathForks are written only in reduce stage.
    std::unique_ptr<IRecordConsumer<NSequoiaClient::NRecords::TPathFork>> PathForks;

    // PathToNodeChanges are written in the map stage and are used for reduce stage.
    std::unique_ptr<IRecordConsumer<TPathToNodeChangeRecord>> PathToNodeChanges;
    // vector of PathToNodeChangeRecords is used for single cell single run, allowing to avoid additional reduce stage.
    std::vector<TPathToNodeChangeRecord> PathToNodeChangeRecords;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
