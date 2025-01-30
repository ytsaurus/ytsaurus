#include "table_descriptor.h"

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/location_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_descendants.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/dependent_transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/unapproved_chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/path_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_snapshots.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_forks.record.h>
#include <yt/yt/ytlib/sequoia_client/records/response_keeper.record.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_refresh_queue.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NSequoiaClient {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TSequoiaTablePathDescriptor::operator size_t() const
{
    return MultiHash(
        Table,
        MasterCellTag);
}

////////////////////////////////////////////////////////////////////////////////

const ITableDescriptor* ITableDescriptor::Get(ESequoiaTable table)
{
    #define XX(type, tableName, TableName) \
        case ESequoiaTable::TableName: \
            class T##TableName##TableDescriptor \
                : public ITableDescriptor \
            { \
            public: \
                static const T##TableName##TableDescriptor* Get() \
                { \
                    return LeakySingleton<T##TableName##TableDescriptor>(); \
                } \
                \
                const TString& GetTableName() const override \
                { \
                    static const TString Result(tableName); \
                    return Result; \
                } \
                \
                const NTableClient::IRecordDescriptor* GetRecordDescriptor() const override \
                { \
                    return NRecords::T##type::TRecordDescriptor::Get(); \
                } \
                \
                const NQueryClient::TColumnEvaluatorPtr& GetColumnEvaluator() const override \
                { \
                    return ColumnEvaluator_; \
                } \
                \
            private: \
                const TColumnEvaluatorPtr ColumnEvaluator_ = TColumnEvaluator::Create( \
                    GetRecordDescriptor()->GetSchema(), \
                    /*typeInferrers*/ nullptr, \
                    /*profilers*/ nullptr); \
            }; \
            \
            return T##TableName##TableDescriptor::Get();

    switch (table) {
        XX(PathToNodeId, "path_to_node_id", PathToNodeId)
        XX(NodeIdToPath, "node_id_to_path", NodeIdToPath)
        XX(ChunkReplicas, "chunk_replicas", ChunkReplicas)
        XX(ChildNode, "child_node", ChildNode)
        XX(LocationReplicas, "location_replicas", LocationReplicas)
        XX(Transaction, "transactions", Transactions)
        XX(TransactionDescendant, "transaction_descendants", TransactionDescendants)
        XX(TransactionReplica, "transaction_replicas", TransactionReplicas)
        XX(DependentTransaction, "dependent_transactions", DependentTransactions)
        XX(UnapprovedChunkReplicas, "unapproved_chunk_replicas", UnapprovedChunkReplicas)
        XX(NodeFork, "node_forks", NodeForks)
        XX(PathFork, "path_forks", PathForks)
        XX(NodeSnapshot, "node_snapshots", NodeSnapshots)
        XX(ChildFork, "child_forks", ChildForks)
        XX(SequoiaResponseKeeper, "response_keeper", ResponseKeeper)
        XX(ChunkRefreshQueue, "chunk_refresh_queue", ChunkRefreshQueue)
        default:
            YT_ABORT();
    }

    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetSequoiaTablePath(
    const NApi::NNative::IClientPtr& client,
    const TSequoiaTablePathDescriptor& tablePathDescriptor)
{
    const auto& rootPath = client->GetNativeConnection()->GetConfig()->SequoiaConnection->SequoiaRootPath;
    const auto* tableDescriptor = ITableDescriptor::Get(tablePathDescriptor.Table);
    auto path = rootPath + "/" + NYPath::ToYPathLiteral(tableDescriptor->GetTableName());
    if (tablePathDescriptor.MasterCellTag) {
        path += "_" + ToString(tablePathDescriptor.MasterCellTag);
    }
    return path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
