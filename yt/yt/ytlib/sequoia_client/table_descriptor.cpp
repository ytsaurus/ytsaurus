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

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

const ITableDescriptor* ITableDescriptor::Get(ESequoiaTable table)
{
    #define XX(type, tableName, TableName) \
        case ESequoiaTable::TableName: \
            class T##TableName##TableDescriptor \
                : public ITableDescriptor \
            { \
            public: \
                \
                static const NRecords::T##type::TRecordDescriptor* GetActualRecordDescriptor() \
                { \
                    return NRecords::T##type::TRecordDescriptor::Get(); \
                } \
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
                bool IsSorted() const override \
                { \
                    return GetPrimarySchema()->IsSorted(); \
                } \
                \
                const NTableClient::IRecordDescriptor* GetRecordDescriptor() const override \
                { \
                    return GetActualRecordDescriptor(); \
                } \
                \
                const NQueryClient::TColumnEvaluatorPtr& GetColumnEvaluator() const override \
                { \
                    return ColumnEvaluator_; \
                } \
                \
                const TTableSchemaPtr& GetPrimarySchema() const override \
                { \
                    return GetActualRecordDescriptor()->GetPrimaryTableSchema(); \
                } \
                \
                const TTableSchemaPtr& GetWriteSchema() const override \
                { \
                    return WriteSchema_; \
                } \
                \
                const TTableSchemaPtr& GetDeleteSchema() const override \
                { \
                    return DeleteSchema_; \
                } \
                \
                const TTableSchemaPtr& GetLockSchema() const override \
                { \
                    return LockSchema_; \
                } \
                \
                const TNameTableToSchemaIdMapping& GetNameTableToPrimarySchemaIdMapping() const override \
                { \
                    return NameTableToPrimarySchemaIdMapping_; \
                } \
                \
                const TNameTableToSchemaIdMapping& GetNameTableToWriteSchemaIdMapping() const override \
                { \
                    return NameTableToWriteSchemaIdMapping_; \
                } \
                \
                const TNameTableToSchemaIdMapping& GetNameTableToDeleteSchemaIdMapping() const override \
                { \
                    return NameTableToDeleteSchemaIdMapping_; \
                } \
                \
                const TNameTableToSchemaIdMapping& GetNameTableToLockSchemaIdMapping() const override \
                { \
                    return NameTableToLockSchemaIdMapping_; \
                } \
                \
            private: \
                const TColumnEvaluatorPtr ColumnEvaluator_ = TColumnEvaluator::Create( \
                    GetActualRecordDescriptor()->GetPrimaryTableSchema(), \
                    GetBuiltinTypeInferrers(), \
                    GetBuiltinFunctionProfilers()); \
                const TTableSchemaPtr WriteSchema_ = GetActualRecordDescriptor()->GetPrimaryTableSchema()->ToWrite(); \
                const TTableSchemaPtr DeleteSchema_ = GetActualRecordDescriptor()->GetPrimaryTableSchema()->ToDelete(); \
                const TTableSchemaPtr LockSchema_ = GetActualRecordDescriptor()->GetPrimaryTableSchema()->ToLock(); \
                const TNameTableToSchemaIdMapping NameTableToPrimarySchemaIdMapping_ = BuildColumnIdMapping( \
                    *GetActualRecordDescriptor()->GetPrimaryTableSchema(), \
                    GetActualRecordDescriptor()->GetNameTable()); \
                const TNameTableToSchemaIdMapping NameTableToWriteSchemaIdMapping_ = BuildColumnIdMapping( \
                    *WriteSchema_, \
                    GetActualRecordDescriptor()->GetNameTable()); \
                const TNameTableToSchemaIdMapping NameTableToDeleteSchemaIdMapping_ = BuildColumnIdMapping( \
                    *DeleteSchema_, \
                    GetActualRecordDescriptor()->GetNameTable()); \
                const TNameTableToSchemaIdMapping NameTableToLockSchemaIdMapping_ = BuildColumnIdMapping( \
                    *LockSchema_, \
                    GetActualRecordDescriptor()->GetNameTable()); \
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

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NSequoiaClient::TSequoiaTablePathDescriptor>::operator()(
    const NYT::NSequoiaClient::TSequoiaTablePathDescriptor& descriptor) const
{
    return MultiHash(descriptor.Table, descriptor.MasterCellTag);
}

////////////////////////////////////////////////////////////////////////////////
