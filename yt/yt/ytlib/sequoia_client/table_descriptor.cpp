#include "table_descriptor.h"

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>
#include <yt/yt/ytlib/sequoia_client/records/node_id_to_path.record.h>
#include <yt/yt/ytlib/sequoia_client/records/child_node.record.h>
#include <yt/yt/ytlib/sequoia_client/records/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/records/location_replicas.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NSequoiaClient {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

const ITableDescriptor* ITableDescriptor::Get(ESequoiaTable table)
{
    #define XX(type, tableName) \
        case ESequoiaTable::type: \
            class T##type##TableDescriptor \
                : public ITableDescriptor \
            { \
            public: \
                static const T##type##TableDescriptor* Get() \
                { \
                    return LeakySingleton<T##type##TableDescriptor>(); \
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
            return T##type##TableDescriptor::Get();

    switch (table) {
        XX(PathToNodeId, "path_to_node_id")
        XX(NodeIdToPath, "node_id_to_path")
        XX(ChunkReplicas, "chunk_replicas")
        XX(ChildNode, "child_node")
        XX(LocationReplicas, "location_replicas")
        default:
            YT_ABORT();
    }

    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetSequoiaTablePath(const NApi::NNative::IClientPtr& client, const ITableDescriptor* tableDescriptor)
{
    const auto& rootPath = client->GetNativeConnection()->GetConfig()->SequoiaConnection->SequoiaRootPath;
    return rootPath + "/" + NYPath::ToYPathLiteral(tableDescriptor->GetTableName());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
