#include "table_descriptor.h"

#include <yt/yt/ytlib/sequoia_client/resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/reverse_resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/children_nodes.record.h>
#include <yt/yt/ytlib/sequoia_client/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/location_replicas.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NSequoiaClient {

using namespace NQueryClient;
using namespace NYPath;

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
        XX(ResolveNode, "resolve_node")
        XX(ChunkReplicas, "chunk_replicas")
        XX(ChildrenNodes, "children_nodes")
        XX(LocationReplicas, "location_replicas")
        XX(ReverseResolveNode, "reverse_resolve_node")
        default:
            YT_ABORT();
    }

    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

TYPath GetSequoiaTablePath(const NApi::NNative::IClientPtr& client, const ITableDescriptor* tableDescriptor)
{
    const auto& sequoiaPath = client->GetNativeConnection()->GetConfig()->SequoiaConnection->SequoiaPath;
    return sequoiaPath + "/" + ToYPathLiteral(tableDescriptor->GetTableName());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
