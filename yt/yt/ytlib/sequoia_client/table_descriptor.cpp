#include "table_descriptor.h"

#include <yt/yt/ytlib/sequoia_client/chunk_meta_extensions.record.h>
#include <yt/yt/ytlib/sequoia_client/resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/chunk_replicas.record.h>
#include <yt/yt/ytlib/sequoia_client/location_replicas.record.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

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
        XX(ChunkMetaExtensions, "chunk_meta_extensions")
        XX(ResolveNode, "resolve_node")
        XX(ChunkReplicas, "chunk_replicas")
        XX(LocationReplicas, "location_replicas")
        default:
            YT_ABORT();
    }

    #undef XX
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
