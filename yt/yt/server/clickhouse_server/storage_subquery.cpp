#include "storage_subquery.h"

#include "block_input_stream.h"
#include "prewhere_block_input_stream.h"
#include "query_context.h"
#include "subquery_spec.h"
#include "query_registry.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TStorageSubquery
    : public DB::IStorage
{
public:
    TStorageSubquery(TQueryContext* queryContext, TSubquerySpec subquerySpec)
        : DB::IStorage({"subquery_db", "subquery"})
        , QueryContext_(queryContext)
        , SubquerySpec_(std::move(subquerySpec))
    {
        if (SubquerySpec_.InitialQueryId != queryContext->QueryId) {
            queryContext->Logger.AddTag("InitialQueryId: %v", SubquerySpec_.InitialQueryId);
            if (queryContext->InitialQuery) {
                YT_VERIFY(*queryContext->InitialQuery == SubquerySpec_.InitialQuery);
            } else {
                queryContext->InitialQuery = SubquerySpec_.InitialQuery;
            }
        }
        Logger = queryContext->Logger;
        Logger.AddTag(
            "SubqueryIndex: %v, SubqueryTableIndex: %v",
            SubquerySpec_.SubqueryIndex,
            SubquerySpec_.TableIndex);

        setColumns(DB::ColumnsDescription(ToNamesAndTypesList(*SubquerySpec_.ReadSchema)));

        queryContext->MoveToPhase(EQueryPhase::Preparation);
    }

    std::string getName() const override
    {
        return "YT";
    }

    bool supportsPrewhere() const override
    {
        return true;
    }

    bool isRemote() const override
    {
        // NB: from CH point of view this is already a non-remote query.
        // If we return here, GLOBAL IN stops working: CHYT-117.
        return false;
    }

    DB::Pipes read(
        const DB::Names& columnNames,
        const DB::SelectQueryInfo& queryInfo,
        const DB::Context& context,
        DB::QueryProcessingStage::Enum /* processedStage */,
        size_t /* maxBlockSize */,
        unsigned maxStreamCount) override
    {
        QueryContext_->MoveToPhase(EQueryPhase::Execution);

        const auto& traceContext = GetQueryContext(context)->TraceContext;

        auto perThreadDataSliceDescriptors = SubquerySpec_.DataSliceDescriptors;

        i64 totalRowCount = 0;
        i64 totalDataWeight = 0;
        i64 totalDataSliceCount = 0;
        for (const auto& threadDataSliceDescriptors : perThreadDataSliceDescriptors) {
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                totalRowCount += dataSliceDescriptor.ChunkSpecs[0].row_count_override();
                totalDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
                ++totalDataSliceCount;
            }
        }
        YT_LOG_DEBUG(
            "Deserialized subquery spec (RowCount: %v, DataWeight: %v, DataSliceCount: %v)",
            totalRowCount,
            totalDataWeight,
            totalDataSliceCount);


        auto tableIndexSuffix = Format(".%v", SubquerySpec_.TableIndex);
        traceContext->AddTag("chyt.row_count" + tableIndexSuffix, ToString(totalRowCount));
        traceContext->AddTag("chyt.data_weight" + tableIndexSuffix, ToString(totalDataWeight));
        traceContext->AddTag("chyt.data_slice_count" + tableIndexSuffix, ToString(totalDataSliceCount));
        if (SubquerySpec_.TableIndex == 0) {
            traceContext->AddTag("chyt.subquery_index", ToString(SubquerySpec_.SubqueryIndex));
        }

        YT_LOG_DEBUG(
            "Creating table readers (MaxStreamCount: %v, StripeCount: %v, Columns: %v)",
            maxStreamCount,
            SubquerySpec_.DataSliceDescriptors.size(),
            columnNames);

        auto schema = SubquerySpec_.ReadSchema;

        YT_LOG_INFO("Creating table readers");
        DB::Pipes pipes;
        const auto& prewhereInfo = queryInfo.prewhere_info;

        for (int threadIndex = 0;
            threadIndex < static_cast<int>(perThreadDataSliceDescriptors.size());
            ++threadIndex)
        {
            const auto& threadDataSliceDescriptors = perThreadDataSliceDescriptors[threadIndex];

            if (prewhereInfo) {
                pipes.emplace_back(std::make_shared<DB::SourceFromInputStream>(CreatePrewhereBlockInputStream(
                    QueryContext_,
                    SubquerySpec_,
                    columnNames,
                    traceContext,
                    threadDataSliceDescriptors,
                    prewhereInfo)));
            } else {
                pipes.emplace_back(std::make_shared<DB::SourceFromInputStream>(CreateBlockInputStream(
                    QueryContext_,
                    SubquerySpec_,
                    columnNames,
                    traceContext,
                    threadDataSliceDescriptors,
                    prewhereInfo)));
            }

            i64 rowCount = 0;
            i64 dataWeight = 0;
            int dataSliceCount = 0;
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                rowCount += dataSliceDescriptor.ChunkSpecs[0].row_count_override();
                dataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
                ++dataSliceCount;
                for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
                    // It is crucial for better memory estimation.
                    YT_VERIFY(FindProtoExtension<NChunkClient::NProto::TMiscExt>(
                        chunkSpec.chunk_meta().extensions()));
                }
            }
            YT_LOG_DEBUG(
                "Thread table reader stream created (ThreadIndex: %v, RowCount: %v, DataWeight: %v, DataSliceCount: %v)",
                threadIndex,
                rowCount,
                dataWeight,
                dataSliceCount);

            TStringBuilder debugString;
            for (const auto& dataSliceDescriptor : threadDataSliceDescriptors) {
                debugString.AppendString(ToString(dataSliceDescriptor));
                debugString.AppendString("\n");
            }

            YT_LOG_DEBUG(
                "Thread debug string (ThreadIndex: %v, DebugString: %v)",
                threadIndex,
                debugString.Flush());
        }

        return pipes;
    }

    virtual DB::QueryProcessingStage::Enum getQueryProcessingStage(
        const DB::Context& /* context */,
        DB::QueryProcessingStage::Enum /* toStage */,
        const DB::ASTPtr &) const override
    {
        return DB::QueryProcessingStage::Enum::FetchColumns;
    }

private:
    TQueryContext* QueryContext_;
    TSubquerySpec SubquerySpec_;
    TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSubquery(TQueryContext* queryContext, TSubquerySpec subquerySpec)
{
    return std::make_shared<TStorageSubquery>(queryContext, std::move(subquerySpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
