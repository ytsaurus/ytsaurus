#include "prewhere_block_input_stream.h"

#include "block_input_stream.h"
#include "host.h"
#include "query_context.h"
#include "read_plan.h"
#include "subquery_spec.h"

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

std::vector<TDataSliceDescriptor> GetFilteredDataSliceDescriptors(std::shared_ptr<TBlockInputStream> blockInputStream)
{
    std::vector<TDataSliceDescriptor> filteredDataSliceDescriptors;

    blockInputStream->readPrefixImpl();

    while (auto block = blockInputStream->read()) {
        if (block.rows() > 0) {
            filteredDataSliceDescriptors.emplace_back(blockInputStream->Reader()->GetCurrentReaderDescriptor());
            blockInputStream->Reader()->SkipCurrentReader();
        }
    }

    blockInputStream->readSuffixImpl();

    return filteredDataSliceDescriptors;
}

std::vector<TDataSliceDescriptor> FilterDataSliceDescriptorsByPrewhereInfo(
    std::vector<TDataSliceDescriptor>&& dataSliceDescriptors,
    const TReadPlanWithFilterPtr& readPlan,
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    const NTracing::TTraceContextPtr& traceContext,
    IGranuleFilterPtr granuleFilter,
    TCallback<void(const TStatistics&)> statisticsCallback)
{
    auto* queryContext = storageContext->QueryContext;

    auto prewhereReadPlan = ExtractPrewhereOnlyReadPlan(readPlan);

    auto Logger = queryContext->Logger;
    YT_LOG_DEBUG(
        "Started executing PREWHERE data slice filtering (PrewhereColumnCount: %v, TotalColumnCount: %v)",
        prewhereReadPlan->GetReadColumnCount(),
        readPlan->GetReadColumnCount());

    auto blockInputStream = CreateBlockInputStream(
        storageContext,
        subquerySpec,
        std::move(prewhereReadPlan),
        traceContext,
        dataSliceDescriptors,
        std::move(granuleFilter),
        std::move(statisticsCallback));

    return WaitFor(BIND(&GetFilteredDataSliceDescriptors, blockInputStream)
        .AsyncVia(queryContext->Host->GetClickHouseWorkerInvoker())
        .Run())
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TPrewhereBlockInputStream
    : public DB::IBlockInputStream
{
public:
    TPrewhereBlockInputStream(
        TStorageContext* storageContext,
        const TSubquerySpec& subquerySpec,
        TReadPlanWithFilterPtr readPlan,
        NTracing::TTraceContextPtr traceContext,
        std::vector<NChunkClient::TDataSliceDescriptor> dataSliceDescriptors,
        IGranuleFilterPtr granuleFilter,
        TCallback<void(const TStatistics&)> statisticsCallback)
        : StorageContext_(storageContext)
        , QueryContext_(storageContext->QueryContext)
        , SubquerySpec_(subquerySpec)
        , ReadPlan_(std::move(readPlan))
        , TraceContext_(traceContext)
        , Header_(CreateBlockInputStream(
            StorageContext_,
            SubquerySpec_,
            ReadPlan_,
            TraceContext_,
            /*dataSliceDescriptors*/ {},
            /*granuleFilter*/ nullptr,
            /*statisticsCallback*/ {})->getHeader())
        , GranuleFilter_(std::move(granuleFilter))
        , StatisticsCallback_(std::move(statisticsCallback))
        , DataSliceDescriptors_(std::move(dataSliceDescriptors))
        , Logger(QueryContext_->Logger)
    { }

    std::string getName() const override
    {
        return "PrewhereBlockInputStream";
    }

    DB::Block getHeader() const override
    {
        return Header_;
    }

    void readPrefixImpl() override
    {
        i64 totalDataWeight = 0;
        for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
            totalDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
        }

        DataSliceDescriptors_ = NDetail::FilterDataSliceDescriptorsByPrewhereInfo(
            std::move(DataSliceDescriptors_),
            ReadPlan_,
            StorageContext_,
            SubquerySpec_,
            TraceContext_,
            GranuleFilter_,
            StatisticsCallback_);

        i64 filteredDataWeight = 0;
        for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
            filteredDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
        }
        double droppedRate  = 1.0 - static_cast<double>(filteredDataWeight) / static_cast<double>(totalDataWeight);
        YT_LOG_DEBUG("PREWHERE filtration finished (DroppedRate: %v)", droppedRate);

        BlockInputStream_ = CreateBlockInputStream(
            StorageContext_,
            SubquerySpec_,
            ReadPlan_,
            TraceContext_,
            DataSliceDescriptors_,
            // Chunks have been already prefiltered.
            /*granuleFilter*/ nullptr,
            StatisticsCallback_);
        BlockInputStream_->readPrefixImpl();
    }

    void readSuffixImpl() override
    {
        BlockInputStream_->readSuffixImpl();
    }

private:
    TStorageContext* StorageContext_;
    TQueryContext* QueryContext_;
    const TSubquerySpec SubquerySpec_;
    TReadPlanWithFilterPtr ReadPlan_;
    NTracing::TTraceContextPtr TraceContext_;
    DB::Block Header_;
    const IGranuleFilterPtr GranuleFilter_;
    TCallback<void(const TStatistics&)> StatisticsCallback_;

    std::vector<NChunkClient::TDataSliceDescriptor> DataSliceDescriptors_;
    std::shared_ptr<TBlockInputStream> BlockInputStream_;

    TLogger Logger;

    Block readImpl() override
    {
        return BlockInputStream_->read();
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreatePrewhereBlockInputStream(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    TReadPlanWithFilterPtr readPlan,
    const NTracing::TTraceContextPtr& traceContext,
    std::vector<NChunkClient::TDataSliceDescriptor> dataSliceDescriptors,
    IGranuleFilterPtr granuleFilter,
    TCallback<void(const TStatistics&)> statisticsCallback)
{
    for (const auto& dataSource : subquerySpec.DataSourceDirectory->DataSources()) {
        if (dataSource.GetType() == EDataSourceType::VersionedTable) {
            THROW_ERROR_EXCEPTION("PREWHERE stage is not supported for dynamic tables (CHYT-462)");
        }
    }

    return std::make_shared<TPrewhereBlockInputStream>(
        storageContext,
        subquerySpec,
        std::move(readPlan),
        traceContext,
        std::move(dataSliceDescriptors),
        std::move(granuleFilter),
        std::move(statisticsCallback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
