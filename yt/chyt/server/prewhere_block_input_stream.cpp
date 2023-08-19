#include "prewhere_block_input_stream.h"

#include "subquery_spec.h"
#include "host.h"
#include "block_input_stream.h"
#include "query_context.h"

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

Names ExtractColumnsFromPrewhereInfo(PrewhereInfoPtr prewhereInfo)
{
    // TODO(dakovalkov): prewhereInfo also contains row_level_filter. Explore what it is.
    // Probable we need to use both of them (see IMergeTreeSelectAlgorithm::getPrewhereActions).
    return prewhereInfo->prewhere_actions->getRequiredColumnsNames();
}

std::vector<TDataSliceDescriptor> GetFilteredDataSliceDescriptors(std::shared_ptr<TBlockInputStream> blockInputStream)
{
    std::vector<TDataSliceDescriptor> filteredDataSliceDescriptors;
    while (auto block = blockInputStream->read()) {
        if (block.rows() > 0) {
            filteredDataSliceDescriptors.emplace_back(blockInputStream->Reader()->GetCurrentReaderDescriptor());
            blockInputStream->Reader()->SkipCurrentReader();
        }
    }
    return filteredDataSliceDescriptors;
}

std::vector<TDataSliceDescriptor> FilterDataSliceDescriptorsByPrewhereInfo(
    std::vector<TDataSliceDescriptor>&& dataSliceDescriptors,
    PrewhereInfoPtr prewhereInfo,
    const std::vector<TString>& virtualColumnNames,
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    const NTracing::TTraceContextPtr& traceContext,
    IGranuleFilterPtr granuleFilter)
{
    auto prewhereColumns = ExtractColumnsFromPrewhereInfo(prewhereInfo);
    auto* queryContext = storageContext->QueryContext;

    std::vector<TString> realPrewhereColumns;
    std::vector<TString> virtualPrewhereColumns;

    for (const auto& column : prewhereColumns) {
        if (std::find(virtualColumnNames.begin(), virtualColumnNames.end(), column) != virtualColumnNames.end()) {
            virtualPrewhereColumns.emplace_back(column);
        } else {
            realPrewhereColumns.emplace_back(column);
        }
    }

    auto Logger = queryContext->Logger;
    YT_LOG_DEBUG(
        "Started executing PREWHERE data slice filtering (PrewhereColumnName: %v, RealPrewhereColumns: %v, VirtualPrewhereColumns: %v)",
        prewhereInfo->prewhere_column_name,
        realPrewhereColumns,
        virtualPrewhereColumns);

    auto blockInputStream = CreateBlockInputStream(
        storageContext,
        subquerySpec,
        realPrewhereColumns,
        virtualPrewhereColumns,
        traceContext,
        dataSliceDescriptors,
        std::move(prewhereInfo),
        std::move(granuleFilter));

    return WaitFor(BIND(&GetFilteredDataSliceDescriptors, blockInputStream)
        // TODO(max42): use clickhouse worker invoker?
        .AsyncVia(queryContext->Host->GetWorkerInvoker())
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
        const std::vector<TString>& realColumns,
        const std::vector<TString>& virtualColumns,
        NTracing::TTraceContextPtr traceContext,
        DB::PrewhereInfoPtr prewhereInfo,
        std::vector<NChunkClient::TDataSliceDescriptor> dataSliceDescriptors,
        IGranuleFilterPtr granuleFilter)
        : StorageContext_(storageContext)
        , QueryContext_(storageContext->QueryContext)
        , SubquerySpec_(subquerySpec)
        , RealColumnNames_(realColumns)
        , VirtualColumnNames_(virtualColumns)
        , TraceContext_(traceContext)
        , PrewhereInfo_(std::move(prewhereInfo))
        , Header_(CreateBlockInputStream(
            StorageContext_,
            SubquerySpec_,
            realColumns,
            virtualColumns,
            TraceContext_,
            /*dataSliceDescriptors*/ {},
            PrewhereInfo_,
            /*granuleFilter*/ nullptr)->getHeader())
        , GranuleFilter_(std::move(granuleFilter))
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
            PrewhereInfo_,
            VirtualColumnNames_,
            StorageContext_,
            SubquerySpec_,
            TraceContext_,
            GranuleFilter_);

        i64 filteredDataWeight = 0;
        for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
            filteredDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
        }
        double droppedRate  = 1.0 - static_cast<double>(filteredDataWeight) / static_cast<double>(totalDataWeight);
        YT_LOG_DEBUG("PREWHERE filtration finished (DroppedRate: %v)", droppedRate);

        BlockInputStream_ = CreateBlockInputStream(
            StorageContext_,
            SubquerySpec_,
            RealColumnNames_,
            VirtualColumnNames_,
            TraceContext_,
            DataSliceDescriptors_,
            PrewhereInfo_,
            // Chunks have been already prefiltered.
            /*granuleFilter*/ nullptr);
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
    const std::vector<TString> RealColumnNames_;
    const std::vector<TString> VirtualColumnNames_;
    NTracing::TTraceContextPtr TraceContext_;
    DB::PrewhereInfoPtr PrewhereInfo_;
    DB::Block Header_;
    const IGranuleFilterPtr GranuleFilter_;

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
    const std::vector<TString>& realColumns,
    const std::vector<TString>& virtualColumns,
    const NTracing::TTraceContextPtr& traceContext,
    std::vector<NChunkClient::TDataSliceDescriptor> dataSliceDescriptors,
    DB::PrewhereInfoPtr prewhereInfo,
    IGranuleFilterPtr granuleFilter)
{
    return std::make_shared<TPrewhereBlockInputStream>(
        storageContext,
        subquerySpec,
        realColumns,
        virtualColumns,
        traceContext,
        std::move(prewhereInfo),
        std::move(dataSliceDescriptors),
        std::move(granuleFilter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
