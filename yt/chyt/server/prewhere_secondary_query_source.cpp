#include "prewhere_secondary_query_source.h"

#include "secondary_query_source.h"
#include "host.h"
#include "query_context.h"
#include "read_plan.h"
#include "subquery_spec.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <Interpreters/ExpressionActions.h>

namespace NYT::NClickHouseServer {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NTableClient;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

std::vector<TDataSliceDescriptor> GetFilteredDataSliceDescriptors(DB::SourcePtr source, ISchemalessMultiChunkReaderPtr reader)
{
    std::vector<TDataSliceDescriptor> filteredDataSliceDescriptors;

    DB::InputPort nullPort(source->getPort().getHeader());
    DB::connect(source->getPort(), nullPort);
    nullPort.setNeeded();

    for (auto status = source->prepare(); status != DB::IProcessor::Status::Finished; status = source->prepare()) {
        if (status == DB::IProcessor::Status::Ready) {
            source->work();
        } else if (status == DB::IProcessor::Status::PortFull) {
            if (nullPort.pull()) {
                filteredDataSliceDescriptors.emplace_back(reader->GetCurrentReaderDescriptor());
                reader->SkipCurrentReader();
            }
        } else {
            THROW_ERROR_EXCEPTION(
                "Unexpected status in prepare while filetring data slices for prewhere source: %v",
                DB::IProcessor::statusToName(status));
        }
    }

    return filteredDataSliceDescriptors;
}

std::vector<TDataSliceDescriptor> FilterDataSliceDescriptorsByPrewhereInfo(
    std::vector<TDataSliceDescriptor>&& dataSliceDescriptors,
    const TReadPlanWithFilterPtr& readPlan,
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    const TTraceContextPtr& traceContext,
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

    auto chunkReadOptions = CreateChunkReadOptions(queryContext->User, std::move(granuleFilter));
    auto reader = CreateSourceReader(
        storageContext,
        subquerySpec,
        prewhereReadPlan,
        chunkReadOptions,
        dataSliceDescriptors);

    auto source = CreateSecondaryQuerySource(
        reader,
        std::move(prewhereReadPlan),
        traceContext,
        queryContext->Host,
        storageContext->Settings,
        Logger.WithTag("ReadSessionId: %v", chunkReadOptions.ReadSessionId),
        chunkReadOptions.ChunkReaderStatistics,
        std::move(statisticsCallback));

    return WaitFor(BIND(&GetFilteredDataSliceDescriptors, std::move(source) , std::move(reader))
        .AsyncVia(queryContext->Host->GetClickHouseWorkerInvoker())
        .Run())
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TPrewhereSecondaryQuerySource
    : public DB::ISource
{
public:
    TPrewhereSecondaryQuerySource(
        TStorageContext* storageContext,
        const TSubquerySpec& subquerySpec,
        TReadPlanWithFilterPtr readPlan,
        TTraceContextPtr traceContext,
        std::vector<TDataSliceDescriptor> dataSliceDescriptors,
        IGranuleFilterPtr granuleFilter,
        TCallback<void(const TStatistics&)> statisticsCallback)
    : DB::ISource(
        DeriveHeaderBlockFromReadPlan(readPlan, storageContext->Settings->Composite),
        /*enable_auto_progress*/ false)
    , StorageContext_(storageContext)
    , QueryContext_(storageContext->QueryContext)
    , SubquerySpec_(subquerySpec)
    , ReadPlan_(std::move(readPlan))
    , TraceContext_(std::move(traceContext))
    , GranuleFilter_(std::move(granuleFilter))
    , StatisticsCallback_(std::move(statisticsCallback))
    , DataSliceDescriptors_(std::move(dataSliceDescriptors))
    , Logger(QueryContext_->Logger)
    { }

    DB::String getName() const override
    {
        return "PrewhereSecondaryQuerySource";
    }


    Status prepare() override
    {
        auto selfStatus = DB::ISource::prepare();
        if (selfStatus != Status::Ready) {
            return selfStatus;
        }

        if (!InnerSource_) {
            return Status::Ready;
        }

        auto status = InnerSource_->prepare();
        if (InnerSourceOutput_->hasData()) {
            output.pushData(InnerSourceOutput_->pullData());
            return Status::PortFull;
        }

        finished = (status == Status::Finished);

        return Status::Ready;
    }

    void work() override
    {
        if (finished) {
            return;
        }

        if (!InnerSource_) {
            PrepareInnerSource();
        }

        InnerSource_->work();

        if (auto workProgress = InnerSource_->getReadProgress()) {
            addTotalRowsApprox(workProgress->counters.total_rows_approx);
            addTotalBytes(workProgress->counters.total_bytes);
            progress(workProgress->counters.read_rows, workProgress->counters.read_bytes);
        }
    }

private:
    void PrepareInnerSource()
    {
        i64 totalDataWeight = 0;
        for (const auto& dataSliceDescriptor : DataSliceDescriptors_) {
            totalDataWeight += dataSliceDescriptor.ChunkSpecs[0].data_weight_override();
        }

        DataSliceDescriptors_ = FilterDataSliceDescriptorsByPrewhereInfo(
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

        InnerSource_ = CreateSecondaryQuerySource(
            StorageContext_,
            SubquerySpec_,
            ReadPlan_,
            TraceContext_,
            DataSliceDescriptors_,
            // Chunks have been already prefiltered.
            /*granuleFilter*/ nullptr,
            StatisticsCallback_);
        InnerSourceOutput_ = std::make_shared<DB::InputPort>(InnerSource_->getPort().getHeader());
        DB::connect(InnerSource_->getPort(), *InnerSourceOutput_);
        InnerSourceOutput_->setNeeded();
    }

    TStorageContext* StorageContext_;
    TQueryContext* QueryContext_;
    const TSubquerySpec SubquerySpec_;
    TReadPlanWithFilterPtr ReadPlan_;
    TTraceContextPtr TraceContext_;
    const IGranuleFilterPtr GranuleFilter_;
    TCallback<void(const TStatistics&)> StatisticsCallback_;

    std::vector<TDataSliceDescriptor> DataSliceDescriptors_;

    std::shared_ptr<DB::InputPort> InnerSourceOutput_;
    DB::SourcePtr InnerSource_;

    TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

DB::SourcePtr CreatePrewhereSecondaryQuerySource(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    TReadPlanWithFilterPtr readPlan,
    const TTraceContextPtr& traceContext,
    std::vector<TDataSliceDescriptor> dataSliceDescriptors,
    IGranuleFilterPtr granuleFilter,
    TCallback<void(const TStatistics&)> statisticsCallback)
{
    for (const auto& dataSource : subquerySpec.DataSourceDirectory->DataSources()) {
        if (dataSource.GetType() == EDataSourceType::VersionedTable) {
            THROW_ERROR_EXCEPTION("PREWHERE stage is not supported for dynamic tables (CHYT-462)");
        }
    }

    return std::make_shared<TPrewhereSecondaryQuerySource>(
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
