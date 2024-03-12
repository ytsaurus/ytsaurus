#include "partition_tables.h"

#include <yt/yt/ytlib/chunk_client/combine_data_slices.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/chunk_pools/chunk_pool.h>
#include <yt/yt/ytlib/chunk_pools/chunk_pool_factory.h>
#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/yt/ytlib/table_client/columnar_statistics_fetcher.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/ytree/permission.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/iterator/zip.h>

#include <algorithm>

namespace NYT::NApi::NNative {

using namespace NChunkClient;
using namespace NChunkPools;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TMultiTablePartitioner::TMultiTablePartitioner(
    IClientPtr client,
    std::vector<TRichYPath> paths,
    TPartitionTablesOptions options,
    NLogging::TLogger logger)
    : Client_(std::move(client))
    , Paths_(std::move(paths))
    , Options_(std::move(options))
    , Logger(std::move(logger))
{ }

TMultiTablePartitions TMultiTablePartitioner::PartitionTables()
{
    YT_LOG_INFO("Partitioning tables (DataWeightPerPartition: %v, MaxPartitionCount: %v, AdjustDataWeightPerPartition: %v)",
        Options_.DataWeightPerPartition,
        Options_.MaxPartitionCount,
        Options_.AdjustDataWeightPerPartition);

    InitializeChunkPool();
    CollectInput();
    BuildPartitions();

    return Partitions_;
}

void TMultiTablePartitioner::InitializeChunkPool()
{
    ChunkPool_ = CreateChunkPool(
        Options_.PartitionMode,
        Options_.DataWeightPerPartition,
        Options_.AdjustDataWeightPerPartition ? Options_.MaxPartitionCount : std::nullopt,
        Logger);
}

void TMultiTablePartitioner::CollectInput()
{
    YT_LOG_INFO("Collecting input (TableCount: %v)", Paths_.size());

    YT_VERIFY(ChunkPool_);

    int totalChunkCount = 0;

    std::vector<TInputTable> inputTables;

    auto columnarStatisticsFetcher = New<TColumnarStatisticsFetcher>(
        CreateSerializedInvoker(GetCurrentInvoker()),
        Client_,
        TColumnarStatisticsFetcher::TOptions{
            .Config = Options_.FetcherConfig,
            .NodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory(),
            .EnableEarlyFinish = true,
            .Logger = Logger,
        });

    for (const auto& [tableIndex, path] : Enumerate(Paths_)) {
        auto transactionId = path.GetTransactionId();

        // TODO(galtsev): make these requests asynchronously; see https://a.yandex-team.ru/review/2564596/details#comment-3570976
        auto [inputChunks, schema, dynamic] = CollectTableInputChunks(
            path,
            Client_,
            /*nodeDirectory*/ nullptr,
            Options_.FetchChunkSpecConfig,
            transactionId
                ? *transactionId
                : Options_.TransactionId,
            {
                TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value,
                TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value,
            },
            Logger);

        YT_LOG_DEBUG("Input chunks fetched (TableIndex: %v, Path: %v, Schema: %v, ChunkCount: %v)",
            tableIndex,
            path,
            schema,
            inputChunks.size());

        AddDataSource(tableIndex, schema, dynamic);

        if (path.GetColumns()) {
            auto stableColumnNames = MapNamesToStableNames(*schema, *path.GetColumns(), NonexistentColumnName);
            for (const auto& inputChunk : inputChunks) {
                columnarStatisticsFetcher->AddChunk(inputChunk, stableColumnNames);
            }
        }

        FixLimitsInOrderedDynamicStore(tableIndex, inputChunks);

        inputTables.emplace_back(TInputTable{std::move(inputChunks), static_cast<int>(tableIndex)});

        totalChunkCount += inputChunks.size();
    }

    if (columnarStatisticsFetcher->GetChunkCount() > 0) {
        YT_LOG_INFO("Fetching chunk columnar statistics for tables with column selectors (ChunkCount: %v)",
            columnarStatisticsFetcher->GetChunkCount());
        WaitFor(columnarStatisticsFetcher->Fetch())
            .ThrowOnError();
        YT_LOG_INFO("Columnar statistics fetched");
        columnarStatisticsFetcher->ApplyColumnSelectivityFactors();
    }

    YT_VERIFY(IsDataSourcesReady());

    for (const auto& inputTable : inputTables) {
        YT_LOG_DEBUG("Fetching chunks (Path: %v)", Paths_[inputTable.TableIndex]);

        const auto& dataSource = DataSourceDirectory_->DataSources()[inputTable.TableIndex];
        auto dynamic = dataSource.GetType() == EDataSourceType::VersionedTable;
        auto sorted = dataSource.Schema()->IsSorted();

        if (dynamic && sorted) {
            RequestVersionedDataSlices(inputTable);
        } else {
            AddUnversionedDataSlices(inputTable);
        }
    }

    FetchVersionedDataSlices();

    YT_LOG_INFO("Finishing chunk pool (TotalChunkCount: %v)", totalChunkCount);

    ChunkPool_->Finish();

    YT_LOG_INFO("Input collected");
}

void TMultiTablePartitioner::BuildPartitions()
{
    YT_LOG_INFO("Building partitions");

    YT_VERIFY(ChunkPool_);
    YT_VERIFY(IsDataSourcesReady());

    while (true) {
        auto cookie = ChunkPool_->Extract();
        if (cookie == IChunkPoolOutput::NullCookie) {
            break;
        }

        if (Options_.MaxPartitionCount && std::ssize(Partitions_.Partitions) >= *Options_.MaxPartitionCount) {
            THROW_ERROR_EXCEPTION("Maximum partition count exceeded")
                << TErrorAttribute("limit", *Options_.MaxPartitionCount);
        }

        auto chunkStripeList = ChunkPool_->GetStripeList(cookie);
        auto slicesByTable = ConvertChunkStripeListIntoDataSliceDescriptors(chunkStripeList);

        Partitions_.Partitions.emplace_back(TMultiTablePartition{
            CombineDataSlices(DataSourceDirectory_, slicesByTable, Paths_),
            chunkStripeList->GetAggregateStatistics()});
    }

    YT_LOG_INFO("Partitions built (PartitionCount: %v)", Partitions_.Partitions.size());
}

bool TMultiTablePartitioner::IsDataSourcesReady()
{
    YT_VERIFY(DataSourceDirectory_->DataSources().size() <= Paths_.size());

    return DataSourceDirectory_->DataSources().size() == Paths_.size();
}

void TMultiTablePartitioner::AddDataSource(int tableIndex, const TTableSchemaPtr& schema, bool dynamic)
{
    YT_VERIFY(!IsDataSourcesReady());
    YT_VERIFY(tableIndex == std::ssize(DataSourceDirectory_->DataSources()));

    auto& dataSource = DataSourceDirectory_->DataSources().emplace_back();
    auto& path = Paths_[tableIndex];

    if (dynamic) {
        dataSource = MakeVersionedDataSource(
            path.GetPath(),
            schema,
            path.GetColumns(),
            /*omittedInaccessibleColumns*/ {},
            path.GetTimestamp().value_or(NTransactionClient::AsyncLastCommittedTimestamp));
    } else {
        dataSource = MakeUnversionedDataSource(
            path.GetPath(),
            schema,
            path.GetColumns(),
            /*omittedInaccessibleColumns*/ {});
    }
}

std::vector<std::vector<TDataSliceDescriptor>> TMultiTablePartitioner::ConvertChunkStripeListIntoDataSliceDescriptors(
    const TChunkStripeListPtr& chunkStripeList)
{
    YT_VERIFY(IsDataSourcesReady());

    std::vector<std::vector<TDataSliceDescriptor>> slicesByTable(DataSourceDirectory_->DataSources().size());

    for (auto chunkStripe : chunkStripeList->Stripes) {
        for (auto dataSlice : chunkStripe->DataSlices) {
            auto tableIndex = dataSlice->GetInputStreamIndex();
            const auto& comparator = GetComparator(tableIndex);
            for (auto chunkSlice : dataSlice->ChunkSlices) {
                YT_VERIFY(tableIndex < std::ssize(slicesByTable));
                auto& dataSliceDescriptor = slicesByTable[tableIndex].emplace_back();
                auto& chunkSpec = dataSliceDescriptor.ChunkSpecs.emplace_back();

                ToProto(&chunkSpec, chunkSlice, comparator, dataSlice->Type);
            }
        }
    }

    return slicesByTable;
}

void TMultiTablePartitioner::AddDataSlice(int tableIndex, TLegacyDataSlicePtr dataSlice)
{
    YT_VERIFY(ChunkPool_);

    dataSlice->SetInputStreamIndex(tableIndex);
    auto chunkStripe = New<TChunkStripe>(std::move(dataSlice));

    ChunkPool_->Add(std::move(chunkStripe));
}

void TMultiTablePartitioner::RequestVersionedDataSlices(const TInputTable& inputTable)
{
    const auto& [inputChunks, tableIndex] = inputTable;
    const auto& comparator = GetComparator(tableIndex);

    YT_LOG_DEBUG("Fetching versioned data slices (TableIndex: %v, ChunkCount: %v)",
        tableIndex,
        inputChunks.size());

    TRowBufferPtr rowBuffer = New<TRowBuffer>();
    auto fetcher = CreateChunkSliceFetcher(
        Options_.ChunkSliceFetcherConfig,
        Client_->GetNativeConnection()->GetNodeDirectory(),
        GetCurrentInvoker(),
        /*chunkScraper*/ nullptr,
        Client_,
        rowBuffer,
        Logger);

    for (const auto& inputChunk : inputChunks) {
        auto inputChunkSlice = CreateInputChunkSlice(inputChunk);
        InferLimitsFromBoundaryKeys(inputChunkSlice, rowBuffer);
        auto dataSlice = CreateUnversionedInputDataSlice(inputChunkSlice);
        dataSlice->SetInputStreamIndex(tableIndex);
        dataSlice->TransformToNew(rowBuffer, comparator.GetLength());
        YT_LOG_TRACE("Add data slice for slicing (TableIndex: %v, DataSlice: %v)",
            tableIndex,
            dataSlice);
        fetcher->AddDataSliceForSlicing(dataSlice, comparator, Options_.DataWeightPerPartition, /*sliceByKeys*/ true);
    }

    FetchState_.TableFetchers.push_back(std::move(fetcher));
    FetchState_.TableIndices.push_back(tableIndex);
}

void TMultiTablePartitioner::FetchVersionedDataSlices()
{
    YT_VERIFY(IsDataSourcesReady());

    if (FetchState_.TableFetchers.empty()) {
        return;
    }

    std::vector<TFuture<void>> asyncResults;
    asyncResults.reserve(FetchState_.TableFetchers.size());
    for (const auto& fetcher : FetchState_.TableFetchers) {
        asyncResults.push_back(fetcher->Fetch());
    }

    WaitFor(AllSucceeded(asyncResults))
        .ThrowOnError();

    for (const auto& [fetcher, tableIndex] : Zip(FetchState_.TableFetchers, FetchState_.TableIndices)) {
        const auto& comparator = GetComparator(tableIndex);
        auto chunkSliceList = fetcher->GetChunkSlices();
        for (const auto& dataSlice : CombineVersionedChunkSlices(chunkSliceList, comparator)) {
            AddDataSlice(tableIndex, dataSlice);
        }
    }
}

void TMultiTablePartitioner::AddUnversionedDataSlices(const TInputTable& inputTable)
{
    const auto& comparator = GetComparator(inputTable.TableIndex);

    for (const auto& inputChunk : inputTable.Chunks) {
        auto inputChunkSlice = CreateInputChunkSlice(inputChunk);
        inputChunkSlice->TransformToNew(RowBuffer_, comparator.GetLength());
        TLegacyDataSlice::TChunkSliceList inputChunkSliceList;

        inputChunkSliceList.emplace_back(std::move(inputChunkSlice));
        auto dataSlice = New<TLegacyDataSlice>(
            EDataSourceType::UnversionedTable,
            std::move(inputChunkSliceList),
            TInputSliceLimit());

        AddDataSlice(inputTable.TableIndex, dataSlice);
    }
}

TComparator TMultiTablePartitioner::GetComparator(int tableIndex)
{
    YT_VERIFY(tableIndex < std::ssize(DataSourceDirectory_->DataSources()));

    return DataSourceDirectory_->DataSources()[tableIndex].Schema()->ToComparator();
}

void TMultiTablePartitioner::FixLimitsInOrderedDynamicStore(
    size_t tableIndex,
    const std::vector<NChunkClient::TInputChunkPtr>& inputChunks)
{
    YT_VERIFY(tableIndex < DataSourceDirectory_->DataSources().size());

    const auto& dataSource = DataSourceDirectory_->DataSources()[tableIndex];
    auto dynamic = dataSource.GetType() == EDataSourceType::VersionedTable;
    auto sorted = dataSource.Schema()->IsSorted();

    if (!dynamic || sorted) {
        return;
    }

    std::vector<size_t> dynamicStores;
    THashMap<i64, i64> maxStaticStoreUpperRowIndexForTablet;

    for (const auto& [chunkIndex, inputChunk] : Enumerate(inputChunks)) {
        if (inputChunk->IsOrderedDynamicStore()) {
            dynamicStores.push_back(chunkIndex);
        } else {
            YT_VERIFY(inputChunk->GetTableRowIndex() >= 0);
            YT_VERIFY(inputChunk->GetTotalRowCount() >= 0);

            maxStaticStoreUpperRowIndexForTablet[inputChunk->GetTabletIndex()] = std::max(
                maxStaticStoreUpperRowIndexForTablet[inputChunk->GetTabletIndex()],
                inputChunk->GetTableRowIndex() + inputChunk->GetTotalRowCount());
        }
    }

    for (const auto& chunkIndex : dynamicStores) {
        auto& inputChunk = inputChunks[chunkIndex];

        // Rows in ordered dynamic stores go after rows in static stores of the ordered dynamic table.
        i64 lowerRowIndex = maxStaticStoreUpperRowIndexForTablet[inputChunk->GetTabletIndex()];

        if (!inputChunk->LowerLimit()) {
            inputChunk->LowerLimit() = std::make_unique<TLegacyReadLimit>();
        }
        if (!inputChunk->LowerLimit()->HasRowIndex()) {
            inputChunk->LowerLimit()->SetRowIndex(lowerRowIndex);
        }
        if (!inputChunk->UpperLimit()) {
            inputChunk->UpperLimit() = std::make_unique<TLegacyReadLimit>();
        }
        if (!inputChunk->UpperLimit()->HasRowIndex()) {
            YT_VERIFY(inputChunk->GetTotalRowCount() >= 0);
            inputChunk->UpperLimit()->SetRowIndex(lowerRowIndex + inputChunk->GetTotalRowCount());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
