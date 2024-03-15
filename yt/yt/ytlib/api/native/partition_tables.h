#pragma once

#include "client_impl.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TMultiTablePartitioner
{
public:
    TMultiTablePartitioner(
        IClientPtr client,
        std::vector<NYPath::TRichYPath> paths,
        TPartitionTablesOptions options,
        NLogging::TLogger logger);

    TMultiTablePartitions PartitionTables();

private:
    struct TInputTable
    {
        std::vector<NChunkClient::TInputChunkPtr> Chunks;
        int TableIndex;
    };

    struct TVersionedSliceFetchState
    {
        std::vector<NTableClient::IChunkSliceFetcherPtr> TableFetchers;
        std::vector<int> TableIndices;
    };

    const IClientPtr Client_;
    const std::vector<NYPath::TRichYPath> Paths_;
    const TPartitionTablesOptions Options_;
    const NLogging::TLogger Logger;

    NChunkPools::IChunkPoolPtr ChunkPool_;
    const NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory_ = New<NChunkClient::TDataSourceDirectory>();
    TMultiTablePartitions Partitions_;
    NTableClient::TRowBufferPtr RowBuffer_ = New<NTableClient::TRowBuffer>();
    TVersionedSliceFetchState FetchState_;

    void InitializeChunkPool();
    void CollectInput();
    void BuildPartitions();

    bool IsDataSourcesReady();
    void AddDataSource(int tableIndex, const NQueryClient::TTableSchemaPtr& schema, bool dynamic);
    std::vector<std::vector<NChunkClient::TDataSliceDescriptor>> ConvertChunkStripeListIntoDataSliceDescriptors(
        const NChunkPools::TChunkStripeListPtr& chunkStripeList);
    void AddDataSlice(int tableIndex, NChunkClient::TLegacyDataSlicePtr dataSlice);
    void RequestVersionedDataSlices(const TInputTable& inputTable);
    void FetchVersionedDataSlices();
    void AddUnversionedDataSlices(const TInputTable& inputTable);
    NTableClient::TComparator GetComparator(int tableIndex);
    void FixLimitsInOrderedDynamicStore(
        size_t tableIndex,
        const std::vector<NChunkClient::TInputChunkPtr>& inputChunks);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
