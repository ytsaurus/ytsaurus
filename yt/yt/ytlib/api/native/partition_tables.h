#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/table_client.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_pools/public.h>

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/library/query/row_level_security_api/row_level_security.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TMultiTablePartitioner
{
public:
    TMultiTablePartitioner(
        IClientPtr client,
        std::vector<NYPath::TRichYPath> paths,
        TPartitionTablesOptions options,
        std::string user,
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
    const std::string User_;
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
    // FIXME(coteeq): Hide read spec somehow
    void AddDataSource(int tableIndex, const NTableClient::TTableSchemaPtr& schema, bool dynamic, std::optional<NTableClient::TRlsReadSpec> rlsReadSpec);
    std::vector<std::vector<NChunkClient::TDataSliceDescriptor>> ConvertChunkStripeListIntoDataSliceDescriptors(
        const NChunkPools::TChunkStripeListPtr& chunkStripeList);
    void AddDataSlice(int tableIndex, NChunkClient::TLegacyDataSlicePtr dataSlice);
    void PrepareVersionedSliceFetcher(const TInputTable& inputTable);
    void FetchVersionedDataSlices();
    void AddUnversionedDataSlices(const TInputTable& inputTable);
    NTableClient::TComparator GetComparator(int tableIndex);
    void FixLimitsInOrderedDynamicStore(
        int tableIndex,
        const std::vector<NChunkClient::TInputChunkPtr>& inputChunks);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
