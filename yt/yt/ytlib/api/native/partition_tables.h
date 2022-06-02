#pragma once

#include "client_impl.h"

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TMultiTablePartitioner
{
public:
    TMultiTablePartitioner(
        const IClientPtr& client,
        const std::vector<NYPath::TRichYPath>& paths,
        const TPartitionTablesOptions& options,
        const NLogging::TLogger& logger);

    TMultiTablePartitions DoPartitionTables();

private:
    void InitializeChunkPool();
    void CollectInput();
    void BuildPartitions();

    bool IsDataSourcesReady();
    void AddDataSource(size_t tableIndex, const NQueryClient::TTableSchemaPtr& schema, bool dynamic);
    std::vector<std::vector<NChunkClient::TDataSliceDescriptor>> ConvertChunkStripeListIntoDataSliceDescriptors(const NChunkPools::TChunkStripeListPtr& chunkStripeList);

    const IClientPtr& Client_;
    const std::vector<NYPath::TRichYPath>& Paths_;
    const TPartitionTablesOptions& Options_;
    const NLogging::TLogger Logger;

    NChunkPools::IChunkPoolPtr ChunkPool_ = nullptr;
    NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory_ = New<NChunkClient::TDataSourceDirectory>();
    TMultiTablePartitions Partitions_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
