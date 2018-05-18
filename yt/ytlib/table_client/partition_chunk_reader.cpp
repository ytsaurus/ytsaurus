#include "partition_chunk_reader.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "name_table.h"
#include "schema.h"
#include "schemaless_chunk_reader.h"

#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/reader_factory.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NRpc;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NYTree;

using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TChunkReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkReader::TPartitionChunkReader(
    TBlockFetcherConfigPtr config,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    int partitionTag)
    : TChunkReaderBase(
        config,
        underlyingReader,
        blockCache,
        TReadSessionId())
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , PartitionTag_(partitionTag)
{
    ReadyEvent_ = BIND(&TPartitionChunkReader::InitializeBlockSequence, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

TFuture<void> TPartitionChunkReader::InitializeBlockSequence()
{
    std::vector<int> extensionTags = {
        TProtoExtensionTag<TMiscExt>::Value,
        TProtoExtensionTag<NProto::TBlockMetaExt>::Value,
        TProtoExtensionTag<NProto::TNameTableExt>::Value,
        TProtoExtensionTag<NProto::TKeyColumnsExt>::Value
    };

    TClientBlockReadOptions options;
    options.WorkloadDescriptor = Config_->WorkloadDescriptor;
    options.ChunkReaderStatistics = New<TChunkReaderStatistics>(); //FIXME(savrus) pass correct value here
    options.ReadSessionId = ReadSessionId_;

    ChunkMeta_ = WaitFor(UnderlyingReader_->GetMeta(
        options,
        PartitionTag_,
        extensionTags))
        .ValueOrThrow();

    YCHECK(ChunkMeta_.version() == static_cast<int>(ETableChunkFormat::SchemalessHorizontal));

    TNameTablePtr chunkNameTable;
    auto nameTableExt = GetProtoExtension<NProto::TNameTableExt>(ChunkMeta_.extensions());
    try {
        FromProto(&chunkNameTable, nameTableExt);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::CorruptedNameTable,
            "Failed to deserialize name table for partition chunk reader")
            << TErrorAttribute("chunk_id", UnderlyingReader_->GetChunkId())
            << ex;
    }

    InitNameTable(chunkNameTable);

    auto keyColumnsExt = GetProtoExtension<NProto::TKeyColumnsExt>(ChunkMeta_.extensions());
    auto chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);
    YCHECK(chunkKeyColumns == KeyColumns_);

    BlockMetaExt_ = GetProtoExtension<NProto::TBlockMetaExt>(ChunkMeta_.extensions());
    std::vector<TBlockFetcher::TBlockInfo> blocks;
    for (auto& blockMeta : BlockMetaExt_.blocks()) {
        TBlockFetcher::TBlockInfo blockInfo;
        blockInfo.Index = blockMeta.block_index();
        blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
        blockInfo.Priority = blocks.size();
        blocks.push_back(blockInfo);
    }

    return DoOpen(blocks, GetProtoExtension<TMiscExt>(ChunkMeta_.extensions()));
}

void TPartitionChunkReader::InitFirstBlock()
{
    YCHECK(CurrentBlock_ && CurrentBlock_.IsSet());
    BlockReader_ = new THorizontalSchemalessBlockReader(
        CurrentBlock_.Get().ValueOrThrow().Data,
        BlockMetaExt_.blocks(CurrentBlockIndex_),
        IdMapping_,
        KeyColumns_.size(),
        KeyColumns_.size());

    BlockReaders_.emplace_back(BlockReader_);
}

void TPartitionChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    InitFirstBlock();
}

void TPartitionChunkReader::InitNameTable(TNameTablePtr chunkNameTable)
{
    IdMapping_.reserve(chunkNameTable->GetSize());

    try {
        for (int chunkNameId = 0; chunkNameId < chunkNameTable->GetSize(); ++chunkNameId) {
            auto name = chunkNameTable->GetName(chunkNameId);
            auto id = NameTable_->GetIdOrRegisterName(name);
            IdMapping_.push_back({chunkNameId, id});
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to add column to name table for partition chunk reader") << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionMultiChunkReader::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<TPartitionChunkReader*>(CurrentSession_.Reader.Get());
    YCHECK(CurrentReader_);
}

TPartitionMultiChunkReaderPtr CreatePartitionMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    INativeClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    int partitionTag,
    TTrafficMeterPtr trafficMeter)
{
    std::vector<IReaderFactoryPtr> factories;
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        const auto& dataSource = dataSourceDirectory->DataSources()[dataSliceDescriptor.GetDataSourceIndex()];
        switch (dataSource.GetType()) {
            case EDataSourceType::UnversionedTable: {
                const auto& chunkSpec = dataSliceDescriptor.GetSingleChunk();

                auto memoryEstimate = GetChunkReaderMemoryEstimate(chunkSpec, config);
                auto createReader = [=] () {
                    auto remoteReader = CreateRemoteReader(
                        chunkSpec,
                        config,
                        options,
                        client,
                        nodeDirectory,
                        TNodeDescriptor(),
                        blockCache,
                        trafficMeter,
                        GetUnlimitedThrottler());

                    YCHECK(!chunkSpec.has_lower_limit());
                    YCHECK(!chunkSpec.has_upper_limit());

                    TBlockFetcherConfigPtr sequentialReaderConfig = config;

                    return New<TPartitionChunkReader>(
                        sequentialReaderConfig,
                        remoteReader,
                        nameTable,
                        blockCache,
                        keyColumns,
                        partitionTag);
                };

                factories.emplace_back(CreateReaderFactory(
                    createReader,
                    memoryEstimate,
                    dataSliceDescriptor));
                break;
            }

            default:
                Y_UNREACHABLE();
        }
    }

    auto reader = New<TPartitionMultiChunkReader>(
        config,
        options,
        factories);

    reader->Open();
    return reader;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NTableClient
} // namespace NYT

