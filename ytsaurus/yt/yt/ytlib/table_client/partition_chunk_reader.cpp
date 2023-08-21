#include "partition_chunk_reader.h"
#include "private.h"

#include "chunk_meta_extensions.h"
#include "columnar_chunk_meta.h"
#include "schemaless_multi_chunk_reader.h"
#include "helpers.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>
#include <yt/yt/ytlib/chunk_client/reader_factory.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NRpc;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NTracing;

using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TChunkReaderStatistics;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkReader::TPartitionChunkReader(
    TBlockFetcherConfigPtr config,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    IBlockCachePtr blockCache,
    const TClientChunkReadOptions& chunkReadOptions,
    int partitionTag,
    const NChunkClient::TDataSource& dataSource,
    TChunkReaderMemoryManagerPtr memoryManager)
    : TChunkReaderBase(
        std::move(config),
        std::move(underlyingReader),
        std::move(blockCache),
        chunkReadOptions,
        std::move(memoryManager))
    , NameTable_(nameTable)
    , PartitionTag_(partitionTag)
{
    // NB(gepardo): Real extraChunkTags will be packed later, in InitializeBlockSequence().
    PackBaggageForChunkReader(TraceContext_, dataSource, TExtraChunkTags{});

    SetReadyEvent(BIND(&TPartitionChunkReader::InitializeBlockSequence, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        .Run());
}

TFuture<void> TPartitionChunkReader::InitializeBlockSequence()
{
    TCurrentTraceContextGuard traceGuard(TraceContext_);

    const std::vector<int> extensionTags = {
        TProtoExtensionTag<TMiscExt>::Value,
        TProtoExtensionTag<NProto::TDataBlockMetaExt>::Value,
        TProtoExtensionTag<NProto::TTableSchemaExt>::Value,
        TProtoExtensionTag<NProto::TNameTableExt>::Value,
    };

    ChunkMeta_ = WaitFor(UnderlyingReader_->GetMeta(
        ChunkReadOptions_,
        PartitionTag_,
        extensionTags))
        .ValueOrThrow();

    SortOrders_ = GetSortOrders(GetTableSchema(*ChunkMeta_)->GetSortColumns());

    YT_VERIFY(FromProto<EChunkFormat>(ChunkMeta_->format()) == EChunkFormat::TableUnversionedSchemalessHorizontal);

    TNameTablePtr chunkNameTable;
    auto nameTableExt = GetProtoExtension<NProto::TNameTableExt>(ChunkMeta_->extensions());
    try {
        FromProto(&chunkNameTable, nameTableExt);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(
            NTableClient::EErrorCode::CorruptedNameTable,
            "Failed to deserialize name table for partition chunk reader")
            << TErrorAttribute("chunk_id", UnderlyingReader_->GetChunkId())
            << ex;
    }

    InitNameTable(chunkNameTable);

    BlockMetaExt_ = GetProtoExtension<NProto::TDataBlockMetaExt>(ChunkMeta_->extensions());
    std::vector<TBlockFetcher::TBlockInfo> blocks;
    blocks.reserve(BlockMetaExt_.data_blocks_size());
    for (const auto& blockMeta : BlockMetaExt_.data_blocks()) {
        int priority = blocks.size();
        blocks.push_back({
            .ReaderIndex = 0,
            .BlockIndex = blockMeta.block_index(),
            .Priority = priority,
            .UncompressedDataSize = blockMeta.uncompressed_size(),
            .BlockType = EBlockType::UncompressedData,
        });
    }

    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_->extensions());

    PackBaggageFromExtraChunkTags(TraceContext_, MakeExtraChunkTags(miscExt));

    return DoOpen(std::move(blocks), miscExt);
}

void TPartitionChunkReader::InitFirstBlock()
{
    TCurrentTraceContextGuard traceGuard(TraceContext_);

    YT_VERIFY(CurrentBlock_ && CurrentBlock_.IsSet());

    auto schema = GetTableSchema(*ChunkMeta_);

    BlockReader_ = new THorizontalBlockReader(
        CurrentBlock_.Get().ValueOrThrow().Data,
        BlockMetaExt_.data_blocks(CurrentBlockIndex_),
        GetCompositeColumnFlags(schema),
        GetHunkColumnFlags(FromProto<EChunkFormat>(ChunkMeta_->format()), FromProto<EChunkFeatures>(ChunkMeta_->features()), schema),
        /*hunkChunkMetasExt*/ {},
        /*hunkChunkRefsExt*/ {},
        ChunkToReaderIdMapping_,
        SortOrders_,
        SortOrders_.size(),
        /*keyWideningOptions*/ {});

    BlockReaders_.emplace_back(BlockReader_);
}

void TPartitionChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    InitFirstBlock();
}

void TPartitionChunkReader::InitNameTable(TNameTablePtr chunkNameTable)
{
    ChunkToReaderIdMapping_.reserve(chunkNameTable->GetSize());

    try {
        for (int chunkNameId = 0; chunkNameId < chunkNameTable->GetSize(); ++chunkNameId) {
            auto name = chunkNameTable->GetName(chunkNameId);
            auto id = NameTable_->GetIdOrRegisterName(name);
            ChunkToReaderIdMapping_.push_back(id);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to add column to name table for partition chunk reader") << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

TPartitionMultiChunkReader::TPartitionMultiChunkReader(IMultiReaderManagerPtr multiReaderManager)
    : MultiReaderManager_(std::move(multiReaderManager))
{
    MultiReaderManager_->SubscribeReaderSwitched(BIND(
        &TPartitionMultiChunkReader::OnReaderSwitched,
        MakeWeak(this)));
}

void TPartitionMultiChunkReader::OnReaderSwitched()
{
    CurrentReader_ = DynamicPointerCast<TPartitionChunkReader>(MultiReaderManager_->GetCurrentSession().Reader);
    YT_VERIFY(CurrentReader_);
}

TPartitionMultiChunkReaderPtr CreatePartitionMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    int partitionTag,
    const TClientChunkReadOptions& chunkReadOptions,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    if (!multiReaderMemoryManager) {
        multiReaderMemoryManager = CreateParallelReaderMemoryManager(
            TParallelReaderMemoryManagerOptions{
                .TotalReservedMemorySize = config->MaxBufferSize,
                .MaxInitialReaderReservedMemory = config->WindowSize
            },
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    std::vector<IReaderFactoryPtr> factories;
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        const auto& dataSource = dataSourceDirectory->DataSources()[dataSliceDescriptor.GetDataSourceIndex()];
        switch (dataSource.GetType()) {
            case EDataSourceType::UnversionedTable: {
                const auto& chunkSpec = dataSliceDescriptor.GetSingleChunk();

                auto memoryEstimate = GetChunkReaderMemoryEstimate(chunkSpec, config);

                auto createReader = BIND([=] () -> IReaderBasePtr {
                    auto remoteReader = CreateRemoteReader(
                        chunkSpec,
                        config,
                        options,
                        chunkReaderHost);

                    YT_VERIFY(!chunkSpec.has_lower_limit());
                    YT_VERIFY(!chunkSpec.has_upper_limit());

                    TBlockFetcherConfigPtr sequentialReaderConfig = config;

                    return New<TPartitionChunkReader>(
                        sequentialReaderConfig,
                        remoteReader,
                        nameTable,
                        chunkReaderHost->BlockCache,
                        chunkReadOptions,
                        partitionTag,
                        dataSource,
                        multiReaderMemoryManager->CreateChunkReaderMemoryManager(memoryEstimate));
                });

                auto canCreateReader = BIND([=] {
                    return multiReaderMemoryManager->GetFreeMemorySize() >= memoryEstimate;
                });

                factories.push_back(CreateReaderFactory(
                    createReader,
                    canCreateReader,
                    dataSliceDescriptor));
                break;
            }

            default:
                YT_ABORT();
        }
    }

    auto reader = New<TPartitionMultiChunkReader>(CreateParallelMultiReaderManager(
        std::move(config),
        std::move(options),
        factories,
        std::move(multiReaderMemoryManager)));

    reader->Open();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

