#include "chunk_file_reader.h"
#include "chunk_fragment.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NIO {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TChunkFileReaderDataBufferTag
{ };

struct TChunkFileReaderMetaBufferTag
{ };

////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
void ReadMetaHeader(
    const TSharedRef& metaFileBlob,
    const TString& fileName,
    TChunkMetaHeader_2* metaHeader,
    TRef* metaBlob)
{
    if (metaFileBlob.Size() < sizeof(T)) {
        THROW_ERROR_EXCEPTION("Chunk meta file %v is too short: at least %v bytes expected",
            fileName,
            sizeof(T));
    }
    *static_cast<T*>(metaHeader) = *reinterpret_cast<const T*>(metaFileBlob.Begin());
    *metaBlob = metaFileBlob.Slice(sizeof(T), metaFileBlob.Size());
}

} // namespace

////////////////////////////////////////////////////////////////////////////

TPhysicalChunkLayoutReader::TPhysicalChunkLayoutReader(
    TChunkId chunkId,
    TString chunkFileName,
    TString chunkMetaFileName,
    const TOptions& options,
    NLogging::TLogger logger,
    TDumpBrokenBlockCallback dumpBrokenBlockCallback,
    TDumpBrokenMetaCallback dumpBrokenMetaCallback)
    : ChunkFileName_(std::move(chunkFileName))
    , ChunkMetaFileName_(std::move(chunkMetaFileName))
    , Options_(options)
    , Logger(logger.WithTag("ChunkId: %v", chunkId))
    , ChunkId_(chunkId)
    , DumpBrokenBlockCallback_(std::move(dumpBrokenBlockCallback))
    , DumpBrokenMetaCallback_(std::move(dumpBrokenMetaCallback))
{ }

TPhysicalChunkLayoutReader::TChunkMetaWithChunkId TPhysicalChunkLayoutReader::DeserializeMeta(
    TSharedRef metaFileBlob,
    TChunkReaderStatisticsPtr chunkReaderStatistics)
{
    if (metaFileBlob.Size() < sizeof(TChunkMetaHeaderBase)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::BrokenChunkFileMeta,
            "Chunk meta file %v is too short: at least %v bytes expected",
            ChunkMetaFileName_,
            sizeof(TChunkMetaHeaderBase));
    }

    chunkReaderStatistics->MetaBytesReadFromDisk.fetch_add(
        metaFileBlob.Size(),
        std::memory_order::relaxed);

    TChunkMetaHeader_2 metaHeader;
    TRef metaBlob;
    const auto* metaHeaderBase = reinterpret_cast<const TChunkMetaHeaderBase*>(metaFileBlob.Begin());

    switch (metaHeaderBase->Signature) {
        case TChunkMetaHeader_1::ExpectedSignature:
            ReadMetaHeader<TChunkMetaHeader_1>(metaFileBlob, ChunkMetaFileName_, &metaHeader, &metaBlob);
            metaHeader.ChunkId = ChunkId_;
            break;

        case TChunkMetaHeader_2::ExpectedSignature:
            ReadMetaHeader<TChunkMetaHeader_2>(metaFileBlob, ChunkMetaFileName_, &metaHeader, &metaBlob);
            break;

        default:
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::BrokenChunkFileMeta,
                "Incorrect header signature %x in chunk meta file %v",
                metaHeaderBase->Signature,
                ChunkMetaFileName_);
    }

    auto checksum = GetChecksum(metaBlob);
    if (checksum != metaHeader.Checksum) {
        DumpBrokenMetaCallback_(metaBlob);
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::BrokenChunkFileMeta,
            "Incorrect checksum in chunk meta file %v: expected %x, actual %x",
            ChunkMetaFileName_,
            metaHeader.Checksum,
            checksum)
            << TErrorAttribute("meta_file_length", metaFileBlob.Size());
    }

    if (ChunkId_ != NullChunkId && metaHeader.ChunkId != ChunkId_) {
        THROW_ERROR_EXCEPTION("Invalid chunk id in meta file %v: expected %v, actual %v",
            ChunkMetaFileName_,
            ChunkId_,
            metaHeader.ChunkId);
    }

    TPhysicalChunkLayoutReader::TChunkMetaWithChunkId result;
    result.ChunkId = metaHeader.ChunkId;
    TChunkMeta meta;
    if (!TryDeserializeProtoWithEnvelope(&meta, metaBlob)) {
        THROW_ERROR_EXCEPTION("Failed to parse chunk meta file %v",
            ChunkMetaFileName_);
    }

    result.ChunkMeta = New<TRefCountedChunkMeta>(std::move(meta));

    return result;
}

std::vector<TBlock> TPhysicalChunkLayoutReader::DeserializeBlocks(
    TSharedRef blocksBlob,
    TBlockRange blockRange,
    const TBlocksExtPtr& blocksExt,
    TChunkReaderStatisticsPtr chunkReaderStatistics)
{
    chunkReaderStatistics->DataBytesReadFromDisk.fetch_add(
        blocksBlob.Size(),
        std::memory_order::relaxed);

    const auto& firstBlockInfo = blocksExt->Blocks[blockRange.StartBlockIndex];

    std::vector<TBlock> blocks;
    blocks.reserve(blockRange.EndBlockIndex - blockRange.StartBlockIndex);

    for (int blockIndex = blockRange.StartBlockIndex; blockIndex < blockRange.EndBlockIndex; ++blockIndex) {
        const auto& blockInfo = blocksExt->Blocks[blockIndex];
        auto block = blocksBlob.Slice(
            blockInfo.Offset - firstBlockInfo.Offset,
            blockInfo.Offset - firstBlockInfo.Offset + blockInfo.Size);
        if (Options_.ValidateBlockChecksums) {
            auto checksum = GetChecksum(block);
            if (checksum != blockInfo.Checksum) {
                DumpBrokenBlockCallback_(blockIndex, blockInfo, block);
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::IncorrectChunkFileChecksum,
                    "Incorrect checksum of block %v in chunk data file %v: expected %v, actual %v",
                    blockIndex,
                    ChunkFileName_,
                    blockInfo.Checksum,
                    checksum)
                    << TErrorAttribute("first_block_index", blockRange.StartBlockIndex)
                    << TErrorAttribute("block_count", blockRange.EndBlockIndex - blockRange.StartBlockIndex);
            }
        }
        blocks.push_back(TBlock(block, blockInfo.Checksum));
    }

    return blocks;
}

const TString& TPhysicalChunkLayoutReader::GetChunkFileName() const
{
    return ChunkFileName_;
}

const TString& TPhysicalChunkLayoutReader::GetChunkMetaFileName() const
{
    return ChunkMetaFileName_;
}

TChunkId TPhysicalChunkLayoutReader::GetChunkId() const
{
    return ChunkId_;
}

////////////////////////////////////////////////////////////////////////////

TChunkFileReader::TChunkFileReader(
    IIOEnginePtr ioEngine,
    TChunkId chunkId,
    TString fileName,
    bool validateBlocksChecksums,
    IBlocksExtCache* blocksExtCache)
    : IOEngine_(std::move(ioEngine))
    , BlocksExtCache_(std::move(blocksExtCache))
    , ChunkLayoutReader_(New<TPhysicalChunkLayoutReader>(
        chunkId,
        fileName,
        fileName + ChunkMetaSuffix,
        TPhysicalChunkLayoutReader::TOptions{
            .ValidateBlockChecksums=validateBlocksChecksums,
        },
        IOLogger(),
        BIND(&TChunkFileReader::DumpBrokenBlock, MakeWeak(this)),
        BIND(&TChunkFileReader::DumpBrokenMeta, MakeWeak(this))))
    , Logger(IOLogger())
{ }

TFuture<std::vector<TBlock>> TChunkFileReader::ReadBlocks(
    const TClientChunkReadOptions& options,
    const std::vector<int>& blockIndexes,
    TFairShareSlotId fairShareSlotId,
    TBlocksExtPtr blocksExt)
{
    std::vector<TFuture<std::vector<TBlock>>> futures;
    auto count = std::ssize(blockIndexes);

    try {
        // Extract maximum contiguous ranges of blocks.
        int localIndex = 0;
        while (localIndex < count) {
            int startLocalIndex = localIndex;
            int startBlockIndex = blockIndexes[startLocalIndex];
            int endLocalIndex = startLocalIndex;
            while (endLocalIndex < count &&
                   blockIndexes[endLocalIndex] == startBlockIndex + (endLocalIndex - startLocalIndex))
            {
                ++endLocalIndex;
            }

            int blockCount = endLocalIndex - startLocalIndex;
            auto subfuture = DoReadBlocks(options, startBlockIndex, blockCount, fairShareSlotId, blocksExt);
            futures.push_back(std::move(subfuture));

            localIndex = endLocalIndex;
        }
    } catch (const std::exception& ex) {
        TError error(ex);
        for (const auto& future : futures) {
            future.Cancel(error);
        }
        return MakeFuture<std::vector<TBlock>>(error);
    }

    return AllSet(std::move(futures))
        .Apply(BIND([count] (const std::vector<TErrorOr<std::vector<TBlock>>>& result) {
            std::vector<TBlock> blocks;
            blocks.reserve(count);

            for (const auto& subblocksOrError : result) {
                const auto& subblocks = subblocksOrError.ValueOrThrow();
                blocks.insert(blocks.end(), subblocks.begin(), subblocks.end());
            }

            return blocks;
        }));
}

TFuture<std::vector<TBlock>> TChunkFileReader::ReadBlocks(
    const TClientChunkReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    TFairShareSlotId fairShareSlotId,
    TBlocksExtPtr blocksExt)
{
    YT_VERIFY(firstBlockIndex >= 0);

    try {
        return DoReadBlocks(options, firstBlockIndex, blockCount, fairShareSlotId, std::move(blocksExt));
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TBlock>>(ex);
    }
}

i64 TChunkFileReader::GetMetaSize() const
{
    return NFS::GetPathStatistics(ChunkLayoutReader_->GetChunkMetaFileName()).Size;
}

TFuture<TRefCountedChunkMetaPtr> TChunkFileReader::GetMeta(
    const TClientChunkReadOptions& options,
    TFairShareSlotId fairShareSlotId,
    const std::optional<TPartitionTags>& partitionTags)
{
    try {
        return DoReadMeta(options, partitionTags, fairShareSlotId);
    } catch (const std::exception& ex) {
        return MakeFuture<TRefCountedChunkMetaPtr>(ex);
    }
}

TChunkId TChunkFileReader::GetChunkId() const
{
    return ChunkLayoutReader_->GetChunkId();
}

TFuture<void> TChunkFileReader::PrepareToReadChunkFragments(
    const TClientChunkReadOptions& options,
    bool useDirectIO)
{
    auto directIOFlag = GetDirectIOFlag(useDirectIO);

    // Fast path.
    if (ChunkFragmentReadsPrepared_[directIOFlag].load()) {
        return {};
    }

    // Slow path.
    TFuture<void> chunkFragmentFuture;

    auto guard = Guard(ChunkFragmentReadsLock_);
    if (!ChunkFragmentReadsPreparedFuture_[directIOFlag]) {
        auto chunkFragmentPromise = NewPromise<void>();
        ChunkFragmentReadsPreparedFuture_[directIOFlag] = chunkFragmentPromise;
        guard.Release();

        chunkFragmentFuture = OpenDataFile(directIOFlag)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& /*handle*/) {
                {
                    auto guard = Guard(ChunkFragmentReadsLock_);

                    if (BlocksExt_) {
                        ChunkFragmentReadsPrepared_[directIOFlag].store(true);
                        return VoidFuture;
                    }

                    if (BlocksExtCache_) {
                        BlocksExt_ = BlocksExtCache_->Find();
                    }

                    if (BlocksExt_) {
                        ChunkFragmentReadsPrepared_[directIOFlag].store(true);
                        return VoidFuture;
                    }
                }

                return DoReadMeta(options, /*partitionTags*/ {}, /*fairShareSlotId*/ {})
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
                        auto guard = Guard(ChunkFragmentReadsLock_);
                        BlocksExt_ = New<NIO::TBlocksExt>(GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions()));
                        if (BlocksExtCache_) {
                            BlocksExtCache_->Put(meta, BlocksExt_);
                        }
                        ChunkFragmentReadsPrepared_[directIOFlag].store(true);
                    }));
            }))
            .ToUncancelable();

        chunkFragmentPromise.SetFrom(chunkFragmentFuture);
    } else {
        chunkFragmentFuture = ChunkFragmentReadsPreparedFuture_[directIOFlag];
    }
    return chunkFragmentFuture;
}

TReadRequest TChunkFileReader::MakeChunkFragmentReadRequest(
    const TChunkFragmentDescriptor& fragmentDescriptor,
    bool useDirectIO)
{
    auto directIOFlag = GetDirectIOFlag(useDirectIO);
    if (!ChunkFragmentReadsPrepared_[directIOFlag].load()) {
        directIOFlag = directIOFlag == EDirectIOFlag::On ? EDirectIOFlag::Off : EDirectIOFlag::On;
    }

    YT_ASSERT(ChunkFragmentReadsPrepared_[directIOFlag].load());

    auto makeErrorAttributes = [&] {
        return std::vector{
            TErrorAttribute("chunk_id", ChunkLayoutReader_->GetChunkId()),
            TErrorAttribute("block_index", fragmentDescriptor.BlockIndex),
            TErrorAttribute("block_offset", fragmentDescriptor.BlockOffset),
            TErrorAttribute("length", fragmentDescriptor.Length)
        };
    };

    if (fragmentDescriptor.BlockIndex < 0 ||
        fragmentDescriptor.BlockIndex >= std::ssize(BlocksExt_->Blocks))
    {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MalformedReadRequest,
            "Invalid block index in fragment descriptor")
            << makeErrorAttributes()
            << TErrorAttribute("block_count", BlocksExt_->Blocks.size());
    }

    const auto& blockInfo = BlocksExt_->Blocks[fragmentDescriptor.BlockIndex];

    auto requestLength = fragmentDescriptor.Length;
    if (requestLength == WholeBlockFragmentRequestLength) {
        requestLength = blockInfo.Size;
    } else if (requestLength < 0) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MalformedReadRequest,
            "Negative length in fragment descriptor")
            << makeErrorAttributes();
    }

    if (fragmentDescriptor.BlockOffset < 0 ||
        fragmentDescriptor.BlockOffset + requestLength > blockInfo.Size)
    {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MalformedReadRequest,
            "Fragment is out of block range")
            << makeErrorAttributes()
            << TErrorAttribute("block_size", blockInfo.Size);
    }

    return TReadRequest{
        .Handle = DataFileHandle_[directIOFlag],
        .Offset = blockInfo.Offset + fragmentDescriptor.BlockOffset,
        .Size = requestLength,
    };
}

std::vector<TBlock> TChunkFileReader::OnBlocksRead(
    const TClientChunkReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    const NIO::TBlocksExtPtr& blocksExt,
    const TReadResponse& readResponse)
{
    YT_VERIFY(readResponse.OutputBuffers.size() == 1);
    const auto& buffer = readResponse.OutputBuffers[0];

    options.ChunkReaderStatistics->DataIORequests.fetch_add(
        readResponse.IORequests,
        std::memory_order::relaxed);

    return ChunkLayoutReader_->DeserializeBlocks(
        buffer,
        TPhysicalChunkLayoutReader::TBlockRange{
            .StartBlockIndex = firstBlockIndex,
            .EndBlockIndex = firstBlockIndex + blockCount,
        },
        blocksExt,
        options.ChunkReaderStatistics);
}

TFuture<std::vector<TBlock>> TChunkFileReader::DoReadBlocks(
    const TClientChunkReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    TFairShareSlotId fairShareSlotId,
    NIO::TBlocksExtPtr blocksExt,
    TIOEngineHandlePtr dataFile)
{
    if (blockCount == 0) {
        return MakeFuture(std::vector<TBlock>());
    }
    if (!blocksExt && BlocksExtCache_) {
        blocksExt = BlocksExtCache_->Find();
    }

    if (!blocksExt) {
        return DoReadMeta(options, /*partitionTags*/ {}, fairShareSlotId)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
                auto loadedBlocksExt = New<NIO::TBlocksExt>(GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions()));
                if (BlocksExtCache_) {
                    BlocksExtCache_->Put(meta, loadedBlocksExt);
                }
                return DoReadBlocks(options, firstBlockIndex, blockCount, fairShareSlotId, loadedBlocksExt, dataFile);
            }))
            .ToUncancelable();
    }

    if (!dataFile) {
        auto asyncDataFile = OpenDataFile(GetDirectIOFlag(/*useDirectIO*/ false));
        auto optionalDataFileOrError = asyncDataFile.TryGet();
        if (!optionalDataFileOrError || !optionalDataFileOrError->IsOK()) {
            return asyncDataFile
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& dataFile) {
                    return DoReadBlocks(
                        options,
                        firstBlockIndex,
                        blockCount,
                        fairShareSlotId,
                        blocksExt,
                        dataFile);
                }));
        }
        dataFile = optionalDataFileOrError->Value();
    }

    int chunkBlockCount = std::ssize(blocksExt->Blocks);
    if (firstBlockIndex + blockCount > chunkBlockCount) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MalformedReadRequest,
            "Requested to read blocks [%v,%v] from chunk %v while only %v blocks exist",
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            ChunkLayoutReader_->GetChunkFileName(),
            chunkBlockCount);
    }

    // Read all blocks within a single request.
    int lastBlockIndex = firstBlockIndex + blockCount - 1;
    const auto& firstBlockInfo = blocksExt->Blocks[firstBlockIndex];
    const auto& lastBlockInfo = blocksExt->Blocks[lastBlockIndex];
    i64 totalSize = lastBlockInfo.Offset + lastBlockInfo.Size - firstBlockInfo.Offset;

    struct TChunkFileReaderBufferTag
    { };
    return IOEngine_->Read({{
            dataFile,
            firstBlockInfo.Offset,
            totalSize,
            fairShareSlotId,
        }},
        options.WorkloadDescriptor.Category,
        GetRefCountedTypeCookie<TChunkFileReaderBufferTag>(),
        options.ReadSessionId,
        options.UseDedicatedAllocations)
        .Apply(BIND(&TChunkFileReader::OnBlocksRead, MakeStrong(this), options, firstBlockIndex, blockCount, blocksExt)
            .AsyncVia(IOEngine_->GetAuxPoolInvoker()));
}

TFuture<TRefCountedChunkMetaPtr> TChunkFileReader::DoReadMeta(
    const TClientChunkReadOptions& options,
    const std::optional<TPartitionTags>& partitionTags,
    TFairShareSlotId fairShareSlotId)
{
    // Partition tag filtering not implemented here
    // because there is no practical need.
    // Implement when necessary.
    YT_VERIFY(!partitionTags);

    const auto& metaFileName = ChunkLayoutReader_->GetChunkMetaFileName();

    YT_LOG_DEBUG("Started reading chunk meta file (FileName: %v)",
        metaFileName);

    return IOEngine_->ReadAll(
        metaFileName,
        options.WorkloadDescriptor.Category,
        options.ReadSessionId,
        fairShareSlotId)
        .Apply(BIND(&TChunkFileReader::OnMetaRead, MakeStrong(this), metaFileName, options.ChunkReaderStatistics)
            .AsyncVia(IOEngine_->GetAuxPoolInvoker()));
}

TRefCountedChunkMetaPtr TChunkFileReader::OnMetaRead(
    const TString& metaFileName,
    TChunkReaderStatisticsPtr chunkReaderStatistics,
    const TReadResponse& readResponse)
{
    YT_VERIFY(readResponse.OutputBuffers.size() == 1);
    const auto& metaFileBlob = readResponse.OutputBuffers[0];

    YT_LOG_DEBUG("Finished reading chunk meta file (FileName: %v)",
        metaFileName);

    auto meta = ChunkLayoutReader_->DeserializeMeta(metaFileBlob, chunkReaderStatistics);
    chunkReaderStatistics->MetaIORequests.fetch_add(
        readResponse.IORequests,
        std::memory_order::relaxed);

    return meta.ChunkMeta;
}

TFuture<TIOEngineHandlePtr> TChunkFileReader::OpenDataFile(EDirectIOFlag useDirectIO)
{
    auto guard = Guard(DataFileHandleLock_);
    auto& fileHandleFuture = DataFileHandleFuture_[useDirectIO];

    if (fileHandleFuture && fileHandleFuture.IsSet() &&
        !fileHandleFuture.Get().IsOK())
    {
        // Seems like on the previous attempt we failed to open the file for some reason.
        // Let us try again.
        fileHandleFuture.Reset();
    }

    const auto& fileName = ChunkLayoutReader_->GetChunkFileName();
    if (!fileHandleFuture) {
        YT_LOG_DEBUG("Started opening chunk data file (FileName: %v, DirectIO: %v)",
            fileName,
            useDirectIO);

        EOpenMode mode = OpenExisting | RdOnly | CloseOnExec;
        if (useDirectIO == EDirectIOFlag::On) {
            TIOEngineHandle::MarkOpenForDirectIO(&mode);
        }
        fileHandleFuture = IOEngine_->Open({fileName, mode})
            .Apply(BIND(&TChunkFileReader::OnDataFileOpened, MakeStrong(this), useDirectIO)
            .AsyncVia(IOEngine_->GetAuxPoolInvoker()))
            .ToUncancelable();
    }
    return fileHandleFuture;
}

TIOEngineHandlePtr TChunkFileReader::OnDataFileOpened(EDirectIOFlag useDirectIO, const TIOEngineHandlePtr& file)
{
    YT_LOG_DEBUG("Finished opening chunk data file (FileName: %v, Handle: %v, DirectIO: %v)",
        ChunkLayoutReader_->GetChunkFileName(),
        static_cast<FHANDLE>(*file),
        useDirectIO);

    DataFileHandle_[useDirectIO] = file;

    return file;
}

EDirectIOFlag TChunkFileReader::GetDirectIOFlag(bool useDirectIO)
{
    auto policy = IOEngine_->UseDirectIOForReads();
    if (policy == EDirectIOPolicy::Never) {
        return EDirectIOFlag::Off;
    } else if (policy == EDirectIOPolicy::Always) {
        return EDirectIOFlag::On;
    } else {
        return useDirectIO ? EDirectIOFlag::On : EDirectIOFlag::Off;
    }
}

void TChunkFileReader::DumpBrokenMeta(TRef block) const
{
    auto fileName = ChunkLayoutReader_->GetChunkFileName() + ".broken.meta";
    TFile file(fileName, CreateAlways | WrOnly);
    file.Write(block.Begin(), block.Size());
    file.Flush();
}

void TChunkFileReader::DumpBrokenBlock(
    int blockIndex,
    const NIO::TBlockInfo& blockInfo,
    TRef block) const
{
    auto fileName = Format("%v.broken.%v.%v.%v.%v",
        ChunkLayoutReader_->GetChunkFileName(),
        blockIndex,
        blockInfo.Offset,
        blockInfo.Size,
        blockInfo.Checksum);

    TFile file(fileName, CreateAlways | WrOnly);
    file.Write(block.Begin(), block.Size());
    file.Flush();
}

////////////////////////////////////////////////////////////////////////////////

TBlocksExt::TBlocksExt(const NChunkClient::NProto::TBlocksExt& message)
{
    Blocks.reserve(message.blocks_size());
    for (const auto& blockInfo : message.blocks()) {
        Blocks.push_back(TBlockInfo{
            .Offset = blockInfo.offset(),
            .Size = blockInfo.size(),
            .Checksum = blockInfo.checksum(),
        });
    }

    SyncOnClose = message.sync_on_close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
