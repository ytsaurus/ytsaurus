#include "chunk_file_reader.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/ytalloc/memory_zone.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NIO {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NYTAlloc;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = IOLogger;

////////////////////////////////////////////////////////////////////////////////

struct TChunkFileReaderDataBufferTag
{ };

struct TChunkFileReaderMetaBufferTag
{ };

namespace {

template <class T>
void ReadHeader(
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

TChunkFileReader::TChunkFileReader(
    IIOEnginePtr ioEngine,
    TChunkId chunkId,
    TString fileName,
    bool validateBlocksChecksums,
    bool useDirectIO,
    IBlocksExtCache* blocksExtCache)
    : IOEngine_(std::move(ioEngine))
    , ChunkId_(chunkId)
    , FileName_(std::move(fileName))
    , ValidateBlockChecksums_(validateBlocksChecksums)
    , UseDirectIO_(useDirectIO)
    , BlocksExtCache_(std::move(blocksExtCache))
{ }

TFuture<std::vector<TBlock>> TChunkFileReader::ReadBlocks(
    const TClientChunkReadOptions& options,
    const std::vector<int>& blockIndexes,
    std::optional<i64> /* estimatedSize */)
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
            auto subfuture = DoReadBlocks(options, startBlockIndex, blockCount);
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
    std::optional<i64> /* estimatedSize */)
{
    YT_VERIFY(firstBlockIndex >= 0);

    try {
        return DoReadBlocks(options, firstBlockIndex, blockCount);
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TBlock>>(ex);
    }
}

TFuture<TRefCountedChunkMetaPtr> TChunkFileReader::GetMeta(
    const TClientChunkReadOptions& options,
    std::optional<int> partitionTag)
{
    try {
        return DoReadMeta(options, partitionTag);
    } catch (const std::exception& ex) {
        return MakeFuture<TRefCountedChunkMetaPtr>(ex);
    }
}

TChunkId TChunkFileReader::GetChunkId() const
{
    return ChunkId_;
}

TFuture<void> TChunkFileReader::PrepareToReadChunkFragments()
{
    // Fast path.
    if (DataFileOpened_.load()) {
        return {};
    }

    // Slow path.
    return OpenDataFile().AsVoid();
}

IIOEngine::TReadRequest TChunkFileReader::MakeChunkFragmentReadRequest(
    const TChunkFragmentDescriptor& fragmentDescriptor)
{
    YT_ASSERT(DataFileOpened_.load());

    return IIOEngine::TReadRequest{
        DataFile_,
        fragmentDescriptor.Offset,
        fragmentDescriptor.Length
    };
}

std::vector<TBlock> TChunkFileReader::OnBlocksRead(
    const TClientChunkReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    const TRefCountedBlocksExtPtr& blocksExt,
    const std::vector<TSharedRef>& readResult)
{
    YT_VERIFY(readResult.size() == 1);
    const auto& buffer = readResult[0];

    options.ChunkReaderStatistics->DataBytesReadFromDisk += buffer.Size();
    const auto& firstBlockInfo = blocksExt->blocks(firstBlockIndex);

    std::vector<TBlock> blocks;
    blocks.reserve(blockCount);

    for (int localIndex = 0; localIndex < blockCount; ++localIndex) {
        int blockIndex = firstBlockIndex + localIndex;
        const auto& blockInfo = blocksExt->blocks(blockIndex);
        auto block = buffer.Slice(
            blockInfo.offset() - firstBlockInfo.offset(),
            blockInfo.offset() - firstBlockInfo.offset() + blockInfo.size());
        if (ValidateBlockChecksums_) {
            auto checksum = GetChecksum(block);
            if (checksum != blockInfo.checksum()) {
                DumpBrokenBlock(blockIndex, blockInfo, block);
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::IncorrectChunkFileChecksum,
                    "Incorrect checksum of block %v in chunk data file %v: expected %v, actual %v",
                    blockIndex,
                    FileName_,
                    blockInfo.checksum(),
                    checksum)
                    << TErrorAttribute("first_block_index", firstBlockIndex)
                    << TErrorAttribute("block_count", blockCount);
            }
        }
        blocks.push_back(TBlock(block, blockInfo.checksum()));
        blocks.back().BlockOrigin = EBlockOrigin::Disk;
    }

    return blocks;
}

TFuture<std::vector<TBlock>> TChunkFileReader::DoReadBlocks(
    const TClientChunkReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    TRefCountedBlocksExtPtr blocksExt,
    TIOEngineHandlePtr dataFile)
{
    if (!blocksExt && BlocksExtCache_) {
        blocksExt = BlocksExtCache_->Find();
    }

    if (!blocksExt) {
        return DoReadMeta(options, std::nullopt)
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
                auto loadedBlocksExt = New<TRefCountedBlocksExt>(GetProtoExtension<TBlocksExt>(meta->extensions()));
                if (BlocksExtCache_) {
                    BlocksExtCache_->Put(meta, loadedBlocksExt);
                }
                return DoReadBlocks(options, firstBlockIndex, blockCount, loadedBlocksExt, dataFile);
            }))
            .ToUncancelable();
    }

    if (!dataFile) {
        auto asyncDataFile = OpenDataFile();
        auto optionalDataFileOrError = asyncDataFile.TryGet();
        if (!optionalDataFileOrError || !optionalDataFileOrError->IsOK()) {
            return asyncDataFile
                .Apply(BIND([=, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& dataFile) {
                    return DoReadBlocks(options, firstBlockIndex, blockCount, blocksExt, dataFile);
                }));
        }
        dataFile = optionalDataFileOrError->Value();
    }

    int chunkBlockCount = blocksExt->blocks_size();
    if (firstBlockIndex + blockCount > chunkBlockCount) {
        THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::BlockOutOfRange,
            "Requested to read blocks [%v,%v] from chunk %v while only %v blocks exist",
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            FileName_,
            chunkBlockCount);
    }

    // Read all blocks within a single request.
    int lastBlockIndex = firstBlockIndex + blockCount - 1;
    const auto& firstBlockInfo = blocksExt->blocks(firstBlockIndex);
    const auto& lastBlockInfo = blocksExt->blocks(lastBlockIndex);
    i64 totalSize = lastBlockInfo.offset() + lastBlockInfo.size() - firstBlockInfo.offset();

    struct TChunkFileReaderBufferTag
    { };
    return IOEngine_->Read<TChunkFileReaderBufferTag>({{
            dataFile,
            firstBlockInfo.offset(),
            totalSize
        }},
        options.WorkloadDescriptor.GetPriority(),
        EMemoryZone::Undumpable)
        .Apply(BIND(&TChunkFileReader::OnBlocksRead, MakeStrong(this), options, firstBlockIndex, blockCount, blocksExt));
}

TFuture<TRefCountedChunkMetaPtr> TChunkFileReader::DoReadMeta(
    const TClientChunkReadOptions& options,
    std::optional<int> partitionTag)
{
    // Partition tag filtering not implemented here
    // because there is no practical need.
    // Implement when necessary.
    YT_VERIFY(!partitionTag);

    auto metaFileName = FileName_ + ChunkMetaSuffix;

    YT_LOG_DEBUG("Started reading chunk meta file (FileName: %v)",
        metaFileName);

    return IOEngine_->ReadAll(metaFileName)
        .Apply(BIND(&TChunkFileReader::OnMetaRead, MakeStrong(this), metaFileName, options.ChunkReaderStatistics));
}

TRefCountedChunkMetaPtr TChunkFileReader::OnMetaRead(
    const TString& metaFileName,
    TChunkReaderStatisticsPtr chunkReaderStatistics,
    const TSharedRef& metaFileBlob)
{
    YT_LOG_DEBUG("Finished reading chunk meta file (FileName: %v)",
        FileName_);

    if (metaFileBlob.Size() < sizeof (TChunkMetaHeaderBase)) {
        THROW_ERROR_EXCEPTION("Chunk meta file %v is too short: at least %v bytes expected",
            metaFileName,
            sizeof (TChunkMetaHeaderBase));
    }

    chunkReaderStatistics->MetaBytesReadFromDisk += metaFileBlob.Size();

    TChunkMetaHeader_2 metaHeader;
    TRef metaBlob;
    const auto* metaHeaderBase = reinterpret_cast<const TChunkMetaHeaderBase*>(metaFileBlob.Begin());

    switch (metaHeaderBase->Signature) {
        case TChunkMetaHeader_1::ExpectedSignature:
            ReadHeader<TChunkMetaHeader_1>(metaFileBlob, metaFileName, &metaHeader, &metaBlob);
            metaHeader.ChunkId = ChunkId_;
            break;

        case TChunkMetaHeader_2::ExpectedSignature:
            ReadHeader<TChunkMetaHeader_2>(metaFileBlob, metaFileName, &metaHeader, &metaBlob);
            break;

        default:
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::IncorrectChunkFileHeaderSignature,
                "Incorrect header signature %llx in chunk meta file %v",
                metaHeaderBase->Signature,
                metaFileName);
    }

    auto checksum = GetChecksum(metaBlob);
    if (checksum != metaHeader.Checksum) {
        DumpBrokenMeta(metaBlob);
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::IncorrectChunkFileChecksum,
            "Incorrect checksum in chunk meta file %v: expected %v, actual %v",
            metaFileName,
            metaHeader.Checksum,
            checksum)
            << TErrorAttribute("meta_file_length", metaFileBlob.Size());
    }

    if (ChunkId_ != NullChunkId && metaHeader.ChunkId != ChunkId_) {
        THROW_ERROR_EXCEPTION("Invalid chunk id in meta file %v: expected %v, actual %v",
            metaFileName,
            ChunkId_,
            metaHeader.ChunkId);
    }

    TChunkMeta meta;
    if (!TryDeserializeProtoWithEnvelope(&meta, metaBlob)) {
        THROW_ERROR_EXCEPTION("Failed to parse chunk meta file %v",
            metaFileName);
    }

    return New<TRefCountedChunkMeta>(std::move(meta));
}

TFuture<TIOEngineHandlePtr> TChunkFileReader::OpenDataFile()
{
    auto guard = Guard(DataFileLock_);
    if (!DataFileFuture_) {
        YT_LOG_DEBUG("Started opening chunk data file (FileName: %v)",
            FileName_);

        EOpenMode mode = OpenExisting | RdOnly | CloseOnExec;
        if (UseDirectIO_) {
            mode |= DirectAligned;
        }
        DataFileFuture_ = IOEngine_->Open({FileName_, mode})
            .ToUncancelable()
            .Apply(BIND(&TChunkFileReader::OnDataFileOpened, MakeStrong(this)));
    }
    return DataFileFuture_;
}

TIOEngineHandlePtr TChunkFileReader::OnDataFileOpened(const TIOEngineHandlePtr& file)
{
    YT_LOG_DEBUG("Finished opening chunk data file (FileName: %v, Handle: %v)",
        FileName_,
        static_cast<FHANDLE>(*file));

    DataFile_ = file;
    DataFileOpened_.store(true);

    return file;
}

void TChunkFileReader::DumpBrokenMeta(TRef block) const
{
    auto fileName = FileName_ + ".broken.meta";
    TFile file(fileName, CreateAlways | WrOnly);
    file.Write(block.Begin(), block.Size());
    file.Flush();
}

void TChunkFileReader::DumpBrokenBlock(
    int blockIndex,
    const TBlockInfo& blockInfo,
    TRef block) const
{
    auto fileName = Format("%v.broken.%v.%v.%v.%v",
        FileName_,
        blockIndex,
        blockInfo.offset(),
        blockInfo.size(),
        blockInfo.checksum());

    TFile file(fileName, CreateAlways | WrOnly);
    file.Write(block.Begin(), block.Size());
    file.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
