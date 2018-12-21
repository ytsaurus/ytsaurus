#include "file_reader.h"
#include "chunk_meta_extensions.h"
#include "format.h"
#include "chunk_reader_statistics.h"
#include "io_engine.h"

#include <yt/core/misc/fs.h>
#include <yt/core/misc/checksum.h>

#include <yt/client/misc/workload.h>

namespace NYT::NChunkClient {

using namespace NChunkClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TFileReaderDataBufferTag
{ };

struct TFileReaderMetaBufferTag
{ };

namespace {

template <class T>
void ReadHeader(
    const TSharedMutableRef& metaFileBlob,
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

TFileReader::TFileReader(
    const IIOEnginePtr& ioEngine,
    TChunkId chunkId,
    const TString& fileName,
    bool validateBlocksChecksums,
    IBlocksExtCache* blocksExtCache)
    : IOEngine_(ioEngine)
    , ChunkId_(chunkId)
    , FileName_(fileName)
    , ValidateBlockChecksums_(validateBlocksChecksums)
    , BlocksExtCache_(std::move(blocksExtCache))
{ }

TFuture<std::vector<TBlock>> TFileReader::ReadBlocks(
    const TClientBlockReadOptions& options,
    const std::vector<int>& blockIndexes,
    const std::optional<i64>& /* estimatedSize */)
{
    std::vector<TFuture<std::vector<TBlock>>> futures;
    auto count = blockIndexes.size();

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
            auto subfutures = DoReadBlocks(options, startBlockIndex, blockCount);
            futures.push_back(std::move(subfutures));

            localIndex = endLocalIndex;
        }
    } catch (const std::exception& ex) {

        for (auto& future : futures) {
            future.Cancel();
        }

        return MakeFuture<std::vector<TBlock>>(ex);
    }

    return CombineAll(std::move(futures))
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

TFuture<std::vector<TBlock>> TFileReader::ReadBlocks(
    const TClientBlockReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    const std::optional<i64>& /* estimatedSize */)
{
    YCHECK(firstBlockIndex >= 0);

    try {
        return DoReadBlocks(options, firstBlockIndex, blockCount);
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TBlock>>(ex);
    }
}

TFuture<TRefCountedChunkMetaPtr> TFileReader::GetMeta(
    const TClientBlockReadOptions& options,
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags)
{
    try {
        return DoReadMeta(options, partitionTag, extensionTags);
    } catch (const std::exception& ex) {
        return MakeFuture<TRefCountedChunkMetaPtr>(ex);
    }
}

TChunkId TFileReader::GetChunkId() const
{
    return ChunkId_;
}

bool TFileReader::IsValid() const
{
    return true;
}

void TFileReader::SetSlownessChecker(TCallback<TError(i64, TDuration)>)
{
    Y_UNIMPLEMENTED();
}

void TFileReader::DumpBrokenBlock(
    int blockIndex,
    const TBlockInfo& blockInfo,
    const TRef& block) const
{
    auto fileName = FileName_ + ".broken." +
        ToString(blockIndex) + "." +
        ToString(blockInfo.offset()) + "." +
        ToString(blockInfo.size()) + "." +
        ToString(blockInfo.checksum());

    TFile file(fileName, CreateAlways | WrOnly);
    file.Write(block.Begin(), block.Size());
    file.Flush();
}

std::vector<TBlock> TFileReader::OnDataBlock(
    const TClientBlockReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    const TRefCountedBlocksExtPtr& blocksExt,
    const TSharedMutableRef& data)
{
    options.ChunkReaderStatistics->DataBytesReadFromDisk += data.Size();
    const auto& firstBlockInfo = blocksExt->blocks(firstBlockIndex);

    std::vector<TBlock> blocks;
    blocks.reserve(blockCount);

    for (int localIndex = 0; localIndex < blockCount; ++localIndex) {
        int blockIndex = firstBlockIndex + localIndex;
        const auto& blockInfo = blocksExt->blocks(blockIndex);
        auto block = data.Slice(
            blockInfo.offset() - firstBlockInfo.offset(),
            blockInfo.offset() - firstBlockInfo.offset() + blockInfo.size());
        if (ValidateBlockChecksums_) {
            auto checksum = GetChecksum(block);
            if (checksum != blockInfo.checksum()) {
                DumpBrokenBlock(blockIndex, blockInfo, block);
                THROW_ERROR_EXCEPTION(
                    "Incorrect checksum of block %v in chunk data file %Qv: expected %v, actual %v",
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

TFuture<std::vector<TBlock>> TFileReader::DoReadBlocks(
    const TClientBlockReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    TRefCountedBlocksExtPtr blocksExt)
{
    if (!blocksExt && BlocksExtCache_) {
        blocksExt = BlocksExtCache_->Find();
    }

    if (!blocksExt) {
        return DoReadMeta(options, std::nullopt, std::nullopt)
            .ToUncancelable()
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
                auto loadedBlocksExt = New<TRefCountedBlocksExt>(GetProtoExtension<NProto::TBlocksExt>(meta->extensions()));
                if (BlocksExtCache_) {
                    BlocksExtCache_->Put(meta, loadedBlocksExt);
                }
                return DoReadBlocks(options, firstBlockIndex, blockCount, loadedBlocksExt);
            }));
    }

    int chunkBlockCount = blocksExt->blocks_size();
    if (firstBlockIndex + blockCount > chunkBlockCount) {
        THROW_ERROR_EXCEPTION(EErrorCode::BlockOutOfRange,
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

    const auto& file = GetDataFile();

    return IOEngine_->Pread(file, totalSize, firstBlockInfo.offset(), options.WorkloadDescriptor.GetPriority())
        .Apply(BIND(&TFileReader::OnDataBlock, MakeStrong(this), options, firstBlockIndex, blockCount, blocksExt));
}

void TFileReader::DumpBrokenMeta(const TRef& block) const
{
    auto fileName = FileName_ + ".broken.meta";
    TFile file(fileName, CreateAlways | WrOnly);
    file.Write(block.Begin(), block.Size());
    file.Flush();
}

TRefCountedChunkMetaPtr TFileReader::OnMetaDataBlock(
    const TString& metaFileName,
    TChunkReaderStatisticsPtr chunkReaderStatistics,
    const TSharedMutableRef& metaFileBlob)
{
    if (metaFileBlob.Size() < sizeof (TChunkMetaHeaderBase)) {
        THROW_ERROR_EXCEPTION("Chunk meta file %v is too short: at least %v bytes expected",
            FileName_,
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
            THROW_ERROR_EXCEPTION("Incorrect header signature %llx in chunk meta file %v",
                metaHeaderBase->Signature,
                FileName_);
    }

    auto checksum = GetChecksum(metaBlob);
    if (checksum != metaHeader.Checksum) {
        DumpBrokenMeta(metaBlob);
        THROW_ERROR_EXCEPTION("Incorrect checksum in chunk meta file %v: expected %v, actual %v",
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

TFuture<TRefCountedChunkMetaPtr> TFileReader::DoReadMeta(
    const TClientBlockReadOptions& options,
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags)
{
    // Partition tag filtering not implemented here
    // because there is no practical need.
    // Implement when necessary.
    YCHECK(!partitionTag);

    auto metaFileName = FileName_ + ChunkMetaSuffix;
    auto chunkReaderStatistics = options.ChunkReaderStatistics;

    return IOEngine_->ReadAll(metaFileName)
        .Apply(BIND(&TFileReader::OnMetaDataBlock, MakeStrong(this), metaFileName, chunkReaderStatistics));
}

const std::shared_ptr<TFileHandle>& TFileReader::GetDataFile()
{
    if (!HasCachedDataFile_) {
        TGuard<TMutex> guard(Mutex_);
        if (!CachedDataFile_) {
            CachedDataFile_ = IOEngine_->Open(FileName_, OpenExisting | RdOnly | CloseOnExec).ToUncancelable();
            HasCachedDataFile_ = true;
        }
    }
    // TODO(aozeritsky) move this check to WaitFor
    if (!CachedDataFile_.IsSet()) {
        Y_UNUSED(WaitFor(CachedDataFile_));
    }
    return CachedDataFile_.Get().ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
