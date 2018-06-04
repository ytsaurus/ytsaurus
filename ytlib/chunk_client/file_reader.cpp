#include "file_reader.h"
#include "chunk_meta_extensions.h"
#include "format.h"
#include "chunk_reader_statistics.h"

#include <yt/core/misc/fs.h>
#include <yt/core/misc/checksum.h>

#include <yt/ytlib/misc/workload.h>

namespace NYT {
namespace NChunkClient {

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
    const TChunkId& chunkId,
    const TString& fileName,
    bool validateBlocksChecksums)
    : IOEngine_(ioEngine)
    , ChunkId_(chunkId)
    , FileName_(fileName)
    , ValidateBlockChecksums_(validateBlocksChecksums)
{ }

TFuture<std::vector<TBlock>> TFileReader::ReadBlocks(
    const TClientBlockReadOptions& options,
    const std::vector<int>& blockIndexes)
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
    int blockCount)
{
    YCHECK(firstBlockIndex >= 0);

    try {
        return DoReadBlocks(options, firstBlockIndex, blockCount);
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TBlock>>(ex);
    }
}

TFuture<TChunkMeta> TFileReader::GetMeta(
    const TClientBlockReadOptions& options,
    const TNullable<int>& partitionTag,
    const TNullable<std::vector<int>>& extensionTags)
{
    try {
        return DoGetMeta(options, partitionTag, extensionTags);
    } catch (const std::exception& ex) {
        return MakeFuture<TChunkMeta>(ex);
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
    const TSharedMutableRef& data)
{
    options.ChunkReaderStatistics->DataBytesReadFromDisk += data.Size();

    // Slice the result; validate checksums.
    YCHECK(HasCachedBlocksExt_ && CachedBlocksExt_.IsSet());

    const auto& blockExts = CachedBlocksExt_.Get().ValueOrThrow();
    const auto& firstBlockInfo = blockExts.blocks(firstBlockIndex);

    std::vector<TBlock> blocks;
    blocks.reserve(blockCount);

    for (int localIndex = 0; localIndex < blockCount; ++localIndex) {
        int blockIndex = firstBlockIndex + localIndex;
        const auto& blockInfo = blockExts.blocks(blockIndex);
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
    int blockCount)
{
    const auto& blockExts = GetBlockExts(options);
    int chunkBlockCount = blockExts.blocks_size();
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
    const auto& firstBlockInfo = blockExts.blocks(firstBlockIndex);
    const auto& lastBlockInfo = blockExts.blocks(lastBlockIndex);
    i64 totalSize = lastBlockInfo.offset() + lastBlockInfo.size() - firstBlockInfo.offset();

    const auto& file = GetDataFile();

    return IOEngine_->Pread(file, totalSize, firstBlockInfo.offset(), options.WorkloadDescriptor.GetPriority())
        .Apply(BIND(&TFileReader::OnDataBlock, MakeStrong(this), options, firstBlockIndex, blockCount));
}

void TFileReader::DumpBrokenMeta(const TRef& block) const
{
    auto fileName = FileName_ + ".broken.meta";
    TFile file(fileName, CreateAlways | WrOnly);
    file.Write(block.Begin(), block.Size());
    file.Flush();
}

NProto::TChunkMeta TFileReader::OnMetaDataBlock(    
    const TString& metaFileName,
    i64 metaFileLength,
    TChunkReaderStatisticsPtr chunkReaderStatistics,
    const TSharedMutableRef& metaFileBlob)
{
    chunkReaderStatistics->MetaBytesReadFromDisk += metaFileLength;

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
            << TErrorAttribute("meta_file_length", metaFileLength);
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

    return meta;
}

TFuture<TChunkMeta> TFileReader::DoGetMeta(
    const TClientBlockReadOptions& options,
    const TNullable<int>& partitionTag,
    const TNullable<std::vector<int>>& extensionTags)
{
    // Partition tag filtering not implemented here
    // because there is no practical need.
    // Implement when necessary.
    YCHECK(!partitionTag);

    auto metaFileName = FileName_ + ChunkMetaSuffix;
    auto chunkReaderStatistics = options.ChunkReaderStatistics;

    return IOEngine_->Open(
        metaFileName,
        OpenExisting | RdOnly | Seq | CloseOnExec).Apply(
            BIND([this, metaFileName, chunkReaderStatistics, this_ = MakeStrong(this)] (const std::shared_ptr<TFileHandle>& metaFile) {
                if (metaFile->GetLength() < sizeof (TChunkMetaHeaderBase)) {
                    THROW_ERROR_EXCEPTION("Chunk meta file %v is too short: at least %v bytes expected",
                        FileName_,
                        sizeof (TChunkMetaHeaderBase));
                }

                return IOEngine_->Pread(metaFile, metaFile->GetLength(), 0)
                        .Apply(BIND(&TFileReader::OnMetaDataBlock, MakeStrong(this), metaFileName, metaFile->GetLength(), chunkReaderStatistics));
            }));
}

const TBlocksExt& TFileReader::GetBlockExts(const TClientBlockReadOptions& options)
{
    if (!HasCachedBlocksExt_) {
        TGuard<TMutex> guard(Mutex_);
        if (!CachedBlocksExt_) {
            CachedBlocksExt_ = DoGetMeta(options, Null, Null).Apply(BIND([](const TChunkMeta& meta) {
                return GetProtoExtension<TBlocksExt>(meta.extensions());
            })).ToUncancelable();
            HasCachedBlocksExt_ = true;
        }
    }
    // TODO(aozeritsky) move this check to WaitFor
    if (!CachedBlocksExt_.IsSet()) {
        Y_UNUSED(WaitFor(CachedBlocksExt_));
    }
    return CachedBlocksExt_.Get().ValueOrThrow();
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

} // namespace NChunkClient
} // namespace NYT
