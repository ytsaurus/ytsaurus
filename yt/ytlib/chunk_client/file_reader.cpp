#include "stdafx.h"
#include "file_reader.h"
#include "chunk_meta_extensions.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/misc/fs.h>
#include <ytlib/misc/assert.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(const Stroka& fileName)
    : FileName(fileName)
    , Opened(false)
    , InfoSize(-1)
    , DataSize(-1)
{ }

void TFileReader::Open()
{
    YASSERT(!Opened);

    Stroka chunkMetaFileName = FileName + ChunkMetaSuffix;
    TFile chunkMetaFile(
        chunkMetaFileName,
        OpenExisting | RdOnly | Seq | CloseOnExec);

    InfoSize = chunkMetaFile.GetLength();
    TBufferedFileInput chunkMetaInput(chunkMetaFile);

    TChunkMetaHeader metaHeader;
    ReadPod(chunkMetaInput, metaHeader);
    if (metaHeader.Signature != TChunkMetaHeader::ExpectedSignature) {
        THROW_ERROR_EXCEPTION("Incorrect signature in chunk meta header (FileName: %s, Expected: %" PRIx64 ", Found: %" PRIx64")",
            ~FileName,
            TChunkMetaHeader::ExpectedSignature,
            metaHeader.Signature);
    }

    Stroka chunkMetaBlob = chunkMetaInput.ReadAll();
    TRef chunkMetaRef(chunkMetaBlob.begin(), chunkMetaBlob.size());

    auto checksum = GetChecksum(chunkMetaRef);
    if (checksum != metaHeader.Checksum) {
        THROW_ERROR_EXCEPTION("Incorrect checksum in chunk meta file (FileName: %s, Expected: %" PRIx64 ", Found: %" PRIx64")",
                ~FileName,
                metaHeader.Checksum,
                checksum);
    }

    if (!DeserializeFromProtoWithEnvelope(&ChunkMeta, chunkMetaRef)) {
        THROW_ERROR_EXCEPTION("Failed to parse chunk meta (FileName: %s)",
            ~FileName); 
    }

    DataFile.Reset(new TFile(FileName, OpenExisting | RdOnly | CloseOnExec));
    Opened = true;
}

TFuture<IAsyncReader::TReadResult>
TFileReader::AsyncReadBlocks(const std::vector<int>& blockIndexes)
{
    YASSERT(Opened);

    std::vector<TSharedRef> blocks;
    blocks.reserve(blockIndexes.size());

    for (int index = 0; index < blockIndexes.size(); ++index) {
        i32 blockIndex = blockIndexes[index];
        blocks.push_back(ReadBlock(blockIndex));
    }

    return MakeFuture(TReadResult(MoveRV(blocks)));
}

TSharedRef TFileReader::ReadBlock(int blockIndex)
{
    YASSERT(Opened);

    auto blocksExt = GetProtoExtension<TBlocksExt>(ChunkMeta.extensions());
    i32 blockCount = blocksExt.blocks_size();

    if (blockIndex > blockCount || blockIndex < -blockCount) {
        return TSharedRef();
    }

    while (blockIndex < 0) {
        blockIndex += blockCount;
    }

    if (blockIndex >= blockCount) {
        return TSharedRef();
    }

    const auto& blockInfo = blocksExt.blocks(blockIndex);
    TSharedRef data(blockInfo.size());
    i64 offset = blockInfo.offset();
    DataFile->Pread(data.Begin(), data.Size(), offset);

    auto checksum = GetChecksum(data);
    if (checksum != blockInfo.checksum()) {
        THROW_ERROR_EXCEPTION("Incorrect checksum in chunk block (FileName: %s, BlockIndex: %d, Expected: %" PRIx64 ", Found: %" PRIx64 ")",
            ~FileName,
            blockIndex,
            blockInfo.checksum(),
            checksum);
    }

    return data;
}

i64 TFileReader::GetMetaSize() const
{
    YASSERT(Opened);
    return InfoSize;
}

i64 TFileReader::GetDataSize() const
{
    YASSERT(Opened);
    return DataSize;
}

i64 TFileReader::GetFullSize() const
{
    YASSERT(Opened);
    return InfoSize + DataSize;
}

TChunkMeta TFileReader::GetChunkMeta(const std::vector<int>* tags) const
{
    YASSERT(Opened);
    return tags ? FilterChunkMetaExtensions(ChunkMeta, *tags) : ChunkMeta;
}

IAsyncReader::TAsyncGetMetaResult
TFileReader::AsyncGetChunkMeta(
    const TNullable<int>& partitionTag, 
    const std::vector<int>* tags)
{
    // Partition tag filtering not implemented here
    // because there is no practical need.
    // Implement when necessary.
    YCHECK(!partitionTag.IsInitialized());
    return MakeFuture(TGetMetaResult(GetChunkMeta(tags)));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
