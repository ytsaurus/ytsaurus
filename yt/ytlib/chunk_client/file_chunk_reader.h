#pragma once

#include "chunk_reader.h"
#include "format.h"

#include <util/system/file.h>

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

// TODO: move impl to cpp
//! Provides a local and synchronous implementation of IChunkReader.
class TFileChunkReader
    : public IChunkReader
{
public:
    typedef TIntrusivePtr<TFileChunkReader> TPtr;

    TFileChunkReader(Stroka fileName)
        : FileName(fileName)
    {
        File.Reset(new TFile(fileName, OpenExisting|RdOnly));

        TChunkFooter footer;
        File->Seek(sizeof (TChunkFooter), sEnd);
        File->Read(&footer, sizeof (footer));

        if (footer.Singature != TChunkFooter::ExpectedSignature) {
            ythrow yexception() <<
                Sprintf("Chunk footer signature mismatch in %s", ~fileName.Quote());
        }

        YASSERT(footer.BlockCount >= 0);
        YASSERT(footer.BlockInfoOffset >= 0);

        BlockInfos.resize(footer.BlockCount);

        TBlockInfo* infoBegin = &*BlockInfos.begin();
        TBlockInfo* infoEnd = &*BlockInfos.end();

        // Check alignment.
        YASSERT(
            static_cast<i64>(reinterpret_cast<char*>(infoEnd) - reinterpret_cast<char*>(infoBegin)) ==
            static_cast<i64>(footer.BlockCount * sizeof (TBlockInfo)));

        File->Seek(footer.BlockInfoOffset, sSet);
        File->Read(infoBegin, footer.BlockCount * sizeof (TBlockInfo));

        TBlockOffset currentOffset = 0;
        BlockOffsets.reserve(footer.BlockCount);
        for (int blockIndex = 0; blockIndex < footer.BlockCount; ++blockIndex) {
            BlockOffsets.push_back(currentOffset);
            currentOffset += BlockInfos[blockIndex].Size;
        }
    }

    //! Returns the number of blocks in the chunk.
    i32 GetBlockCount() const
    {
        return BlockInfos.ysize();
    }

    //! Implements IChunkReader and calls #ReadBlock.
    virtual TAsyncResult<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes)
    {
        TReadResult result;
        result.Blocks.reserve(blockIndexes.ysize());

        for (int index = 0; index < blockIndexes.ysize(); ++index) {
            i32 blockIndex = blockIndexes[index];
            result.Blocks.push_back(ReadBlock(blockIndex));
        }
        
        return new TAsyncResult<TReadResult>(result);
    }

    TSharedRef ReadBlock(int blockIndex)
    {
        i32 blockCount = GetBlockCount();

        if (blockIndex > blockCount || blockIndex < -blockCount) {
            return TSharedRef();
        }

        if (blockIndex < 0) {
            blockIndex += blockCount;
        }

        YASSERT(blockIndex >= 0 && blockIndex < blockCount);
        const TBlockInfo& blockInfo = BlockInfos[blockIndex];

        TBlob data(blockInfo.Size);
        File->Pread(data.begin(), data.size(), BlockOffsets[blockIndex]); 

        TSharedRef result(data);

        if (blockInfo.Checksum != GetChecksum(result)) {
            ythrow yexception() << Sprintf("Chunk footer signature mismatch in %s (BlockIndex: %d)",
                ~FileName.Quote(),
                blockIndex);
        }

        return result;
    }


private:
    Stroka FileName;
    THolder<TFile> File;
    yvector<TBlockInfo> BlockInfos;
    yvector<TBlockOffset> BlockOffsets;

};


///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
