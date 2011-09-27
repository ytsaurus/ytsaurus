#pragma once

#include "chunk_reader.h"
#include "format.h"
#include "chunk.pb.h"

#include <util/system/file.h>

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of IChunkReader.
class TFileChunkReader
    : public IChunkReader
{
public:
    typedef TIntrusivePtr<TFileChunkReader> TPtr;

    //! Creates a new reader.
    TFileChunkReader(Stroka fileName);

    //! Returns the number of blocks in the chunk.
    i32 GetBlockCount() const;

    //! Implements IChunkReader and calls #ReadBlock.
    virtual TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);

    //! Synchronously reads a given block from the file.
    /*!
     *  Returns NULL is the block does not exist.
     */
    TSharedRef ReadBlock(int blockIndex);

private:
    Stroka FileName;
    THolder<TFile> File;
    NChunkClient::NProto::TChunkMeta Meta;
    yvector<TChunkOffset> BlockOffsets;

};


///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
