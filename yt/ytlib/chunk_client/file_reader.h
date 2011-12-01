#pragma once

#include "async_reader.h"
#include "format.h"
#include "chunk.pb.h"

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of IAsyncReader.
class TFileReader
    : public IAsyncReader
{
public:
    typedef TIntrusivePtr<TFileReader> TPtr;

    //! Creates a new reader.
    TFileReader(const Stroka& fileName);

    //! Opens the files, reads chunk info. Must call this before reading blocks.
    void Open();

    //! Implements IChunkReader and calls #ReadBlock.
    virtual TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);

    //! Synchronously reads a given block from the file.
    /*!
     *  Returns empty block if the block does not exist.
     */
    TSharedRef ReadBlock(int blockIndex);

private:
    Stroka FileName;
    THolder<TFile> DataFile;
    NProto::TChunkInfo ChunkInfo;
};


///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
