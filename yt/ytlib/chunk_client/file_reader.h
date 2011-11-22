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

    //! Returns the full file size.
    i64 GetSize() const;

    //! Returns the number of blocks in the chunk.
    i32 GetBlockCount() const;

    //! Returns the master meta.
    TSharedRef GetMasterMeta() const;

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
    i64 Size;
    NChunkClient::NProto::TChunkMeta Meta;
    yvector<TChunkOffset> BlockOffsets;
    TSharedRef MasterMeta;

};


///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
