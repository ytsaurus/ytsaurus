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

    //! Opens the files, reads chunk info. Must be called before reading blocks.
    void Open();

    //! Returns the info file size.
    i64 GetInfoSize() const;

    //! Returns the data file size.
    i64 GetDataSize() const;

    //! Returns the full chunk size.
    i64 GetFullSize() const;

    //! Returns the typed chunk info.
    const NChunkServer::NProto::TChunkInfo& GetChunkInfo() const;

    //! Implements IChunkReader and calls #ReadBlock.
    virtual TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);

    //! Synchronously reads a given block from the file.
    /*!
     *  Returns NULL reference if the block does not exist.
     */
    TSharedRef ReadBlock(int blockIndex);

private:
    Stroka FileName;
    bool Opened;
    THolder<TFile> DataFile;
    i64 InfoSize;
    i64 DataSize;
    NChunkServer::NProto::TChunkInfo ChunkInfo;

};


///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
