#pragma once

#include "async_reader.h"
#include "format.h"
#include <ytlib/chunk_client/chunk.pb.h>

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of IAsyncReader.
class TFileReader
    : public IAsyncReader
{
public:
    //! Creates a new reader.
    explicit TFileReader(const Stroka& fileName);

    //! Opens the files, reads chunk meta. Must be called before reading blocks.
    void Open();

    //! Returns the meta file size.
    i64 GetMetaSize() const;

    //! Returns the data file size.
    i64 GetDataSize() const;

    //! Returns the full chunk size.
    i64 GetFullSize() const;

    NChunkClient::NProto::TChunkMeta GetChunkMeta(const std::vector<int>* tags = NULL) const;

    //! Implements IChunkReader and calls #ReadBlock.
    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes);

    //! Implements IChunkReader and calls #GetChunkMeta.
    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag,
        const std::vector<int>* tags = NULL);

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
    NChunkClient::NProto::TChunkMeta ChunkMeta;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
