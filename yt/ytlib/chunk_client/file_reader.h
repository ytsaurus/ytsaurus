#pragma once

#include "async_reader.h"
#include "format.h"
#include <ytlib/chunk_holder/chunk.pb.h>

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
    TFileReader(const Stroka& fileName);

    //! Opens the files, reads chunk meta. Must be called before reading blocks.
    void Open();

    //! Returns the meta file size.
    i64 GetMetaSize() const;

    //! Returns the data file size.
    i64 GetDataSize() const;

    //! Returns the full chunk size.
    i64 GetFullSize() const;

    const NChunkHolder::NProto::TChunkMeta& GetChunkMeta() const;
    NChunkHolder::NProto::TChunkMeta GetChunkMeta(const std::vector<int>& extensionTags) const;

    const NChunkHolder::NProto::TChunkInfo& GetChunkInfo() const;

    //! Implements IChunkReader and calls #ReadBlock.
    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes);

    //! Implements IChunkReader and calls #GetChunkMeta.
    virtual TAsyncGetMetaResult AsyncGetChunkMeta(const std::vector<int>& extensionTags);
    virtual TAsyncGetMetaResult AsyncGetChunkMeta();

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
    NChunkHolder::NProto::TChunkInfo ChunkInfo;
    NChunkHolder::NProto::TChunkMeta ChunkMeta;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
