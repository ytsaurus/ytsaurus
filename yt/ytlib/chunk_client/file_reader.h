#pragma once

#include "reader.h"
#include "format.h"

#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of IAsyncReader.
class TFileReader
    : public IReader
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

    NChunkClient::NProto::TChunkMeta GetChunkMeta(
        const std::vector<int>* extensionTags = nullptr) const;

    //! Implements IChunkReader and calls #ReadBlock.
    virtual TAsyncReadResult ReadBlocks(const std::vector<int>& blockIndexes);

    //! Implements IChunkReader and calls #GetChunkMeta.
    virtual TAsyncGetMetaResult GetChunkMeta(
        const TNullable<int>& partitionTag,
        const std::vector<int>* extensionTags = nullptr);

    //! Synchronously reads a given block from the file.
    TSharedRef ReadBlock(int blockIndex);

    //! Returns null but may be overridden in derived classes.
    virtual TChunkId GetChunkId() const override;

private:
    Stroka FileName_;

    bool Opened_;
    std::unique_ptr<TFile> DataFile_;
    
    i64 MetaSize_;
    i64 DataSize_;

    NChunkClient::NProto::TChunkMeta ChunkMeta_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

};

DEFINE_REFCOUNTED_TYPE(TFileReader)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
