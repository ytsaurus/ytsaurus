#pragma once

#include "chunk_reader.h"

#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of IAsyncReader.
class TFileReader
    : public IChunkReader
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

    //! Synchronously returns the requested meta.
    NChunkClient::NProto::TChunkMeta GetMeta(
        const std::vector<int>* extensionTags = nullptr);

    // IReader implementation.
    virtual TAsyncReadBlocksResult ReadBlocks(const std::vector<int>& blockIndexes) override;

    virtual TAsyncReadBlocksResult ReadBlocks(int firstBlockIndex, int blockCount) override;
    
    virtual TAsyncGetMetaResult GetMeta(
        const TNullable<int>& partitionTag,
        const std::vector<int>* extensionTags = nullptr) override;

    virtual TChunkId GetChunkId() const override;

private:
    Stroka FileName_;

    bool Opened_;
    std::unique_ptr<TFile> DataFile_;
    
    i64 MetaSize_;
    i64 DataSize_;

    NChunkClient::NProto::TChunkMeta Meta_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;
    int BlockCount_;


    TSharedRef ReadBlock(int blockIndex);

};

DEFINE_REFCOUNTED_TYPE(TFileReader)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
