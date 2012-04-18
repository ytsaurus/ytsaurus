#pragma once

#include "async_writer.h"
#include "format.h"
#include <ytlib/chunk_holder/chunk.pb.h>

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of #IAsynckWriter.
class TChunkFileWriter
    : public IAsyncWriter
{
public:
    typedef TIntrusivePtr<TChunkFileWriter> TPtr;

    //! Creates a new writer.
    TChunkFileWriter(const Stroka& fileName);

    virtual void Open();

    virtual TAsyncError AsyncWriteBlocks(const std::vector<TSharedRef>& blocks);

    virtual TAsyncError AsyncClose(
        const std::vector<TSharedRef>& blocks,
        const NChunkHolder::NProto::TChunkMeta& chunkMeta);

    //! Returns chunk info. The writer must be already closed.
    const NChunkHolder::NProto::TChunkInfo& GetChunkInfo() const;
    const NChunkHolder::NProto::TChunkMeta& GetChunkMeta() const;

private:
    Stroka FileName;
    bool IsOpen;
    bool IsClosed;
    i64 DataSize;
    THolder<TFile> DataFile;
    NChunkHolder::NProto::TChunkInfo ChunkInfo;
    NChunkHolder::NProto::TBlocks Blocks;
    NChunkHolder::NProto::TChunkMeta ChunkMeta;

    bool EnsureOpen();

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
