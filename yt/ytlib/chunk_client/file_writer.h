#pragma once

#include "async_writer.h"
#include "format.h"
#include "chunk.pb.h"

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
    TChunkFileWriter(const TChunkId& id, const Stroka& fileName);

    virtual void Open();

    virtual TAsyncError::TPtr AsyncWriteBlocks(std::vector<TSharedRef>&& blocks);

    virtual TAsyncError::TPtr AsyncClose(
        std::vector<TSharedRef>&& blocks,
        const NChunkHolder::NProto::TChunkAttributes& attributes);

    TChunkId GetChunkId() const;

    //! Returns chunk info. The writer must be already closed.
    NChunkHolder::NProto::TChunkInfo GetChunkInfo() const;

private:
    TChunkId Id;
    Stroka FileName;
    bool IsOpen;
    bool IsClosed;
    i64 DataSize;
    THolder<TFile> DataFile;
    NChunkHolder::NProto::TChunkMeta ChunkMeta;
    NChunkHolder::NProto::TChunkInfo ChunkInfo;

    bool EnsureOpen();

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
