#pragma once

#include "public.h"
#include "async_writer.h"
#include "format.h"

#include <ytlib/chunk_holder/chunk.pb.h>

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of #IAsyncWriter.
class TFileWriter
    : public IAsyncWriter
{
public:
    //! Creates a new writer.
    TFileWriter(const Stroka& fileName, bool directMode = false);

    virtual void Open();

    virtual bool TryWriteBlock(const TSharedRef& block);
    virtual TAsyncError GetReadyEvent();

    virtual TAsyncError AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta);

    void Abort();

    //! Returns chunk info. The writer must be already closed.
    const NChunkHolder::NProto::TChunkInfo& GetChunkInfo() const;

    //! Returns chunk meta. The writer must be already closed.
    const NChunkHolder::NProto::TChunkMeta& GetChunkMeta() const;

private:
    Stroka FileName;
    bool IsOpen;
    bool IsClosed;
    const bool DirectMode;
    i64 DataSize;
    THolder<TFile> DataFile;
    NChunkHolder::NProto::TChunkInfo ChunkInfo;
    NChunkHolder::NProto::TBlocksExt BlocksExt;
    NChunkHolder::NProto::TChunkMeta ChunkMeta;

    TAsyncError Result;

    bool EnsureOpen();

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
