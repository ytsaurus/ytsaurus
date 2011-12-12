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

    virtual TAsyncError::TPtr AsyncWriteBlock(const TSharedRef& data);

    virtual TAsyncError::TPtr AsyncClose(const NChunkHolder::NProto::TChunkAttributes& attributes);

    void Cancel(const TError& error);

    TChunkId GetChunkId() const;

    //! Returns chunk info. The writer must be already closed.
    NChunkHolder::NProto::TChunkInfo GetChunkInfo() const;

private:
    TChunkId Id;
    Stroka FileName;
    bool Open;
    bool Closed;
    i64 DataSize;
    THolder<TFile> DataFile;
    NChunkHolder::NProto::TChunkMeta ChunkMeta;
    TAsyncError::TPtr Result;
    NChunkHolder::NProto::TChunkInfo ChunkInfo;

    bool EnsureOpen();

};

///////////////////////////////////////////////////////////////////////////////

class TChunkFileDeleter
{
public:
    void Delete(const Stroka& path);

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
