#pragma once

#include "async_writer.h"
#include "format.h"
#include "chunk.pb.h"

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of #IAsynckWriter.
class TFileWriter
    : public IAsyncWriter
{
public:
    typedef TIntrusivePtr<TFileWriter> TPtr;

    //! Creates a new writer.
    TFileWriter(const TChunkId& id, const Stroka& fileName);

    TAsyncStreamState::TAsyncResult::TPtr 
    AsyncWriteBlock(const TSharedRef& data);

    TAsyncStreamState::TAsyncResult::TPtr 
    AsyncClose(const NChunkHolder::NProto::TChunkAttributes& attributes);

    void Cancel(const Stroka& errorMessage);

    TChunkId GetChunkId() const;

private:
    TChunkId Id;
    Stroka FileName;
    THolder<TFile> DataFile;
    NChunkHolder::NProto::TChunkMeta ChunkMeta;
    TAsyncStreamState::TAsyncResult::TPtr Result;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
