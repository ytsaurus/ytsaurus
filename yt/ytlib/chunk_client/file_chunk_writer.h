#pragma once

#include "chunk_writer.h"
#include "format.h"
#include "chunk.pb.h"

#include <util/system/file.h>

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of IChunkWriter.
// TODO -> TChunkWriter
class TFileChunkWriter
    : public IChunkWriter
{
public:
    typedef TIntrusivePtr<TFileChunkWriter> TPtr;

    //! Creates a new writer.
    TFileChunkWriter(Stroka fileName);

    TAsyncStreamState::TAsyncResult::TPtr 
    AsyncWriteBlock(const TSharedRef& data);

    TAsyncStreamState::TAsyncResult::TPtr 
    AsyncClose(const TSharedRef& masterMeta);

    void Cancel(const Stroka& errorMessage);

    const TChunkId& GetChunkId() const;

private:
    Stroka FileName;
    THolder<TFile> File;
    NChunkClient::NProto::TChunkMeta Meta;
    TAsyncStreamState::TAsyncResult::TPtr Result;
};


///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
