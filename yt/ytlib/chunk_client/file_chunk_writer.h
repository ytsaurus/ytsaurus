#pragma once

#include "chunk_writer.h"
#include "format.h"
#include "chunk.pb.h"

#include <util/system/file.h>

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

//! Provides a local and synchronous implementation of IChunkWriter.
class TFileChunkWriter
    : public IChunkWriter
{
public:
    typedef TIntrusivePtr<TFileChunkWriter> TPtr;

    //! Creates a new writer.
    TFileChunkWriter(Stroka fileName);

    //! Implements IChunkWriter and calls #AddBlock.
    TAsyncStreamState::TAsyncResult::TPtr 
    AsyncWriteBlock(const TSharedRef& data);

    //! Implements IChunkWriter and calls #Close.
    TAsyncStreamState::TAsyncResult::TPtr AsyncClose();
    void Cancel(const Stroka& errorMessage);

    const TChunkId& GetChunkId();

private:
    Stroka FileName;
    THolder<TFile> File;
    NChunkClient::NProto::TChunkMeta Meta;
    TAsyncStreamState::TAsyncResult::TPtr Result;
};


///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
