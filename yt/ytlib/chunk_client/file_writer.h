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
    TFileWriter(const Stroka& fileName);

    TAsyncStreamState::TAsyncResult::TPtr 
    AsyncWriteBlock(const TSharedRef& data);

    TAsyncStreamState::TAsyncResult::TPtr 
    AsyncClose(const TSharedRef& masterMeta);

    void Cancel(const Stroka& errorMessage);

    TChunkId GetChunkId() const;

private:
    Stroka FileName;
    THolder<TFile> File;
    NChunkClient::NProto::TChunkMeta Meta;
    TAsyncStreamState::TAsyncResult::TPtr Result;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
