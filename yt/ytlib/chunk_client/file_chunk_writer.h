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

    //! A synchronous version of #AsyncAddBlock.
    void AddBlock(const TSharedRef& data);

    //! Implements IChunkWriter and calls #AddBlock.
    virtual EResult AsyncAddBlock(const TSharedRef& data, TAsyncResult<TVoid>::TPtr* ready);


    //! A synchronous version of #Close.
    void Close();
    //! Implements IChunkWriter and calls #Close.
    virtual TAsyncResult<EResult>::TPtr AsyncClose();
    virtual void Cancel();

private:
    Stroka FileName;
    THolder<TFile> File;
    NChunkClient::NProto::TChunkMeta Meta;

};


///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
