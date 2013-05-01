#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <ytlib/misc/async_stream_state.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/logging/tagged_logger.h>


namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileChunkWriterFacade
    : public TNonCopyable
{
public:
    TFileChunkWriterFacade(TFileChunkWriter* writer);

    void Write(const TRef& data);

private:
    friend class TFileChunkWriter;
    TFileChunkWriter* Writer;

    bool IsReady;

    void NextChunk();

};

////////////////////////////////////////////////////////////////////////////////

class TFileChunkWriter
    : public TRefCounted
{
public:
    typedef TFileChunkWriterProvider TProvider;
    typedef TFileChunkWriterFacade TFacade;

    TFileChunkWriter(
        TFileChunkWriterConfigPtr config,
        NChunkClient::TEncodingWriterOptionsPtr options,
        NChunkClient::IAsyncWriterPtr chunkWriter);

    ~TFileChunkWriter();

    // Is retuns nullptr, take GetReadyEvent and wait.
    TFacade* GetFacade();
    TAsyncError GetReadyEvent();

    TAsyncError AsyncClose();

    i64 GetCurrentSize() const;
    i64 GetMetaSize() const;

    NChunkClient::NProto::TChunkMeta GetMasterMeta() const;
    NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const;

    void Write(const TRef& data);

private:
    TFileChunkWriterConfigPtr Config;
    NChunkClient::TEncodingWriterOptionsPtr Options;
    NChunkClient::TEncodingWriterPtr EncodingWriter;
    NChunkClient::IAsyncWriterPtr AsyncWriter;

    TFileChunkWriterFacade Facade;
    TBlob Buffer;

    i64 Size;
    i32 BlockCount;

    NChunkClient::NProto::TMiscExt MiscExt;
    NFileClient::NProto::TBlocksExt BlocksExt;

    TAsyncStreamState State;

    NLog::TTaggedLogger Logger;

    void OnFinalBlocksWritten(TError error);
    void FlushBlock();

};

////////////////////////////////////////////////////////////////////////////////

class TFileChunkWriterProvider
    : public virtual TRefCounted
{
public:
    TFileChunkWriterProvider(
        TFileChunkWriterConfigPtr config,
        NChunkClient::TEncodingWriterOptionsPtr options);

    TFileChunkWriterPtr CreateChunkWriter(NChunkClient::IAsyncWriterPtr asyncWriter);
    void OnChunkFinished();

private:
    TFileChunkWriterConfigPtr Config;
    NChunkClient::TEncodingWriterOptionsPtr Options;

    int ActiveWriters;

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT