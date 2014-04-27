#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <core/misc/async_stream_state.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <core/logging/tagged_logger.h>


namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileChunkWriterFacade
    : public TNonCopyable
{
public:
    explicit TFileChunkWriterFacade(TFileChunkWriter* writer);

    void Write(const TRef& data);

private:
    friend class TFileChunkWriter;
    TFileChunkWriter* Writer;

};

////////////////////////////////////////////////////////////////////////////////

class TFileChunkWriter
    : public TRefCounted
{
public:
    TFileChunkWriter(
        TFileChunkWriterConfigPtr config,
        NChunkClient::TEncodingWriterOptionsPtr options,
        NChunkClient::IAsyncWriterPtr chunkWriter);

    ~TFileChunkWriter();

    // Is retuns nullptr, take GetReadyEvent and wait.
    TFileChunkWriterFacade* GetFacade();
    TAsyncError GetReadyEvent();

    TAsyncError Close();

    i64 GetDataSize() const;
    i64 GetMetaSize() const;

    NChunkClient::NProto::TChunkMeta GetMasterMeta() const;
    NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const;

    void Write(const TRef& data);

private:
    TFileChunkWriterConfigPtr Config;
    NChunkClient::TEncodingWriterOptionsPtr Options;
    NChunkClient::TEncodingWriterPtr EncodingWriter;
    NChunkClient::IAsyncWriterPtr ChunkWriter;

    TFileChunkWriterFacade Facade;
    TBlob Buffer;

    i64 Size;
    i32 BlockCount;

    NChunkClient::NProto::TMiscExt MiscExt;
    NFileClient::NProto::TBlocksExt BlocksExt;

    NChunkClient::NProto::TChunkMeta Meta;

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
    typedef TFileChunkWriter TChunkWriter;
    typedef TFileChunkWriterFacade TFacade;

    TFileChunkWriterProvider(
        TFileChunkWriterConfigPtr config,
        NChunkClient::TEncodingWriterOptionsPtr options);

    TFileChunkWriterPtr CreateChunkWriter(NChunkClient::IAsyncWriterPtr asyncWriter);
    void OnChunkFinished();
    void OnChunkClosed(TFileChunkWriterPtr writer);

private:
    TFileChunkWriterConfigPtr Config;
    NChunkClient::TEncodingWriterOptionsPtr Options;

    int ActiveWriters;

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
