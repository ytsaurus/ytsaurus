#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <core/misc/async_stream_state.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <core/logging/log.h>

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
        NChunkClient::IChunkWriterPtr chunkWriter);

    ~TFileChunkWriter();

    // If |nullptr| is returned, invoke #GetReadyEvent and wait.
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
    NChunkClient::IChunkWriterPtr ChunkWriter;

    TFileChunkWriterFacade Facade;
    TBlob Buffer;

    i64 Size;
    i32 BlockCount;

    NChunkClient::NProto::TMiscExt MiscExt;
    NFileClient::NProto::TBlocksExt BlocksExt;

    NChunkClient::NProto::TChunkMeta Meta;

    TAsyncStreamState State;

    NLog::TLogger Logger;

    void OnFinalBlocksWritten(TError error);
    void FlushBlock();

};

DEFINE_REFCOUNTED_TYPE(TFileChunkWriter)

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

    TFileChunkWriterPtr CreateChunkWriter(NChunkClient::IChunkWriterPtr chunkWriter);
    void OnChunkFinished();
    void OnChunkClosed(TFileChunkWriterPtr writer);

private:
    TFileChunkWriterConfigPtr Config;
    NChunkClient::TEncodingWriterOptionsPtr Options;

    int ActiveWriters;

};

DEFINE_REFCOUNTED_TYPE(TFileChunkWriterProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
