#pragma once

#include "public.h"

#include <core/misc/async_stream_state.h>

#include <core/compression/codec.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileChunkReaderProvider
    : public TRefCounted
{
public:
    TFileChunkReaderProvider(
        const NChunkClient::TSequentialReaderConfigPtr& config);

    TFileChunkReaderPtr CreateReader(
        const NChunkClient::NProto::TChunkSpec& chunkSpec,
        const NChunkClient::IAsyncReaderPtr& chunkReader);

    void OnReaderOpened(
        TFileChunkReaderPtr reader,
        NChunkClient::NProto::TChunkSpec& chunkSpec);

    void OnReaderFinished(TFileChunkReaderPtr reader);

    bool KeepInMemory() const;

private:
    NChunkClient::TSequentialReaderConfigPtr Config;

};

////////////////////////////////////////////////////////////////////////////////

class TFileChunkReaderFacade
    : public TNonCopyable
{
public:
    TFileChunkReaderFacade(TFileChunkReader* reader);

    TSharedRef GetBlock() const;

private:
    TFileChunkReader* Reader;

};

////////////////////////////////////////////////////////////////////////////////

class TFileChunkReader
    : public TRefCounted
{
public:
    typedef TFileChunkReaderProvider TProvider;
    typedef TFileChunkReaderFacade TFacade;

    TFileChunkReader(
        const NChunkClient::TSequentialReaderConfigPtr& sequentialConfig,
        const NChunkClient::IAsyncReaderPtr& asyncReader,
        NCompression::ECodec codecId,
        i64 startOffset,
        i64 endOffset);

    TAsyncError AsyncOpen();

    bool FetchNext();
    TAsyncError GetReadyEvent();

    const TFacade* GetFacade() const;

    //! Must be called after AsyncOpen has finished.
    TFuture<void> GetFetchingCompleteEvent();

    // Called by facade.
    TSharedRef GetBlock() const;

private:
    NChunkClient::TSequentialReaderConfigPtr SequentialConfig;
    NChunkClient::IAsyncReaderPtr AsyncReader;

    NCompression::ECodec CodecId;

    i64 StartOffset;
    i64 EndOffset;

    NChunkClient::TSequentialReaderPtr SequentialReader;

    TFacade Facade;
    volatile bool IsFinished;

    TAsyncStreamState State;

    NLog::TTaggedLogger Logger;

    void OnNextBlock(TError error);
    void OnGotMeta(NChunkClient::IAsyncReader::TGetMetaResult result);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
