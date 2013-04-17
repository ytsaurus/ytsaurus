#pragma once

#include "public.h"
#include "value.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/input_chunk.h>
#include <ytlib/chunk_client/async_reader.h>

#include <ytlib/logging/tagged_logger.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/compression/codec.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReaderProvider
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(volatile i64, RowIndex);

public:
    TPartitionChunkReaderProvider(
        const NChunkClient::TSequentialReaderConfigPtr& config);

    TPartitionChunkReaderPtr CreateReader(
        const NChunkClient::NProto::TInputChunk& inputChunk,
        const NChunkClient::IAsyncReaderPtr& chunkReader);

    void OnReaderOpened(
        TPartitionChunkReaderPtr reader,
        NChunkClient::NProto::TInputChunk& inputChunk);

    void OnReaderFinished(TPartitionChunkReaderPtr reader);

    bool KeepInMemory() const;

private:
    friend class TPartitionChunkReader;

    NChunkClient::TSequentialReaderConfigPtr Config;

};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReaderFacade
    : public TNonCopyable
{
    DECLARE_BYVAL_RO_PROPERTY(const char*, RowPointer);

public:
    TPartitionChunkReaderFacade(TPartitionChunkReaderPtr reader);

    TValue ReadValue(const TStringBuf& name) const;

private:
    TPartitionChunkReaderPtr Reader;

};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReader
    : public virtual TRefCounted
{
    // Points to a non-key part of the row inside a block.
    DEFINE_BYVAL_RO_PROPERTY(const char*, RowPointer);

public:
    typedef TPartitionChunkReaderProvider TProvider;
    typedef TPartitionChunkReaderFacade TFacade;

    TPartitionChunkReader(
        TPartitionChunkReaderProviderPtr provider,
        const NChunkClient::TSequentialReaderConfigPtr& sequentialReader,
        const NChunkClient::IAsyncReaderPtr& asyncReader,
        int partitionTag,
        NCompression::ECodec codecId);

    TAsyncError AsyncOpen();

    bool FetchNext();
    TAsyncError GetReadyEvent();

    const TFacade* GetFacade() const;

    //! Must be called after AsyncOpen has finished.
    TFuture<void> GetFetchingCompleteEvent();

    // Called by facade.
    TValue ReadValue(const TStringBuf& name) const;

private:
    TPartitionChunkReaderProviderPtr Provider;
    TFacade Facade;

    NChunkClient::TSequentialReaderConfigPtr SequentialConfig;
    NChunkClient::IAsyncReaderPtr AsyncReader;

    int PartitionTag;
    NCompression::ECodec CodecId;

    TAsyncStreamState State;
    NChunkClient::TSequentialReaderPtr SequentialReader;

    std::vector<TSharedRef> Blocks;
    yhash_map<TStringBuf, TValue> CurrentRow;

    ui64 SizeToNextRow;

    TMemoryInput DataBuffer;
    TMemoryInput SizeBuffer;

    NLog::TTaggedLogger Logger;

    void OnGotMeta(NChunkClient::IAsyncReader::TGetMetaResult result);
    void OnNextBlock(TError error);

    bool NextRow();

    void OnFail(const TError& error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

