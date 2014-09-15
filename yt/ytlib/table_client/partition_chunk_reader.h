#pragma once

#include "public.h"
#include "value.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/reader.h>
#include <ytlib/chunk_client/data_statistics.h>

#include <core/misc/async_stream_state.h>

#include <core/compression/public.h>

#include <core/logging/log.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReaderFacade
    : public TNonCopyable
{
    DECLARE_BYVAL_RO_PROPERTY(const char*, RowPointer);

public:
    TValue ReadValue(const TStringBuf& name) const;

private:
    friend class TPartitionChunkReader;
    TPartitionChunkReader* Reader;

    explicit TPartitionChunkReaderFacade(TPartitionChunkReader* reader);

};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReader
    : public virtual TRefCounted
{
public:
    // Points to a non-key part of the row inside a block.
    DEFINE_BYVAL_RO_PROPERTY(const char*, RowPointer);
    DEFINE_BYVAL_RO_PROPERTY(i64, RowIndex);

public:
    typedef TPartitionChunkReaderProvider TProvider;
    typedef TPartitionChunkReaderFacade TFacade;

    TPartitionChunkReader(
        TPartitionChunkReaderProviderPtr provider,
        NChunkClient::TSequentialReaderConfigPtr sequentialReader,
        NChunkClient::IReaderPtr chunkReader,
        NChunkClient::IBlockCachePtr uncompressedBlockCache,
        int partitionTag,
        NCompression::ECodec codecId);

    TAsyncError AsyncOpen();

    bool FetchNext();
    TAsyncError GetReadyEvent();

    const TFacade* GetFacade() const;

    //! Must be called after AsyncOpen has finished.
    TFuture<void> GetFetchingCompleteEvent();

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const;

    // Called by facade.
    TValue ReadValue(const TStringBuf& name) const;

private:
    TPartitionChunkReaderProviderPtr Provider;
    TFacade Facade;

    NChunkClient::TSequentialReaderConfigPtr SequentialConfig;
    NChunkClient::IReaderPtr ChunkReader;
    NChunkClient::IBlockCachePtr UncompressedBlockCache;
    int PartitionTag;
    NCompression::ECodec CodecId;

    TAsyncStreamState State;
    NChunkClient::TSequentialReaderPtr SequentialReader;

    std::vector<TSharedRef> Blocks;
    yhash_map<TStringBuf, TValue> CurrentRow;

    ui64 SizeToNextRow;

    TMemoryInput DataBuffer;
    TMemoryInput SizeBuffer;

    NLog::TLogger Logger;

    void OnGotMeta(NChunkClient::IReader::TGetMetaResult result);
    void OnNextBlock(TError error);

    bool NextRow();

    void OnFail(const TError& error);

};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReaderProvider
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(volatile i64, RowIndex);

public:
    TPartitionChunkReaderProvider(
        NChunkClient::TSequentialReaderConfigPtr config,
        NChunkClient::IBlockCachePtr uncompressedBlockCache);

    TPartitionChunkReaderPtr CreateReader(
        const NChunkClient::NProto::TChunkSpec& chunkSpec,
        NChunkClient::IReaderPtr chunkReader);

    void OnReaderOpened(
        TPartitionChunkReaderPtr reader,
        NChunkClient::NProto::TChunkSpec& chunkSpec);

    void OnReaderFinished(TPartitionChunkReaderPtr reader);

    bool KeepInMemory() const;
    NChunkClient::NProto::TDataStatistics GetDataStatistics() const;

private:
    friend class TPartitionChunkReader;

    NChunkClient::TSequentialReaderConfigPtr Config;
    NChunkClient::IBlockCachePtr UncompressedBlockCache;

    TSpinLock SpinLock;
    NChunkClient::NProto::TDataStatistics DataStatistics;
    yhash_set<TPartitionChunkReaderPtr> ActiveReaders;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

