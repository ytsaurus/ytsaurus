#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/data_statistics.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <core/compression/public.h>

#include <core/misc/blob_output.h>
#include <core/misc/async_stream_state.h>

#include <core/ytree/public.h>
#include <core/ytree/yson_string.h>

#include <core/yson/lexer.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableChunkReaderFacade
    : public TNonCopyable
{
public:
    const TRow& GetRow() const;
    const NVersionedTableClient::TKey& GetKey() const;
    int GetTableIndex() const;
    i64 GetTableRowIndex() const;

private:
    friend class TTableChunkReader;
    TTableChunkReader* Reader;

    explicit TTableChunkReaderFacade(TTableChunkReader* reader);

};

////////////////////////////////////////////////////////////////////////////////

//! Reads single table chunk row-after-row using given #NChunkClient::IAsyncReader.
class TTableChunkReader
    : public virtual TRefCounted
{
public:
    typedef TTableChunkReaderProvider TProvider;
    typedef TTableChunkReaderFacade TFacade;

    TTableChunkReader(
        TTableChunkReaderProviderPtr provider,
        NChunkClient::TSequentialReaderConfigPtr config,
        const NChunkClient::TChannel& channel,
        NChunkClient::IChunkReaderPtr chunkReader,
        NChunkClient::IBlockCachePtr uncompressedBlockCache,
        const NChunkClient::TReadLimit& startLimit,
        const NChunkClient::TReadLimit& endLimit,
        int tableIndex,
        i64 tableRowIndex,
        int partitionTag,
        TChunkReaderOptionsPtr options);
    ~TTableChunkReader();

    TFuture<void> AsyncOpen();

    bool FetchNext();
    TFuture<void> GetReadyEvent();

    const TFacade* GetFacade() const;

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const;
    i64 GetSessionRowIndex() const;
    i64 GetSessionRowCount() const;
    i64 GetTableRowIndex() const;
    TFuture<void> GetFetchingCompleteEvent();

    // Called by facade.
    const TRow& GetRow() const;
    const NVersionedTableClient::TKey& GetKey() const;
    int GetTableIndex() const;

private:
    struct TTableChunkReaderMemoryPoolTag {};

    struct TColumnInfo
    {
        int KeyIndex;
        bool InChannel;
        i64 RowIndex;

        TColumnInfo()
            : KeyIndex(-1)
            , InChannel(false)
            , RowIndex(-1)
        { }
    };

    class TKeyValidator;

    class TInitializer;

    //! Initializer for regular table chunks.
    class TRegularInitializer;

    //! Initializer for partition table chunks.
    class TPartitionInitializer;

    bool DoFetchNextRow();
    bool ContinueFetchNextRow(int channelIndex, const TError& error);

    void MakeCurrentRow();
    bool ValidateRow();
    void OnRowFetched(const TError& error);

    void ClearKey();

    TColumnInfo& GetColumnInfo(const TStringBuf& column);

    TTableChunkReaderFacade Facade;

    volatile bool IsFinished;

    NChunkClient::TSequentialReaderPtr SequentialReader;
    NChunkClient::TChannel Channel;

    TIntrusivePtr<TInitializer> Initializer;

    TAsyncStreamState ReaderState;
    TAsyncStreamState RowState;

    TChunkReaderOptionsPtr Options;

    TRow CurrentRow;
    TChunkedMemoryPool KeyMemoryPool;
    NVersionedTableClient::TKey CurrentKey;

    int TableIndex;

    NYson::TStatelessLexer Lexer;

    yhash_map<TStringBuf, TColumnInfo> ColumnsMap;
    std::vector<Stroka> ColumnNames;

    i64 StartTableRowIndex;

    i64 CurrentRowIndex;
    i64 StartRowIndex;
    i64 EndRowIndex;

    int PartitionTag;

    TCallback<void(const TError&)> OnRowFetchedCallback;

    std::unique_ptr<TKeyValidator> EndValidator;

    NProto::TKeyColumnsExt KeyColumnsExt;

    /*!
     *  See #DoNextRow for usage.
     */
    const TPromise<void> SuccessResult;

    std::vector<TChannelReaderPtr> ChannelReaders;

    /*!
     *  If #TReaderOptions::KeepBlocks option is set then the reader keeps references
     *  to all (uncompressed) blocks it has fetched.
     */
    std::vector<TSharedRef> FetchedBlocks;

};

////////////////////////////////////////////////////////////////////////////////

class TTableChunkReaderProvider
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(volatile i64, RowCount);

public:
    TTableChunkReaderProvider(
        const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        NChunkClient::TSequentialReaderConfigPtr config,
        NChunkClient::IBlockCachePtr uncompressedBlockCache,
        TChunkReaderOptionsPtr options = New<TChunkReaderOptions>(),
        TNullable<i64> startTableRowIndex = Null);

    TTableChunkReaderPtr CreateReader(
        const NChunkClient::NProto::TChunkSpec& chunkSpec,
        NChunkClient::IChunkReaderPtr chunkReader);

    void OnReaderOpened(
        TTableChunkReaderPtr reader,
        NChunkClient::NProto::TChunkSpec& chunkSpec);

    void OnReaderFinished(TTableChunkReaderPtr reader);

    bool KeepInMemory() const;
    NChunkClient::NProto::TDataStatistics GetDataStatistics() const;

    i64 GetRowIndex() const;

private:
    friend class TTableChunkReader;

    NChunkClient::TSequentialReaderConfigPtr Config;
    NChunkClient::IBlockCachePtr UncompressedBlockCache;
    TChunkReaderOptionsPtr Options;

    TSpinLock SpinLock;

    NChunkClient::NProto::TDataStatistics DataStatistics;
    yhash_set<TTableChunkReaderPtr> ActiveReaders;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
