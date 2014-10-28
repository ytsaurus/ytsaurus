#pragma once

#include "public.h"
#include "chunk_writer_base.h"
#include "channel_writer.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk_ypath_proxy.h>

#include <core/concurrency/thread_affinity.h>

#include <core/misc/blob_output.h>

#include <core/yson/lexer.h>

#include <core/compression/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

// Instance of facade returned from ChunkWriter allows to make single
// write operation.

class TTableChunkWriterFacade
    : public TNonCopyable
{
public:
    // Checks column names for uniqueness.
    void WriteRow(const TRow& row);

    // Used internally. All column names are guaranteed to be unique.
    void WriteRowUnsafe(const TRow& row, const NVersionedTableClient::TKey& key);
    void WriteRowUnsafe(const TRow& row);

private:
    friend class TTableChunkWriter;
    TTableChunkWriter* Writer;

    explicit TTableChunkWriterFacade(TTableChunkWriter* writer);

};

////////////////////////////////////////////////////////////////////////////////

class TTableChunkWriter
    : public TChunkWriterBase
{
public:
    TTableChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IChunkWriterPtr chunkWriter,
        NVersionedTableClient::TOwningKey lastKey);

    ~TTableChunkWriter();

    TTableChunkWriterFacade* GetFacade();
    TAsyncError Close();

    i64 GetMetaSize() const;
    NChunkClient::NProto::TChunkMeta GetMasterMeta() const;
    NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const;

    // Used by provider.
    const NVersionedTableClient::TOwningKey& GetLastKey() const;
    const NProto::TOldBoundaryKeysExt& GetOldBoundaryKeys() const;

    // Used by facade.
    void WriteRow(const TRow& row);
    void WriteRowUnsafe(const TRow& row, const NVersionedTableClient::TKey& key);
    void WriteRowUnsafe(const TRow& row);

private:
    struct TChannelColumn
    {
        int ColumnIndex;
        TChannelWriterPtr Writer;

        TChannelColumn(const TChannelWriterPtr& channelWriter, int columnIndex)
            : ColumnIndex(columnIndex)
            , Writer(channelWriter)
        { }
    };

    struct TColumnInfo {
        i64 LastRow;
        int KeyColumnIndex;
        std::vector<TChannelColumn> Channels;

        TColumnInfo()
            : LastRow(-1)
            , KeyColumnIndex(-1)
        { }
    };

    TTableChunkWriterFacade Facade;
    NChunkClient::TChannels Channels;

    //! Stores mapping from all key columns and channel non-range columns to indexes.
    yhash_map<TStringBuf, TColumnInfo> ColumnMap;
    std::vector<Stroka> ColumnNames;

    // Used for key creation.
    NYson::TStatelessLexer Lexer;

    TChunkedMemoryPool CurrentKeyMemoryPool;
    NVersionedTableClient::TKey CurrentKey;

    NVersionedTableClient::TOwningKey LastKey;

    //! Approximate size of collected samples.
    i64 SamplesSize;
    double AverageSampleSize;

    //! Approximate size of collected index.
    i64 IndexSize;

    // Size of static part of meta, computed during initialization.
    i64 BasicMetaSize;

    NProto::TOldSamplesExt SamplesExt;
    NProto::TSample FirstSample;

    //! Only for sorted tables.
    NProto::TOldBoundaryKeysExt BoundaryKeysExt;
    NProto::TIndexExt IndexExt;

    void PrepareBlock();

    void OnFinalBlocksWritten(TError error);

    void EmitIndexEntry();
    i64 EmitSample(const TRow& row, NProto::TSample* sample);

    void SelectChannels(const TStringBuf& name, TColumnInfo& columnInfo);
    void FinalizeRow(const TRow& row);
    void ProcessKey();
    void WriteValue(const std::pair<TStringBuf, TStringBuf>& value, const TColumnInfo& columnInfo);

    TColumnInfo& GetColumnInfo(const TStringBuf& name);

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

class TTableChunkWriterProvider
    : public virtual TRefCounted
{
public:
    typedef TTableChunkWriter TChunkWriter;
    typedef TTableChunkWriterFacade TFacade;

    TTableChunkWriterProvider(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options);

    TTableChunkWriterPtr CreateChunkWriter(NChunkClient::IChunkWriterPtr chunkWriter);
    void OnChunkFinished();
    void OnChunkClosed(TTableChunkWriterPtr writer);

    const NProto::TOldBoundaryKeysExt& GetOldBoundaryKeys() const;
    i64 GetRowCount() const;
    NChunkClient::NProto::TDataStatistics GetDataStatistics() const;

private:
    TChunkWriterConfigPtr Config;
    TChunkWriterOptionsPtr Options;

    int CreatedWriterCount;
    int FinishedWriterCount;

    NProto::TOldBoundaryKeysExt BoundaryKeysExt;
    TTableChunkWriterPtr CurrentWriter;

    TSpinLock SpinLock;

    yhash_set<TTableChunkWriterPtr> ActiveWriters;
    NChunkClient::NProto::TDataStatistics DataStatistics;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
