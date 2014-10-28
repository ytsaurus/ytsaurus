#pragma once

#include "public.h"
#include "chunk_writer_base.h"

#include <core/concurrency/thread_affinity.h>

#include <core/compression/public.h>
#include <core/yson/lexer.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/chunk_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkWriterFacade
    : public TNonCopyable
{
public:
    // Checks column names for uniqueness.
    void WriteRow(const TRow& row);

    // Used internally. All column names are guaranteed to be unique.
    void WriteRowUnsafe(const TRow& row);

    // Required by SyncWriterAdapter.
    void WriteRowUnsafe(const TRow& row, const NVersionedTableClient::TKey& key);

private:
    friend class TPartitionChunkWriter;
    TPartitionChunkWriter* Writer;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);

    explicit TPartitionChunkWriterFacade(TPartitionChunkWriter* writer);

};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkWriter
    : public TChunkWriterBase
{
    DEFINE_BYVAL_RO_PROPERTY(i64, RowCount);

public:
    TPartitionChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IChunkWriterPtr chunkWriter,
        IPartitioner* partitioner);

    ~TPartitionChunkWriter();

    TPartitionChunkWriterFacade* GetFacade();
    TAsyncError Close();

    i64 GetMetaSize() const;

    NChunkClient::NProto::TChunkMeta GetMasterMeta() const;
    NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const;

    void WriteRow(const TRow& row);
    void WriteRowUnsafe(const TRow& row);

private:
    IPartitioner* Partitioner;
    TPartitionChunkWriterFacade Facade;

    NYson::TStatelessLexer Lexer;
    yhash_map<TStringBuf, int> KeyColumnIndexes;

    i64 BasicMetaSize;

    NVersionedTableClient::TKey PartitionKey;
    TChunkedMemoryPool Pool;

    NProto::TPartitionsExt PartitionsExt;

    void PrepareBlock();
    void OnFinalBlocksWritten(TError error);

};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkWriterProvider
    : public virtual TRefCounted
{
public:
    typedef TPartitionChunkWriter TChunkWriter;
    typedef TPartitionChunkWriterFacade TFacade;

    TPartitionChunkWriterProvider(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        IPartitioner* partitioner);

    TPartitionChunkWriterPtr CreateChunkWriter(NChunkClient::IChunkWriterPtr chunkWriter);
    void OnChunkFinished();
    void OnChunkClosed(TPartitionChunkWriterPtr writer);

    const TNullable<TKeyColumns>& GetKeyColumns() const;
    i64 GetRowCount() const;
    NChunkClient::NProto::TDataStatistics GetDataStatistics() const;

private:
    TChunkWriterConfigPtr Config;
    TChunkWriterOptionsPtr Options;
    IPartitioner* Partitioner;

    int ActiveWriterCount;
    TPartitionChunkWriterPtr CurrentWriter;

    TSpinLock SpinLock;

    yhash_set<TPartitionChunkWriterPtr> ActiveWriters;
    NChunkClient::NProto::TDataStatistics DataStatistics;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
