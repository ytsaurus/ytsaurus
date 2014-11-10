#pragma once

#include "public.h"
#include "chunk_writer_base.h"

#include <core/concurrency/thread_affinity.h>

#include <core/compression/public.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/key.h>
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
    void WriteRowUnsafe(const TRow& row, const NChunkClient::TNonOwningKey& key);

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
    typedef TPartitionChunkWriterProvider TProvider;
    typedef TPartitionChunkWriterFacade TFacade;

    TPartitionChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        NChunkClient::IAsyncWriterPtr chunkWriter,
        IPartitioner* partitioner);

    ~TPartitionChunkWriter();

    TFacade* GetFacade();
    TAsyncError AsyncClose();

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

    NProto::TPartitionsExt PartitionsExt;

    virtual void PrepareBlock() override;
    void OnFinalBlocksWritten(TError error);

};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkWriterProvider
    : public virtual TRefCounted
{
public:
    TPartitionChunkWriterProvider(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        IPartitioner* partitioner);

    TPartitionChunkWriterPtr CreateChunkWriter(NChunkClient::IAsyncWriterPtr asyncWriter);
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
