#pragma once

#include "public.h"
#include "schema.h"
#include "key.h"
#include "chunk_writer_base.h"

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/compression/public.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkWriterFacade
    : public TNonCopyable
{
public:
    TPartitionChunkWriterFacade(TPartitionChunkWriterPtr writer);

    // Checks column names for uniqueness.
    void WriteRow(const TRow& row);

    // Used internally. All column names are guaranteed to be unique.
    void WriteRowUnsafe(const TRow& row);

    // Required by SyncWriterAdapter.
    void WriteRowUnsafe(const TRow& row, const TNonOwningKey& key);

private:
    friend class TPartitionChunkWriter;
    TPartitionChunkWriterPtr Writer;

    bool IsReady;

    void NextRow();

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);

};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkWriter
    : public TChunkWriterBase
{
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

    i64 GetCurrentSize() const;
    i64 GetMetaSize() const;

    NChunkClient::NProto::TChunkMeta GetMasterMeta() const;
    NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const;

    void WriteRow(const TRow& row);
    void WriteRowUnsafe(const TRow& row);

private:
    IPartitioner* Partitioner;
    TPartitionChunkWriterFacade Facade;

    NYson::TLexer Lexer;
    yhash_map<TStringBuf, int> KeyColumnIndexes;

    i64 BasicMetaSize;

    NProto::TPartitionsExt PartitionsExt;

    void PrepareBlock();
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

    // Required by sync writer.
    i64 GetRowCount() const;
    const TNullable<TKeyColumns>& GetKeyColumns() const;

private:
    TChunkWriterConfigPtr Config;
    TChunkWriterOptionsPtr Options;
    IPartitioner* Partitioner;

    int ActiveWriters;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
