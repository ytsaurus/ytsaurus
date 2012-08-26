#pragma once

#include "public.h"
#include "schema.h"
#include "key.h"
#include "chunk_writer_base.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/codecs/codec.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkWriter
    : public TChunkWriterBase
{
public:
    TPartitionChunkWriter(
        TChunkWriterConfigPtr config,
        NChunkClient::IAsyncWriterPtr chunkWriter,
        const TKeyColumns& keyColumns,
        IPartitioner* partitioner);

    ~TPartitionChunkWriter();

    // Checks column names for uniqueness.
    bool TryWriteRow(const TRow& row);

    // Used internally. All column names are guaranteed to be unique.
    bool TryWriteRowUnsafe(const TRow& row);

    TAsyncError AsyncClose();

    i64 GetCurrentSize() const;
    i64 GetMetaSize() const;

    NChunkClient::NProto::TChunkMeta GetMasterMeta() const;
    NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const;

private:
    IPartitioner* Partitioner;

    NYTree::TLexer Lexer;
    yhash_map<TStringBuf, int> KeyColumnIndexes;

    i64 BasicMetaSize;
   
    NProto::TPartitionsExt PartitionsExt;

    void PrepareBlock();

    void OnFinalBlocksWritten(TError error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
