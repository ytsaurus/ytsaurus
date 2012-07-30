#pragma once

#include "public.h"
#include "chunk_sequence_writer_base.h"
#include "table_chunk_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableChunkSequenceWriter
    : public TChunkSequenceWriterBase<TTableChunkWriter>
{
public:
    TTableChunkSequenceWriter(
        TTableWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const NChunkServer::TChunkListId& parentChunkList,
        const std::vector<TChannel>& channels,
        const TNullable<TKeyColumns>& keyColumns = Null);

    ~TTableChunkSequenceWriter();

    // Used internally by jobs that generate sorted output.
    bool TryWriteRowUnsafe(const TRow& row, const TNonOwningKey& key);
    bool TryWriteRowUnsafe(const TRow& row);

    const TOwningKey& GetLastKey() const;
    const TNullable<TKeyColumns>& GetKeyColumns() const;

    //! Current row count.
    i64 GetRowCount() const;

private:
    void InitCurrentSession(TSession nextSession);
    void PrepareChunkWriter(TSession& newSession);

    const std::vector<TChannel> Channels;
    const TNullable<TKeyColumns> KeyColumns;

    i64 RowCount;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
