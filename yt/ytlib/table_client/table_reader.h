#pragma once

#include "value.h"
#include "schema.h"

#include "../chunk_client/sequential_chunk_reader.h"
#include "../misc/thread_affinity.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TTableReader> TPtr;

    TTableReader(
        const TSequentialChunkReader::TConfig& config,
        const TChannel& channel,
        IChunkReader::TPtr chunkReader);

    bool NextRow();
    bool NextColumn();

    const TColumn& GetColumn() const;
    TValue GetValue() const;

private:
    void OnGotMeta(
        IChunkReader::TReadResult readResult,
        const TSequentialChunkReader::TConfig& config,
        IChunkReader::TPtr chunkReader);

    yvector<int> SelectChannels(const yvector<TChannel>& channels);
    int SelectSingleChannel(const yvector<TChannel>& channels, const NProto::TChunkMeta& protoMeta);

    yvector<int> GetBlockReadingOrder(
        const yvector<int>& selectedChannels, 
        const NProto::TChunkMeta& protoMeta);

    TSequentialChunkReader::TPtr SequentialChunkReader;

    TFuture<bool>::TPtr InitSuccess;
    TChannel Channel;

    yhash_set<TColumn> UsedColumns;
    TColumn CurrentColumn;

    int CurrentRow;
    int RowCount;

    bool IsColumnValid;
    bool IsRowValid;

    int CurrentChannel;
    yvector<TChannelReader> ChannelReaders;

    DECLARE_THREAD_AFFINITY_SLOT(Client);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
