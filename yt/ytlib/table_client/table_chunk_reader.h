#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"
#include "channel_reader.h"

#include "../chunk_client/sequential_chunk_reader.h"
#include "../misc/thread_affinity.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

// TODO: extract interface ITableReader
// TODO: write something smart and funny here
class TTableChunkReader
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TTableChunkReader> TPtr;

    TTableChunkReader(
        const TSequentialChunkReader::TConfig& config,
        const TChannel& channel,
        IChunkReader::TPtr chunkReader);

    // TODO: refactor using the following declarations
    DECLARE_ENUM(ECode,
        (OK)
        (Finished)
        (TryLater)
        (Failed)
    );

    struct TResult
    {
        ECode Code;
        // TODO: use an appropriate type for errors
        Stroka Error;
    };

    //! Asynchronously switches the reader to the next row.
    /*!
     *  This call cannot block.
     *  
     *  \param ready A future that gets filled when no more
     *  data is available at hand and then gets set when more data arrives.
     *  
     *  \return Possible outcomes are:
     *  - ECode::OK: the next row is fetched, the client must invoke
     *  #NextColumn to fetch its first column.
     *  - ECode::Finished: the table has no more rows.
     *  - ECode::TryLater: no data is currently available, the client
     *  must wait for #event.
     *  - ECode::Failed: something went wrong.
     */
    TResult AsyncNextRow(TFuture<TVoid>::TPtr* ready);


    // TODO: refactor using AsyncNextRow

    //! Switches the reader to the next row.
    /*!
     *  This call may block.
     *  Internally it invokes #AsyncNextRow.
     *  This call throws an exception on failure.
     *  
     *  \return True iff a new row is fetched, False if the table chunk is finished.
     */
    bool NextRow();

    //! Switches the reader to the next column in the current row.
    /*!
     *  This call cannot block.
     *  This call does not throw.
     *  
     *  \return True iff a new column is fetched, False is the row has finished.
     */
    bool NextColumn();

    //! Returns the name of the current column.
    const TColumn& GetColumn() const;

    //! Returns the value of the current column.
    /*!
     *  This call returns a pointer to the actual data, which is held
     *  by the reader as long as the current row does not change. The client
     *  must make an explicit copy of it if needed.
     */
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
