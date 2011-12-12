#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"
#include "reader.h"
#include "channel_reader.h"

#include "../chunk_client/async_reader.h"
#include "../chunk_client/sequential_reader.h"
#include "../misc/thread_affinity.h"
#include "../misc/async_stream_state.h"
#include "../misc/codec.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Reads single table chunk row-after-row using given #NChunkClient::IAsyncReader.
class TChunkReader
    : public IAsyncReader
{
public:
    typedef TIntrusivePtr<TChunkReader> TPtr;

    /*! 
     *  \param EndRow - if given value exceeds row count of the chunk,
     *  chunk is processed to the end without error. To guarantee reading
     *  chunk to the end, set it to numerical_limits<int>::max().
     */
    TChunkReader(
        const NChunkClient::TSequentialReader::TConfig& config,
        const TChannel& channel,
        NChunkClient::IAsyncReader* chunkReader,
        int startRow,
        int endRow);

    TAsyncError::TPtr AsyncOpen();

    //! Asynchronously switches the reader to the next row.
    /*!
     *  This call cannot block.
     */
    TAsyncError::TPtr AsyncNextRow();

    bool HasNextRow() const;

    //! Switches the reader to the next column in the current row.
    /*!
     *  This call cannot block.
     *  This call does not throw.
     *  
     *  \return True iff a new column is fetched, False is the row has finished.
     */
    bool NextColumn();

    //! Returns the name of the current column.
    TColumn GetColumn() const;

    //! Returns the value of the current column.
    /*!
     *  This call returns a pointer to the actual data, which is held
     *  by the reader as long as the current row does not change. The client
     *  must make an explicit copy of it if needed.
     */
    TValue GetValue();

    void Cancel(const TError& error);

private:
    void OnGotMeta(
        NChunkClient::IAsyncReader::TReadResult readResult,
        const NChunkClient::TSequentialReader::TConfig& config,
        NChunkClient::IAsyncReader::TPtr chunkReader);

    yvector<int> SelectChannels(const yvector<TChannel>& channels);
    int SelectSingleChannel(const yvector<TChannel>& channels, const NProto::TTableChunkAttributes& attributes);

    yvector<int> GetBlockReadingOrder(
        const yvector<int>& selectedChannels, 
        const NProto::TTableChunkAttributes& attributes);

    void ContinueNextRow(TError error, int channelIndex);

    class TInitializer;
    TIntrusivePtr<TInitializer> Initializer;

    ICodec* Codec;
    NChunkClient::TSequentialReader::TPtr SequentialReader;

    TAsyncStreamState State;
    TChannel Channel;

    yhash_set<TColumn> UsedColumns;
    TColumn CurrentColumn;

    bool IsColumnValid;
    bool IsRowValid;

    int CurrentRow;

    int StartRow;
    int EndRow;

    int CurrentChannel;
    yvector<TChannelReader> ChannelReaders;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
