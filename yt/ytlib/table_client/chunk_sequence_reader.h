#pragma once

#include "common.h"
#include "reader.h"
#include "../misc/async_stream_state.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*
class TChunkSequenceReader
    : public IAsyncReader
{
public:
    struct TConfig 
    {
        
    };

    TChunkSequenceReader(
        const TConfig& config,
        const yvector<NChunkClient::TChunkId>& chunks,
        int StartRow,
        int EndRow,
        IChannel::TPtr masterChannel);

    TAsyncStreamState::TAsyncResult::TPtr AsyncOpen();

    bool HasNextRow() const;

    TAsyncStreamState::TAsyncResult::TPtr AsyncNextRow();

    bool NextColumn();

    TValue GetValue();

    TColumn GetColumn() const;

    void Cancel(const Stroka& errorMessage);

private:

    const TConfig Config;

    int CurrentChunk;
    TChunkReader::TPtr CurrentReader;

};
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
