#pragma once

#include "public.h"
#include "value.h"

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/codecs/codec.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReader
    : public virtual TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(const char*, RowPointer);

public:
    TPartitionChunkReader(
        const NChunkClient::TSequentialReaderConfigPtr& sequentialReader,
        const NChunkClient::IAsyncReaderPtr& asyncReader,
        int partitionTag,
        ECodecId codecId);

    TAsyncError AsyncOpen();

    bool IsValid() const;

    bool FetchNextItem();
    TAsyncError GetReadyEvent();

    TValue ReadValue(const TStringBuf& name);

    bool IsFetchingComplete() const;

private:
    NChunkClient::TSequentialReaderConfigPtr SequentialConfig;
    NChunkClient::IAsyncReaderPtr AsyncReader;

    i64 CurrentRowCount;
    int PartitionTag;
    ECodecId CodecId;

    TAsyncStreamState State;
    NChunkClient::TSequentialReaderPtr SequentialReader;

    std::vector<TSharedRef> Blocks;

    ui64 SizeToNextRow;

    TStringBuf ColumnName;
    TValue Value;

    TMemoryInput DataBuffer;
    TMemoryInput SizeBuffer;

    NLog::TTaggedLogger Logger;

    void OnGotMeta(NChunkClient::IAsyncReader::TGetMetaResult result);
    void OnNextBlock(TError error);

    bool NextRow();
    void NextColumn();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

