#pragma once

#include "public.h"
#include "schema.h"
#include "async_reader.h"

#include <ytlib/table_client/table_reader.pb.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/ytree/public.h>
#include <ytlib/misc/codec.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/async_stream_state.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Reads single table chunk row-after-row using given #NChunkClient::IAsyncReader.
class TChunkReader
    : public IAsyncReader
{
public:
    struct TOptions
    {
        bool ReadKey;

        TOptions()
            : ReadKey(false)
        { }
    };

    /*! 
     *  \param EndRow - if given value exceeds row count of the chunk,
     *  chunk is processed to the end without error. To guarantee reading
     *  chunk to the end, set it to numerical_limits<int>::max().
     */
    TChunkReader(
        NChunkClient::TSequentialReaderConfigPtr config,
        const TChannel& channel,
        NChunkClient::IAsyncReaderPtr chunkReader,
        const NProto::TReadLimit& startLimit,
        const NProto::TReadLimit& endLimit,
        const NYTree::TYson& rowAttributes,
        TOptions options = TOptions());

    virtual TAsyncError AsyncOpen();

    virtual TAsyncError AsyncNextRow();
    virtual bool IsValid() const;

    virtual const TRow& GetRow() const;
    virtual const TKey& GetKey() const;
    virtual const TStringBuf& GetRowAttributes() const;

private:
    TAsyncError DoNextRow();

    TAsyncError ContinueNextRow(
        int channelIndex, 
        TAsyncErrorPromise result,
        TError error);

    void MakeCurrentRow();

    class TInitializer;
    TIntrusivePtr<TInitializer> Initializer;

    ICodec* Codec;
    NChunkClient::TSequentialReaderPtr SequentialReader;

    TAsyncStreamState State;
    TChannel Channel;
    TOptions Options;

    TRow CurrentRow;
    TKey CurrentKey;

    struct TColumnInfo
    {
        int KeyIndex;
        bool InChannel;
        bool Used;

        TColumnInfo()
            : KeyIndex(-1)
            , InChannel(false)
            , Used(false)
        { }
    };

    yhash_map<TStringBuf, TColumnInfo> FixedColumns;
    yhash_set<TStringBuf> UsedRangeColumns;

    i64 CurrentRowIndex;
    i64 EndRowIndex;

    struct IValidator;
    THolder<IValidator> EndValidator;

    /*! 
     *  See DoNextRow for usage.
     */
    const TAsyncErrorPromise SuccessResult;

    std::vector<TChannelReader> ChannelReaders;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
