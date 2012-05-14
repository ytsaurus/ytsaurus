#pragma once

#include "public.h"
#include "schema.h"
#include "async_reader.h"
#include "key.h"

#include <ytlib/table_client/table_reader.pb.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/ytree/public.h>
#include <ytlib/misc/codec.h>
#include <ytlib/misc/blob_output.h>
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
    TChunkReader(
        NChunkClient::TSequentialReaderConfigPtr config,
        const TChannel& channel,
        NChunkClient::IAsyncReaderPtr chunkReader,
        const NProto::TReadLimit& startLimit,
        const NProto::TReadLimit& endLimit,
        const NYTree::TYson& rowAttributes,
        int partitionTag,
        TReaderOptions options);

    virtual TAsyncError AsyncOpen();

    virtual TAsyncError AsyncNextRow();
    virtual bool IsValid() const;

    virtual TRow& GetRow();
    virtual const TKey<TFakeStrbufStore>& GetKey() const;
    virtual const NYTree::TYson& GetRowAttributes() const;

private:
    class TKeyValidator;
    template <template <typename T> class TComparator>
    struct TIndexComparator;

    ICodec* Codec;
    NChunkClient::TSequentialReaderPtr SequentialReader;
    TChannel Channel;

    TAsyncError DoNextRow();
    void OnRowFetched(TError error);

    TAsyncError ContinueNextRow(
        int channelIndex, 
        TAsyncErrorPromise result,
        TError error);

    void MakeCurrentRow();

    class TInitializer;
    TIntrusivePtr<TInitializer> Initializer;

    TAsyncStreamState State;
    TReaderOptions Options;

    NYTree::TYson RowAttributes;
    TRow CurrentRow;
    TKey<TFakeStrbufStore> CurrentKey;

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

    THolder<TKeyValidator> EndValidator;

    TAutoPtr<NProto::TKeyColumnsExt> KeyColumnsExt;

    /*! 
     *  See DoNextRow for usage.
     */
    const TAsyncErrorPromise SuccessResult;

    std::vector<TChannelReaderPtr> ChannelReaders;

    //! Stores references to blocks if KeepBlocks option is set.
    std::vector<TSharedRef> FetchedBlocks;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
