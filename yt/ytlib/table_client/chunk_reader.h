#pragma once

#include "public.h"
#include "schema.h"
#include "async_reader.h"
#include "key.h"

#include <ytlib/misc/codec.h>
#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/table_client/table_reader.pb.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/ytree/public.h>
#include <ytlib/ytree/lexer.h>
#include <ytlib/ytree/yson_string.h>

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
        const NYTree::TYsonString& rowAttributes,
        int partitionTag,
        TReaderOptions options);

    virtual TAsyncError AsyncOpen();

    virtual TAsyncError AsyncNextRow();
    virtual bool IsValid() const;

    virtual TRow& GetRow();
    virtual const TNonOwningKey& GetKey() const;
    virtual const NYTree::TYsonString& GetRowAttributes() const;

    i64 GetRowCount() const;

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

    class IInitializer;
    TIntrusivePtr<IInitializer> Initializer;

    class TRegularInitializer;
    class TPartitionInitializer;

    TAsyncStreamState State;
    TReaderOptions Options;

    NYTree::TYsonString RowAttributes;
    TRow CurrentRow;
    TNonOwningKey CurrentKey;


    NYTree::TLexer Lexer;

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
    i64 StartRowIndex;
    i64 EndRowIndex;

    int PartitionTag;

    THolder<TKeyValidator> EndValidator;

    NProto::TKeyColumnsExt KeyColumnsExt;

    /*! 
     *  See #DoNextRow for usage.
     */
    const TAsyncErrorPromise SuccessResult;

    std::vector<TChannelReaderPtr> ChannelReaders;

    /*!
     *  If #TReaderOptions::KeepBlocks option is set then the reader keeps references
     *  to all (uncompressed) blocks it has fetched.
     */
    std::vector<TSharedRef> FetchedBlocks;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
