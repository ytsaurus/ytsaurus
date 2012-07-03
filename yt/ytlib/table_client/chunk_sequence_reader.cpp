#include "stdafx.h"
#include "private.h"
#include "chunk_sequence_reader.h"
#include "chunk_reader.h"
#include "config.h"
#include "schema.h"

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/ytree/convert.h>

#include <limits>

namespace NYT {
namespace NTableClient {

using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableReaderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkSequenceReader::TChunkSequenceReader(
    TChunkSequenceReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    const std::vector<NProto::TInputChunk>& fetchedChunks,
    const TReaderOptions& options)
    : Config(config)
    , BlockCache(blockCache)
    , InputChunks(fetchedChunks)
    , MasterChannel(masterChannel)
    , CurrentReaderIndex(-1)
    , Options(options)
    , TotalValueCount(0)
    , TotalRowCount(0)
    , CurrentRowIndex(0)
    , LastInitializedReader(-1)
    , LastPreparedReader(-1)
{
    // ToDo(psushin): implement TotalRowCount update.

    LOG_DEBUG("Chunk sequence reader created (ChunkCount: %d)", 
        static_cast<int>(InputChunks.size()));

    for (int i = 0; i < static_cast<int>(InputChunks.size()); ++i) {
        auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(InputChunks[i].extensions());
        //TotalRowCount += miscExt.row_count();
        TotalValueCount += miscExt.value_count();
        Readers.push_back(NewPromise<TChunkReaderPtr>());
    }

    for (int i = 0; i < Config->PrefetchWindow; ++i) {
        PrepareNextChunk();
    }
}

void TChunkSequenceReader::PrepareNextChunk()
{
    int chunkSlicesSize = static_cast<int>(InputChunks.size());

    ++LastPreparedReader;
    if (LastPreparedReader >= chunkSlicesSize)
        return;

    const auto& inputChunk = InputChunks[LastPreparedReader];
    const auto& slice = inputChunk.slice();
    auto chunkId = TChunkId::FromProto(inputChunk.slice().chunk_id());

    LOG_DEBUG("Opening chunk (ChunkIndex: %d, ChunkId: %s)", 
        LastPreparedReader,
        ~chunkId.ToString());

    auto remoteReader = CreateRemoteReader(
        Config->RemoteReader,
        BlockCache,
        MasterChannel,
        chunkId,
        FromProto<Stroka>(inputChunk.node_addresses()));

    auto chunkReader = New<TChunkReader>(
        Config->SequentialReader,
        TChannel::FromProto(inputChunk.channel()),
        remoteReader,
        slice.start_limit(),
        slice.end_limit(),
        // TODO(ignat) yson type ?
        NYTree::TYsonString(inputChunk.row_attributes()),
        inputChunk.partition_tag(),
        Options); // ToDo(psushin): pass row attributes here.

    chunkReader->AsyncOpen().Subscribe(BIND(
        &TChunkSequenceReader::OnReaderOpened,
        MakeWeak(this),
        chunkReader,
        LastPreparedReader).Via(NChunkClient::ReaderThread->GetInvoker()));
}

void TChunkSequenceReader::OnReaderOpened(
    TChunkReaderPtr reader,
    int chunkIndex,
    TError error)
{
    ++LastInitializedReader;

    LOG_DEBUG("Chunk opened (ChunkIndex: %d, ReaderIndex: %d)", 
        chunkIndex, 
        LastInitializedReader);

    YASSERT(!Readers[LastInitializedReader].IsSet());

    if (error.IsOK()) {
        Readers[LastInitializedReader].Set(reader);
        return;
    }

    State.Fail(error);
    Readers[LastInitializedReader].Set(TChunkReaderPtr());
}

TAsyncError TChunkSequenceReader::AsyncOpen()
{
    YASSERT(CurrentReaderIndex == -1);
    YASSERT(!State.HasRunningOperation());

    ++CurrentReaderIndex;

    if (CurrentReaderIndex < InputChunks.size()) {
        State.StartOperation();
        Readers[CurrentReaderIndex].Subscribe(BIND(
            &TChunkSequenceReader::SwitchCurrentChunk,
            MakeWeak(this)));
    }

    return State.GetOperationError();
}

void TChunkSequenceReader::SwitchCurrentChunk(TChunkReaderPtr nextReader)
{
    if (!Options.KeepBlocks && CurrentReaderIndex > 0) {
        Readers[CurrentReaderIndex - 1].Reset();
    }

    LOG_DEBUG("Switching to reader %d", CurrentReaderIndex);
    CurrentReader.Reset();

    if (nextReader) {
        {
            // Update row count after opening new reader.
            // Take actual number of rows from used readers, estimated from just opened and
            // misc estimation for pending ones.
            TotalRowCount = CurrentRowIndex + nextReader->GetRowCount();
            for (int i = CurrentReaderIndex + 1; i < InputChunks.size(); ++i) {
                auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(InputChunks[i].extensions());
                TotalRowCount += miscExt.row_count();
            }
        }

        CurrentReader = nextReader;
        PrepareNextChunk();

        if (!nextReader->IsValid()) {
            ++CurrentReaderIndex;
            if (CurrentReaderIndex < InputChunks.size()) {
                Readers[CurrentReaderIndex].Subscribe(BIND(
                    &TChunkSequenceReader::SwitchCurrentChunk,
                    MakeWeak(this)));
                return;
            }
        }
    }

    // Finishing AsyncOpen.
    State.FinishOperation();
}

void TChunkSequenceReader::OnRowFetched(TError error)
{
    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    if (!CurrentReader->IsValid()) {
        ++CurrentReaderIndex;
        if (CurrentReaderIndex < InputChunks.size()) {
            Readers[CurrentReaderIndex].Subscribe(BIND(
                &TChunkSequenceReader::SwitchCurrentChunk,
                MakeWeak(this)));
            return;
        }
    }

    ++CurrentRowIndex;
    State.FinishOperation();
}

bool TChunkSequenceReader::IsValid() const
{
    YASSERT(!State.HasRunningOperation());
    if (CurrentReaderIndex >= InputChunks.size())
        return false;

    return CurrentReader->IsValid();
}

TRow& TChunkSequenceReader::GetRow()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    return CurrentReader->GetRow();
}

const NYTree::TYsonString& TChunkSequenceReader::GetRowAttributes() const
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    return CurrentReader->GetRowAttributes();
}

TAsyncError TChunkSequenceReader::AsyncNextRow()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    State.StartOperation();
    
    // This is a performance-critical spot. Try to avoid using callbacks for synchronously fetched rows.
    auto asyncResult = CurrentReader->AsyncNextRow();
    auto error = asyncResult.TryGet();
    if (error) {
        OnRowFetched(error.Get());
    } else {
        asyncResult.Subscribe(BIND(&TChunkSequenceReader::OnRowFetched, MakeWeak(this)));
    }

    return State.GetOperationError();
}

const TNonOwningKey& TChunkSequenceReader::GetKey() const
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    return CurrentReader->GetKey();
}

double TChunkSequenceReader::GetProgress() const
{
    return CurrentRowIndex / double(TotalRowCount);
}

i64 TChunkSequenceReader::GetRowCount() const
{
    return TotalRowCount;
}

i64 TChunkSequenceReader::GetValueCount() const
{
    return TotalValueCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
