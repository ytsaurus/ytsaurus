#include "stdafx.h"
#include "private.h"
#include "chunk_sequence_reader.h"
#include "chunk_reader.h"
#include "config.h"
#include "schema.h"

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/rpc/channel.h>

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
    int partitionTag,
    const TReaderOptions& options)
    : Config(config)
    , BlockCache(blockCache)
    , InputChunks(fetchedChunks)
    , MasterChannel(masterChannel)
    , CurrentReader(-1)
    , LastInitializedReader(-1)
    , LastPreparedReader(-1)
    , PartitionTag(partitionTag)
    , Options(options)
    , TotalValueCount(0)
    , TotalRowCount(0)
    , CurrentRowIndex(0)
{
    // ToDo(psushin): implement TotalRowCount update.

    LOG_DEBUG("Chunk sequence reader created (ChunkCount: %d)", 
        static_cast<int>(InputChunks.size()));

    for (int i = 0; i < static_cast<int>(InputChunks.size()); ++i) {
        auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(InputChunks[i].extensions());
        //TotalRowCount += miscExt->row_count();
        TotalValueCount += miscExt->value_count();
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
        inputChunk.row_attributes(),
        PartitionTag,
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
    YASSERT(CurrentReader == -1);
    YASSERT(!State.HasRunningOperation());

    ++CurrentReader;

    if (CurrentReader < InputChunks.size()) {
        State.StartOperation();
        Readers[CurrentReader].Subscribe(BIND(
            &TChunkSequenceReader::SwitchCurrentChunk,
            MakeWeak(this)));
    }

    return State.GetOperationError();
}

void TChunkSequenceReader::SwitchCurrentChunk(TChunkReaderPtr nextReader)
{
    if (!Options.KeepBlocks && CurrentReader > 0) {
        Readers[CurrentReader - 1].Reset();
    }

    LOG_DEBUG("Switching to reader %d", CurrentReader);

    if (nextReader) {
        {
            // Update row count after opening new reader.
            // Take actual number of rows from used readers, estimated from just opened and
            // misc estimation for pending ones.
            TotalRowCount = CurrentRowIndex + nextReader->GetRowCount();
            for (int i = CurrentReader + 1; i < InputChunks.size(); ++i) {
                auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(InputChunks[i].extensions());
                TotalRowCount += miscExt->row_count();
            }
        }

        PrepareNextChunk();

        if (!nextReader->IsValid()) {
            ++CurrentReader;
            if (CurrentReader < InputChunks.size()) {
                Readers[CurrentReader].Subscribe(BIND(
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

    if (!Readers[CurrentReader].Get()->IsValid()) {
        ++CurrentReader;
        if (CurrentReader < InputChunks.size()) {
            Readers[CurrentReader].Subscribe(BIND(
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
    if (CurrentReader >= InputChunks.size())
        return false;

    return Readers[CurrentReader].Get()->IsValid();
}

TRow& TChunkSequenceReader::GetRow()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    return Readers[CurrentReader].Get()->GetRow();
}

const NYTree::TYson& TChunkSequenceReader::GetRowAttributes() const
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    return Readers[CurrentReader].Get()->GetRowAttributes();
}

TAsyncError TChunkSequenceReader::AsyncNextRow()
{
    YASSERT(!State.HasRunningOperation());
    YASSERT(IsValid());

    State.StartOperation();
    
    // This is a performance-critical spot. Try to avoid using callbacks for synchronously fetched rows.
    auto asyncResult = Readers[CurrentReader].Get()->AsyncNextRow();
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

    return Readers[CurrentReader].Get()->GetKey();
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
