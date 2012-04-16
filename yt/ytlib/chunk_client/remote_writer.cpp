#include "stdafx.h"
#include "remote_writer.h"
#include "holder_channel_cache.h"
#include "writer_thread.h"

#include <ytlib/chunk_holder/extensions.h>
#include <ytlib/chunk_holder/chunk_holder_service.pb.h>

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/metric.h>
#include <ytlib/misc/string.h>
#include <ytlib/actions/parallel_awaiter.h>

#include <util/generic/yexception.h>

#include <contrib/libs/protobuf/text_format.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NChunkHolder::NProto;
using namespace NChunkServer;

using namespace google::protobuf;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkClientLogger;

///////////////////////////////////////////////////////////////////////////////

struct TRemoteWriter::THolder 
    : public TRefCounted
{
    int Index;
    bool IsAlive;
    const Stroka Address;
    TProxy Proxy;
    TDelayedInvoker::TCookie Cookie;

    THolder(int index, const Stroka& address, TDuration timeout)
        : Index(index)
        , IsAlive(true)
        , Address(address)
        , Proxy(~HolderChannelCache->GetChannel(address))
    {
        Proxy.SetDefaultTimeout(timeout);
    }
};

///////////////////////////////////////////////////////////////////////////////

class TRemoteWriter::TGroup 
    : public TRefCounted
{
public:
    TGroup(
        int holderCount, 
        int startBlockIndex, 
        TRemoteWriter* writer);

    void AddBlock(const TSharedRef& block);
    void Process();
    bool IsWritten() const;
    int GetSize() const;

    /*!
     * \note Thread affinity: any.
     */
    int GetStartBlockIndex() const;

    /*!
     * \note Thread affinity: any.
     */
    int GetEndBlockIndex() const;

    /*!
     * \note Thread affinity: WriterThread.
     */
    bool IsFlushing() const;

    /*!
     * \note Thread affinity: WriterThread.
     */
    void SetFlushing();

private:
    bool IsFlushing_;
    yvector<bool> IsSent;

    yvector<TSharedRef> Blocks;
    int StartBlockIndex;

    int Size;

    TWeakPtr<TRemoteWriter> Writer;

    NLog::TTaggedLogger& Logger;

    /*!
     * \note Thread affinity: WriterThread.
     */
    void PutGroup();

    /*!
     * \note Thread affinity: WriterThread.
     */
    TProxy::TInvPutBlocks::TPtr PutBlocks(THolderPtr holder);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnPutBlocks(THolderPtr holder, TProxy::TRspPutBlocks::TPtr rsp);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void SendGroup(THolderPtr srcHolder);

    /*!
     * \note Thread affinity: WriterThread.
     */
    TProxy::TInvSendBlocks::TPtr SendBlocks(THolderPtr srcHolder, THolderPtr dstHolder);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void CheckSendResponse(
        THolderPtr srcHolder, 
        THolderPtr dstHolder,
        TRemoteWriter::TProxy::TRspSendBlocks::TPtr rsp);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnSentBlocks(THolderPtr srcHolder, THolderPtr dstHolder, TProxy::TRspSendBlocks::TPtr rsp);
};

///////////////////////////////////////////////////////////////////////////////

TRemoteWriter::TGroup::TGroup(
    int holderCount, 
    int startBlockIndex, 
    TRemoteWriter* writer)
    : IsFlushing_(false)
    , IsSent(holderCount, false)
    , StartBlockIndex(startBlockIndex)
    , Size(0)
    , Writer(writer)
    , Logger(writer->Logger)
{ }

void TRemoteWriter::TGroup::AddBlock(const TSharedRef& block)
{
    Blocks.push_back(block);
    Size += block.Size();
}

int TRemoteWriter::TGroup::GetStartBlockIndex() const
{
    return StartBlockIndex;
}

int TRemoteWriter::TGroup::GetEndBlockIndex() const
{
    return StartBlockIndex + Blocks.ysize() - 1;
}

int TRemoteWriter::TGroup::GetSize() const
{
    return Size;
}

bool TRemoteWriter::TGroup::IsWritten() const
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int holderIndex = 0; holderIndex < IsSent.ysize(); ++holderIndex) {
        if (writer->Holders[holderIndex]->IsAlive && !IsSent[holderIndex]) {
            return false;
        }
    }
    return true;
}

void TRemoteWriter::TGroup::PutGroup()
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    int holderIndex = 0;
    while (!writer->Holders[holderIndex]->IsAlive) {
        ++holderIndex;
        YASSERT(holderIndex < writer->Holders.ysize());
    }

    auto holder = writer->Holders[holderIndex];
    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    auto onSuccess = BIND(
        &TGroup::OnPutBlocks, 
        MakeWeak(this), 
        holder);
    auto onResponse = BIND(
        &TRemoteWriter::CheckResponse<TProxy::TRspPutBlocks>,
        Writer,
        holder, 
        onSuccess,
        &writer->PutBlocksTiming);
    awaiter->Await(PutBlocks(holder), onResponse);
    awaiter->Complete(BIND(
        &TRemoteWriter::TGroup::Process, 
        MakeWeak(this)));
}

TRemoteWriter::TProxy::TInvPutBlocks::TPtr
TRemoteWriter::TGroup::PutBlocks(THolderPtr holder)
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    auto req = holder->Proxy.PutBlocks();
    *req->mutable_chunk_id() = writer->ChunkId.ToProto();
    req->set_start_block_index(StartBlockIndex);
    req->Attachments().insert(req->Attachments().begin(), Blocks.begin(), Blocks.end());

    LOG_DEBUG("Putting blocks %d-%d to %s",
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~holder->Address);

    return req->Invoke();
}

void TRemoteWriter::TGroup::OnPutBlocks(THolderPtr holder, TProxy::TRspPutBlocks::TPtr rsp)
{
    auto writer = Writer.Lock();
    if (!writer)
        return;

    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    IsSent[holder->Index] = true;

    LOG_DEBUG("Blocks %d-%d are put to %s",
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~holder->Address);

    writer->SchedulePing(holder);
}

void TRemoteWriter::TGroup::SendGroup(THolderPtr srcHolder)
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int dstHolderIndex = 0; dstHolderIndex < IsSent.ysize(); ++dstHolderIndex) {
        auto dstHolder = writer->Holders[dstHolderIndex];
        if (dstHolder->IsAlive && !IsSent[dstHolderIndex]) {
            auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
            auto onResponse = BIND(
                &TGroup::CheckSendResponse,
                MakeWeak(this),
                srcHolder,
                dstHolder);
            awaiter->Await(SendBlocks(srcHolder, dstHolder), onResponse);
            awaiter->Complete(BIND(&TGroup::Process, MakeWeak(this)));
            break;
        }
    }
}

TRemoteWriter::TProxy::TInvSendBlocks::TPtr
TRemoteWriter::TGroup::SendBlocks(
    THolderPtr srcHolder, 
    THolderPtr dstHolder)
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    LOG_DEBUG("Sending blocks %d-%d from %s to %s",
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~srcHolder->Address,
        ~dstHolder->Address);

    auto req = srcHolder->Proxy.SendBlocks();
    *req->mutable_chunk_id() = writer->ChunkId.ToProto();
    req->set_start_block_index(StartBlockIndex);
    req->set_block_count(Blocks.ysize());
    req->set_address(dstHolder->Address);
    return req->Invoke();
}

void TRemoteWriter::TGroup::CheckSendResponse(
    THolderPtr srcHolder,
    THolderPtr dstHolder,
    TRemoteWriter::TProxy::TRspSendBlocks::TPtr rsp)
{
    auto writer = Writer.Lock();
    if (!writer)
        return;

    if (rsp->GetErrorCode() == EErrorCode::PutBlocksFailed) {
        writer->OnHolderFailed(dstHolder);
        return;
    }

    auto onSuccess = BIND(
        &TGroup::OnSentBlocks, 
        Unretained(this), // No need for a smart pointer here -- we're invoking action directly.
        srcHolder, 
        dstHolder);

    writer->CheckResponse<TRemoteWriter::TProxy::TRspSendBlocks>(
        srcHolder, 
        onSuccess,
        &writer->SendBlocksTiming,
        rsp);
}

void TRemoteWriter::TGroup::OnSentBlocks(
    THolderPtr srcHolder, 
    THolderPtr dstHolder,
    TProxy::TRspSendBlocks::TPtr rsp)
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    LOG_DEBUG("Blocks %d-%d are sent from %s to %s",
        StartBlockIndex, 
        GetEndBlockIndex(),
        ~srcHolder->Address,
        ~dstHolder->Address);

    IsSent[dstHolder->Index] = true;

    writer->SchedulePing(srcHolder);
    writer->SchedulePing(dstHolder);
}

bool TRemoteWriter::TGroup::IsFlushing() const
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    return IsFlushing_;
}

void TRemoteWriter::TGroup::SetFlushing()
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    IsFlushing_ = true;
}

void TRemoteWriter::TGroup::Process()
{
    auto writer = Writer.Lock();
    if (!writer)
        return;

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    if (!writer->State.IsActive()) {
        return;
    }

    YASSERT(writer->IsInitComplete);

    LOG_DEBUG("Processing blocks %d-%d",
        StartBlockIndex, 
        GetEndBlockIndex());

    THolderPtr holderWithBlocks;
    bool emptyHolderFound = false;
    for (int holderIndex = 0; holderIndex < IsSent.ysize(); ++holderIndex) {
        auto holder = writer->Holders[holderIndex];
        if (holder->IsAlive) {
            if (IsSent[holderIndex]) {
                holderWithBlocks = holder;
            } else {
                emptyHolderFound = true;
            }
        }
    }

    if (!emptyHolderFound) {
        writer->ShiftWindow();
    } else if (!holderWithBlocks) {
        PutGroup();
    } else {
        SendGroup(holderWithBlocks);
    }
}

///////////////////////////////////////////////////////////////////////////////

TRemoteWriter::TRemoteWriter(
    TRemoteWriter::TConfig* config, 
    const TChunkId& chunkId,
    const yvector<Stroka>& addresses)
    : Config(config)
    , ChunkId(chunkId) 
    , Addresses(addresses)
    , IsOpen(false)
    , IsInitComplete(false)
    , IsCloseRequested(false)
    , WindowSlots(config->WindowSize)
    , AliveHolderCount(addresses.ysize())
    , CurrentGroup(New<TGroup>(AliveHolderCount, 0, this))
    , BlockCount(0)
    , StartChunkTiming(0, 1000, 20)
    , PutBlocksTiming(0, 1000, 20)
    , SendBlocksTiming(0, 1000, 20)
    , FlushBlockTiming(0, 1000, 20)
    , FinishChunkTiming(0, 1000, 20)
    , Logger(ChunkClientLogger)
{
    YASSERT(AliveHolderCount > 0);

    Logger.AddTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));

    for (int index = 0; index < static_cast<int>(addresses.size()); ++index) {
        auto address = addresses[index];
        auto holder = New<THolder>(
            index,
            address,
            Config->HolderRpcTimeout);
        Holders.push_back(holder);
    }
}

TRemoteWriter::~TRemoteWriter()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Just a quick check.
    if (!State.IsActive())
        return;

    LOG_DEBUG("Writer canceled");
    State.Cancel(TError(TError::Fail, "Writer canceled"));
}

void TRemoteWriter::Open()
{
    LOG_DEBUG("Opening writer (Addresses: [%s])", ~JoinToString(Addresses));

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    FOREACH (auto holder, Holders) {
        auto onSuccess = BIND(
            &TRemoteWriter::OnChunkStarted, 
            MakeWeak(this), 
            holder);
        auto onResponse = BIND(
            &TRemoteWriter::CheckResponse<TProxy::TRspStartChunk>,
            MakeWeak(this),
            holder,
            onSuccess,
            &StartChunkTiming);
        awaiter->Await(StartChunk(holder), onResponse);
    }
    awaiter->Complete(BIND(&TRemoteWriter::OnSessionStarted, MakeWeak(this)));

    IsOpen = true;
}

void TRemoteWriter::ShiftWindow()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!State.IsActive()) {
        YASSERT(Window.empty());
        return;
    }

    int lastFlushableBlock = -1;
    for (auto it = Window.begin(); it != Window.end(); ++it) {
        auto group = *it;
        if (!group->IsFlushing()) {
            if (group->IsWritten()) {
                lastFlushableBlock = group->GetEndBlockIndex();
                group->SetFlushing();
            } else {
                break;
            }
        }
    }

    if (lastFlushableBlock < 0)
        return;

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    FOREACH (auto holder, Holders) {
        if (holder->IsAlive) {
            auto onSuccess = BIND(
                &TRemoteWriter::OnBlockFlushed, 
                MakeWeak(this), 
                holder,
                lastFlushableBlock);
            auto onResponse = BIND(
                &TRemoteWriter::CheckResponse<TProxy::TRspFlushBlock>,
                MakeWeak(this), 
                holder, 
                onSuccess,
                &FlushBlockTiming);
            awaiter->Await(FlushBlock(holder, lastFlushableBlock), onResponse);
        }
    }

    awaiter->Complete(BIND(
        &TRemoteWriter::OnWindowShifted, 
        MakeWeak(this),
        lastFlushableBlock));
}

TRemoteWriter::TProxy::TInvFlushBlock::TPtr
TRemoteWriter::FlushBlock(THolderPtr holder, int blockIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Flushing block %d at %s",
        blockIndex,
        ~holder->Address);

    auto req = holder->Proxy.FlushBlock();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    req->set_block_index(blockIndex);
    return req->Invoke();
}

void TRemoteWriter::OnBlockFlushed(THolderPtr holder, int blockIndex, TProxy::TRspFlushBlock::TPtr rsp)
{
    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Block %d is flushed at %s",
        blockIndex,
        ~holder->Address);

    SchedulePing(holder);
}

void TRemoteWriter::OnWindowShifted(int lastFlushedBlock)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (Window.empty()) {
        // This happens when FlushBlocks responses are disordered
        // (i.e. a bigger BlockIndex is flushed before a smaller one)
        // and prevents repeated calling CloseSession
        return;
    }

    while (!Window.empty()) {
        auto group = Window.front();
        if (group->GetEndBlockIndex() > lastFlushedBlock)
            return;

        LOG_DEBUG("Window shifted (BlockIndex: %d, Size: %d)",
            group->GetEndBlockIndex(),
            group->GetSize());

        WindowSlots.Release(group->GetSize());
        Window.pop_front();
    }

    if (State.IsActive() && IsCloseRequested) {
        CloseSession();
    }
}

void TRemoteWriter::AddGroup(TGroupPtr group)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YASSERT(!IsCloseRequested);

    if (!State.IsActive())
        return;

    LOG_DEBUG("Added blocks %d-%d",
        group->GetStartBlockIndex(),
        group->GetEndBlockIndex());

    Window.push_back(group);

    if (IsInitComplete) {
        group->Process();
    }
}

void TRemoteWriter::OnHolderFailed(THolderPtr holder)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!holder->IsAlive)
        return;

    holder->IsAlive = false;
    --AliveHolderCount;

    LOG_INFO("Holder %s failed, %d holders remaining",
        ~holder->Address,
        AliveHolderCount);

    if (State.IsActive() && AliveHolderCount == 0) {
        TError error(
            TError::Fail,
            Sprintf("All target holders failed (Addresses: [%s])", ~JoinToString(Addresses)));
        LOG_WARNING("Chunk writing failed\n%s", ~error.ToString());
        State.Fail(error);
    }
}

template <class TResponse>
void TRemoteWriter::CheckResponse(
    THolderPtr holder,
    TCallback<void(TIntrusivePtr<TResponse>)> onSuccess, 
    TMetric* metric,
    TIntrusivePtr<TResponse> rsp)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (rsp->IsOK()) {
        metric->AddDelta(rsp->GetStartTime());
        onSuccess.Run(rsp);
    } else {
        // TODO: retry?
        LOG_ERROR("Error reported by holder %s\n%s",
            ~holder->Address, 
            ~rsp->GetError().ToString());
        OnHolderFailed(holder);
    }
}

TRemoteWriter::TProxy::TInvStartChunk::TPtr TRemoteWriter::StartChunk(THolderPtr holder)
{
    LOG_DEBUG("Starting chunk at %s", ~holder->Address);

    auto req = holder->Proxy.StartChunk();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    return req->Invoke();
}

void TRemoteWriter::OnChunkStarted(THolderPtr holder, TProxy::TRspStartChunk::TPtr rsp)
{
    UNUSED(rsp);
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Chunk started at %s", ~holder->Address);

    SchedulePing(holder);
}

void TRemoteWriter::OnSessionStarted()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Check if the session is not canceled yet.
    if (!State.IsActive()) {
        return;
    }

    LOG_DEBUG("Writer is ready");

    IsInitComplete = true;
    FOREACH (auto& group, Window) {
        group->Process();
    }

    // Possible for an empty chunk.
    if (Window.empty() && IsCloseRequested) {
        CloseSession();
    }
}

void TRemoteWriter::CloseSession()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YASSERT(IsCloseRequested);

    LOG_DEBUG("Closing writer");

    auto awaiter = New<TParallelAwaiter>(WriterThread->GetInvoker());
    FOREACH (auto holder, Holders) {
        if (holder->IsAlive) {
            auto onSuccess = BIND(
                &TRemoteWriter::OnChunkFinished, 
                MakeWeak(this), 
                holder);
            auto onResponse = BIND(
                &TRemoteWriter::CheckResponse<TProxy::TRspFinishChunk>,
                MakeWeak(this), 
                holder, 
                onSuccess, 
                &FinishChunkTiming);
            awaiter->Await(FinishChunk(holder), onResponse);
        }
    }
    awaiter->Complete(BIND(&TRemoteWriter::OnSessionFinished, MakeWeak(this)));
}

void TRemoteWriter::OnChunkFinished(THolderPtr holder, TProxy::TRspFinishChunk::TPtr rsp)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto& chunkInfo = rsp->chunk_info();
    LOG_DEBUG("Chunk is finished (Address: %s, Size: %" PRId64 ")",
        ~holder->Address,
        chunkInfo.size());

    // If ChunkInfo is set.
    if (ChunkInfo.has_size()) {
        if (ChunkInfo.meta_checksum() != chunkInfo.meta_checksum() ||
            ChunkInfo.size() != chunkInfo.size()) 
        {
            Stroka knownInfo, newInfo;
            TextFormat::PrintToString(ChunkInfo, &knownInfo);
            TextFormat::PrintToString(chunkInfo, &newInfo);
            LOG_FATAL("Mismatched chunk info reported by holder (KnownInfo: %s, NewInfo: %s, Address: %s)",
                ~knownInfo,
                ~newInfo,
                ~holder->Address);
        }
    } else {
        ChunkInfo = chunkInfo;
    }
}

TRemoteWriter::TProxy::TInvFinishChunk::TPtr
TRemoteWriter::FinishChunk(THolderPtr holder)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Finishing chunk at %s", ~holder->Address);

    auto req = holder->Proxy.FinishChunk();
    req->mutable_chunk_meta()->CopyFrom(ChunkMeta);
    return req->Invoke();
}

void TRemoteWriter::OnSessionFinished()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    YASSERT(Window.empty());

    if (State.IsActive()) {
        State.Close();
    }

    CancelAllPings();

    SetProtoExtension(ChunkMeta.mutable_extensions(), ChunkInfo);
    LOG_DEBUG("Writer closed");

    State.FinishOperation();
}

void TRemoteWriter::PingSession(THolderPtr holder)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Sending ping to %s", ~holder->Address);

    auto req = holder->Proxy.PingSession();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    req->Invoke();

    SchedulePing(holder);
}

void TRemoteWriter::SchedulePing(THolderPtr holder)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!State.IsActive()) {
        return;
    }

    TDelayedInvoker::CancelAndClear(holder->Cookie);
    holder->Cookie = TDelayedInvoker::Submit(
        BIND(
            &TRemoteWriter::PingSession,
            MakeWeak(this),
            holder)
        .Via(WriterThread->GetInvoker()),
        Config->SessionPingInterval);
}

void TRemoteWriter::CancelPing(THolderPtr holder)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    TDelayedInvoker::CancelAndClear(holder->Cookie);
}

void TRemoteWriter::CancelAllPings()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    FOREACH (auto holder, Holders) {
        CancelPing(holder);
    }
}

TAsyncError TRemoteWriter::AsyncWriteBlocks(const std::vector<TSharedRef>& blocks)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(IsOpen);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    i64 sumSize = 0;
    FOREACH (auto& block, blocks) {
        sumSize += block.Size();
    }

    State.StartOperation();

    WindowSlots.AsyncAcquire(sumSize)->Subscribe(BIND(
        &TRemoteWriter::DoWriteBlocks,
        MakeWeak(this),
        blocks));

    return State.GetOperationError();
}

void TRemoteWriter::DoWriteBlocks(const std::vector<TSharedRef>& blocks, TVoid)
{
    if (State.IsActive()) {
        AddBlocks(blocks);
    }

    State.FinishOperation();
}

void TRemoteWriter::AddBlocks(const std::vector<TSharedRef>& blocks)
{
    FOREACH (auto& block, blocks) {
        LOG_DEBUG("Block added (BlockIndex: %d)", BlockCount);

        CurrentGroup->AddBlock(block);
        ++BlockCount;

        if (CurrentGroup->GetSize() >= Config->GroupSize) {
            WriterThread->GetInvoker()->Invoke(BIND(
                &TRemoteWriter::AddGroup,
                MakeWeak(this),
                CurrentGroup));
            // Construct a new (empty) group.
            CurrentGroup = New<TGroup>(Holders.ysize(), BlockCount, this);
        }
    }
}

void TRemoteWriter::DoClose(
    const std::vector<TSharedRef>& lastBlocks,
    TVoid)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YASSERT(!IsCloseRequested);

    LOG_DEBUG("Writer close requested");

    if (!State.IsActive()) {
        State.FinishOperation();
        return;
    }

    AddBlocks(MoveRV(lastBlocks));

    if (CurrentGroup->GetSize() > 0) {
        AddGroup(CurrentGroup);
    }

    IsCloseRequested = true;

    if (Window.empty() && IsInitComplete) {
        CloseSession();
    }
}

TAsyncError TRemoteWriter::AsyncClose(
    const std::vector<TSharedRef>& lastBlocks,
    const NChunkHolder::NProto::TChunkMeta& chunkMeta)
{
    YASSERT(IsOpen);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    i64 sumSize = 0;
    FOREACH (auto& block, lastBlocks) {
        sumSize += block.Size();
    }

    ChunkMeta.CopyFrom(chunkMeta);
    *ChunkMeta.mutable_id() = ChunkId.ToProto();

    LOG_DEBUG("Requesting writer to close.");
    State.StartOperation();

    // XXX(sandello): Do you realize, that lastBlocks and meta are copied back and forth here?
    WindowSlots.AsyncAcquire(sumSize)->Subscribe(BIND(
        &TRemoteWriter::DoClose,
        MakeWeak(this),
        lastBlocks).Via(WriterThread->GetInvoker()));

    return State.GetOperationError();
}

Stroka TRemoteWriter::GetDebugInfo()
{
    return Sprintf(
        "ChunkId: %s; "
        "StartChunk: (%s); "
        "FinishChunk timing: (%s); "
        "PutBlocks timing: (%s); "
        "SendBlocks timing: (%s); "
        "FlushBlocks timing: (%s); ",
        ~ChunkId.ToString(), 
        ~StartChunkTiming.GetDebugInfo(),
        ~FinishChunkTiming.GetDebugInfo(),
        ~PutBlocksTiming.GetDebugInfo(),
        ~SendBlocksTiming.GetDebugInfo(),
        ~FlushBlockTiming.GetDebugInfo());
}

const NChunkHolder::NProto::TChunkMeta& TRemoteWriter::GetChunkMeta() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return ChunkMeta;
}

const std::vector<Stroka> TRemoteWriter::GetHolders() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    std::vector<Stroka> addresses;
    FOREACH (auto holder, Holders) {
        if (holder->IsAlive) {
            addresses.push_back(holder->Address);
        }
    }
    return addresses;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
