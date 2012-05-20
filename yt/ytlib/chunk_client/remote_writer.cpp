#include "stdafx.h"
#include "remote_writer.h"
#include "config.h"
#include "holder_channel_cache.h"

#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/chunk_holder/chunk_holder_service.pb.h>

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/metric.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/actions/parallel_awaiter.h>

#include <util/generic/yexception.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NChunkHolder::NProto;
using namespace NChunkServer;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkWriterLogger;

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
    i64 GetSize() const;

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
    std::vector<bool> IsSent;

    std::vector<TSharedRef> Blocks;
    int StartBlockIndex;

    i64 Size;

    TWeakPtr<TRemoteWriter> Writer;

    NLog::TTaggedLogger& Logger;

    /*!
     * \note Thread affinity: WriterThread.
     */
    void PutGroup();

    /*!
     * \note Thread affinity: WriterThread.
     */
    TProxy::TInvPutBlocks PutBlocks(THolderPtr holder);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnPutBlocks(THolderPtr holder, TProxy::TRspPutBlocksPtr rsp);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void SendGroup(THolderPtr srcHolder);

    /*!
     * \note Thread affinity: WriterThread.
     */
    TProxy::TInvSendBlocks SendBlocks(THolderPtr srcHolder, THolderPtr dstHolder);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void CheckSendResponse(
        THolderPtr srcHolder, 
        THolderPtr dstHolder,
        TRemoteWriter::TProxy::TRspSendBlocksPtr rsp);

    /*!
     * \note Thread affinity: WriterThread.
     */
    void OnSentBlocks(THolderPtr srcHolder, THolderPtr dstHolder, TProxy::TRspSendBlocksPtr rsp);
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
    return StartBlockIndex + Blocks.size() - 1;
}

i64 TRemoteWriter::TGroup::GetSize() const
{
    return Size;
}

bool TRemoteWriter::TGroup::IsWritten() const
{
    auto writer = Writer.Lock();
    YASSERT(writer);

    VERIFY_THREAD_AFFINITY(writer->WriterThread);

    for (int holderIndex = 0; holderIndex < IsSent.size(); ++holderIndex) {
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
        YASSERT(holderIndex < writer->Holders.size());
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

TRemoteWriter::TProxy::TInvPutBlocks
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

void TRemoteWriter::TGroup::OnPutBlocks(THolderPtr holder, TProxy::TRspPutBlocksPtr rsp)
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

    for (int dstHolderIndex = 0; dstHolderIndex < IsSent.size(); ++dstHolderIndex) {
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

TRemoteWriter::TProxy::TInvSendBlocks
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
    req->set_block_count(Blocks.size());
    req->set_address(dstHolder->Address);
    return req->Invoke();
}

void TRemoteWriter::TGroup::CheckSendResponse(
    THolderPtr srcHolder,
    THolderPtr dstHolder,
    TRemoteWriter::TProxy::TRspSendBlocksPtr rsp)
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
    TProxy::TRspSendBlocksPtr rsp)
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
    for (int holderIndex = 0; holderIndex < IsSent.size(); ++holderIndex) {
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
    const TRemoteWriterConfigPtr& config, 
    const TChunkId& chunkId,
    const std::vector<Stroka>& addresses)
    : Config(config)
    , ChunkId(chunkId) 
    , Addresses(addresses)
    , IsOpen(false)
    , IsInitComplete(false)
    , IsClosing(false)
    , IsCloseRequested(false)
    , WindowSlots(config->WindowSize)
    , AliveHolderCount(addresses.size())
    , CurrentGroup(New<TGroup>(AliveHolderCount, 0, this))
    , BlockCount(0)
    , StartChunkTiming(0, 1000, 20)
    , PutBlocksTiming(0, 1000, 20)
    , SendBlocksTiming(0, 1000, 20)
    , FlushBlockTiming(0, 1000, 20)
    , FinishChunkTiming(0, 1000, 20)
    , Logger(ChunkWriterLogger)
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

TRemoteWriter::TProxy::TInvFlushBlock
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

void TRemoteWriter::OnBlockFlushed(THolderPtr holder, int blockIndex, TProxy::TRspFlushBlockPtr rsp)
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

        LOG_DEBUG("Window %d-%d shifted (Size: %" PRId64 ")",
            group->GetStartBlockIndex(),
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

    LOG_DEBUG("Added block group (Group: %p, BlockIndexes: %d-%d)",
        ~group,
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

    LOG_INFO("Node %s failed, %d nodes remaining",
        ~holder->Address,
        AliveHolderCount);

    if (State.IsActive() && AliveHolderCount == 0) {
        TError error(
            TError::Fail,
            Sprintf("All target nodes [%s] have failed", ~JoinToString(Addresses)));
        LOG_WARNING("Chunk writer failed\n%s", ~error.ToString());
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
        LOG_ERROR("Error reported by node %s\n%s",
            ~holder->Address, 
            ~rsp->GetError().ToString());
        OnHolderFailed(holder);
    }
}

TRemoteWriter::TProxy::TInvStartChunk
TRemoteWriter::StartChunk(THolderPtr holder)
{
    LOG_DEBUG("Starting chunk at %s", ~holder->Address);

    auto req = holder->Proxy.StartChunk();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    return req->Invoke();
}

void TRemoteWriter::OnChunkStarted(THolderPtr holder, TProxy::TRspStartChunkPtr rsp)
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

void TRemoteWriter::OnChunkFinished(THolderPtr holder, TProxy::TRspFinishChunkPtr rsp)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto& chunkInfo = rsp->chunk_info();
    LOG_DEBUG("Chunk is finished at %s (Size: %" PRId64 ")",
        ~holder->Address,
        chunkInfo.size());

    // If ChunkInfo is set.
    if (ChunkInfo.has_size()) {
        if (ChunkInfo.meta_checksum() != chunkInfo.meta_checksum() ||
            ChunkInfo.size() != chunkInfo.size()) 
        {
            LOG_FATAL("Mismatched chunk info reported by %s (KnownInfo: {%s}, NewInfo: {%s})",
                ~holder->Address,
                ~ChunkInfo.DebugString(),
                ~chunkInfo.DebugString());
        }
    } else {
        ChunkInfo = chunkInfo;
    }
}

TRemoteWriter::TProxy::TInvFinishChunk
TRemoteWriter::FinishChunk(THolderPtr holder)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    LOG_DEBUG("Finishing chunk at %s", ~holder->Address);

    auto req = holder->Proxy.FinishChunk();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    *req->mutable_chunk_meta() = ChunkMeta;
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
    YASSERT(!IsClosing);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    i64 blocksSize = 0;
    FOREACH (const auto& block, blocks) {
        blocksSize += block.Size();
    }

    State.StartOperation();

    WindowSlots.AsyncAcquire(blocksSize).Subscribe(BIND(
        &TRemoteWriter::DoWriteBlocks,
        MakeWeak(this),
        blocks));

    return State.GetOperationError();
}

void TRemoteWriter::DoWriteBlocks(const std::vector<TSharedRef>& blocks)
{
    if (State.IsActive()) {
        AddBlocks(blocks);
    }

    State.FinishOperation();
}

void TRemoteWriter::AddBlocks(const std::vector<TSharedRef>& blocks)
{
    YASSERT(!IsClosing);

    FOREACH (const auto& block, blocks) {

        CurrentGroup->AddBlock(block);
        ++BlockCount;

        LOG_DEBUG("Added block %d (Group: %p, Size: %" PRISZT ")",
            BlockCount,
            ~CurrentGroup,
            block.Size());
        if (CurrentGroup->GetSize() >= Config->GroupSize) {
            WriterThread->GetInvoker()->Invoke(BIND(
                &TRemoteWriter::AddGroup,
                MakeWeak(this),
                CurrentGroup));
            // Construct a new (empty) group.
            CurrentGroup = New<TGroup>(Holders.size(), BlockCount, this);
        }
    }
}

void TRemoteWriter::DoClose()
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YASSERT(!IsCloseRequested);

    LOG_DEBUG("Writer close requested");

    if (!State.IsActive()) {
        State.FinishOperation();
        return;
    }

    if (CurrentGroup->GetSize() > 0) {
        AddGroup(CurrentGroup);
    }

    IsCloseRequested = true;

    if (Window.empty() && IsInitComplete) {
        CloseSession();
    }
}

TAsyncError TRemoteWriter::AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta)
{
    YASSERT(IsOpen);
    YASSERT(!IsClosing);
    YASSERT(!State.HasRunningOperation());
    YASSERT(!State.IsClosed());

    IsClosing = true;
    ChunkMeta = chunkMeta;

    LOG_DEBUG("Requesting writer to close");
    State.StartOperation();

    WriterThread->GetInvoker()->Invoke(BIND(&TRemoteWriter::DoClose, MakeWeak(this)));

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

const NChunkHolder::NProto::TChunkInfo& TRemoteWriter::GetChunkInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return ChunkInfo;
}

const std::vector<Stroka> TRemoteWriter::GetNodeAddresses() const
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

const TChunkId& TRemoteWriter::GetChunkId() const
{
    return ChunkId;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
