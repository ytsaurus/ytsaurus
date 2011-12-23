#include "stdafx.h"
#include "remote_reader.h"
#include "holder_channel_cache.h"

#include "../misc/foreach.h"
#include "../misc/string.h"
#include "../misc/metric.h"
#include "../misc/thread_affinity.h"
#include "../logging/tagged_logger.h"
#include "../actions/action_util.h"
#include "../chunk_holder/chunk_holder_service_proxy.h"

#include <util/random/shuffle.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkHolder::NProto;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkClientLogger;

///////////////////////////////////////////////////////////////////////////////

class TRemoteReader
    : public IAsyncReader
{
public:
    typedef TIntrusivePtr<TRemoteReader> TPtr;
    typedef TRemoteReaderConfig TConfig;

    TRemoteReader(
        TConfig* config,
        const TChunkId& chunkId,
        const yvector<Stroka>& holderAddresses);

    TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);

    TFuture<IAsyncReader::TGetInfoResult>::TPtr AsyncGetChunkInfo();

private:
    typedef NChunkHolder::TChunkHolderServiceProxy TProxy;

    void DoReadBlocks(
        const yvector<int>& blockIndexes, 
        TFuture<TReadResult>::TPtr result);

    void OnBlocksRead(
        TProxy::TRspGetBlocks::TPtr response,
        TFuture<TReadResult>::TPtr result,
        const yvector<int>& blockIndexes,
        int holderIndex);

    void DoGetChunkInfo(
        TFuture<TGetInfoResult>::TPtr result);

    void OnGotChunkInfo(
        TProxy::TRspGetChunkInfo::TPtr response,
        TFuture<TGetInfoResult>::TPtr result,
        int holderIndex);

    bool GetCurrentHolderIndex(int* holderIndex) const;
    bool ChangeCurrentHolder(int holderIndex, const TError& error);

    TError GetCumulativeError() const;

    TConfig::TPtr Config;
    const TChunkId ChunkId;

    yvector<Stroka> HolderAddresses;

    TMetric ExecutionTime;

    NLog::TTaggedLogger Logger;

    TSpinLock SpinLock;
    int CurrentHolderIndex;
    Stroka CumulativeErrorMessage;
    TError CumulativeError;

};

///////////////////////////////////////////////////////////////////////////////

TRemoteReader::TRemoteReader(
    TConfig* config,
    const TChunkId& chunkId,
    const yvector<Stroka>& holderAddresses)
    : Config(config)
    , ChunkId(chunkId)
    , HolderAddresses(holderAddresses)
    , ExecutionTime(0, 1000, 20)
    , CurrentHolderIndex(0)
    , Logger(ChunkClientLogger)
{
    YASSERT(config);

    Shuffle(HolderAddresses.begin(), HolderAddresses.end());

    Logger.SetTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));
    LOG_DEBUG("Reader created (Addresses: [%s])", ~JoinToString(HolderAddresses));
}

TFuture<IAsyncReader::TReadResult>::TPtr
TRemoteReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto result = New< TFuture<TReadResult> >();
    DoReadBlocks(blockIndexes, result);
    return result;
}

void TRemoteReader::DoReadBlocks(
    const yvector<int>& blockIndexes, 
    TFuture<TReadResult>::TPtr result)
{
    VERIFY_THREAD_AFFINITY_ANY();

    int holderIndex;
    if (!GetCurrentHolderIndex(&holderIndex)) {
        result->Set(GetCumulativeError());
        return;
    }
    auto holderAddress = HolderAddresses[holderIndex];

    LOG_DEBUG("Requesting blocks from holder (HolderAddress: %s, BlockIndexes: [%s])",
        ~holderAddress,
        ~JoinToString(blockIndexes));

    TProxy proxy(~HolderChannelCache->GetChannel(HolderAddresses[holderIndex]));
    proxy.SetTimeout(Config->HolderRpcTimeout);

    auto request = proxy.GetBlocks();
    request->set_chunk_id(ChunkId.ToProto());

    FOREACH(auto index, blockIndexes) {
        request->add_block_indexes(index);
    }

    request->Invoke()->Subscribe(FromMethod(
        &TRemoteReader::OnBlocksRead, 
        TPtr(this), 
        result,
        blockIndexes,
        holderIndex));
}

void TRemoteReader::OnBlocksRead(
    TProxy::TRspGetBlocks::TPtr response,
    TFuture<TReadResult>::TPtr asyncResult, 
    const yvector<int>& blockIndexes,
    int holderIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (response->IsOK()) {
        LOG_DEBUG("Blocks received (HolderAddress: %s)", ~HolderAddresses[holderIndex]);
        ExecutionTime.AddDelta(response->GetStartTime());
        asyncResult->Set(TReadResult(MoveRV(response->Attachments())));
        return;
    }
    
    LOG_WARNING("Error requesting blocks from holder (HolderAddress: %s)%s",
        ~HolderAddresses[holderIndex],
        ~response->GetError().ToString());

    if (ChangeCurrentHolder(holderIndex, response->GetError())) {
        DoReadBlocks(blockIndexes, asyncResult);
        return;
    }

    asyncResult->Set(GetCumulativeError());
}

TFuture<IAsyncReader::TGetInfoResult>::TPtr TRemoteReader::AsyncGetChunkInfo()
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_DEBUG("Getting chunk info");

    auto result = New< TFuture<TGetInfoResult> >();
    DoGetChunkInfo(result);
    return result;
}

void TRemoteReader::DoGetChunkInfo(TFuture<TGetInfoResult>::TPtr result)
{
    VERIFY_THREAD_AFFINITY_ANY();
    
    int holderIndex;
    if (!GetCurrentHolderIndex(&holderIndex)) {
        result->Set(GetCumulativeError());
        return;
    }
    auto holderAddress = HolderAddresses[holderIndex];

    TProxy proxy(~HolderChannelCache->GetChannel(holderAddress));
    proxy.SetTimeout(Config->HolderRpcTimeout);
    
    auto request = proxy.GetChunkInfo();
    request->set_chunk_id(ChunkId.ToProto());

    LOG_DEBUG("Requesting chunk info from holder (HolderAddress: %s)", ~holderAddress);

    return request->Invoke()->Subscribe(FromMethod(
        &TRemoteReader::OnGotChunkInfo,
        TPtr(this),
        result,
        holderIndex));
}

void TRemoteReader::OnGotChunkInfo(
    TProxy::TRspGetChunkInfo::TPtr response,
    TFuture<TGetInfoResult>::TPtr asyncResult,
    int holderIndex)
{
    if (response->IsOK()) {
        LOG_DEBUG("Chunk info received (HolderAddress: %s)", ~HolderAddresses[holderIndex]);
        asyncResult->Set(response->chunk_info());
        return;
    }
    
    LOG_WARNING("Error requesting chunk info (HolderAddress: %s)%s",
        ~HolderAddresses[holderIndex],
        ~response->GetError().ToString());

    if (ChangeCurrentHolder(holderIndex, response->GetError())) {
        DoGetChunkInfo(asyncResult);
        return;
    }

    auto error = GetCumulativeError();
    asyncResult->Set(error);
}

bool TRemoteReader::GetCurrentHolderIndex(int* holderIndex) const
{
    TGuard<TSpinLock> guard(SpinLock);
    if (CurrentHolderIndex < HolderAddresses.ysize()) {
        *holderIndex = CurrentHolderIndex;
        return true;
    } else {
        return false;
    }
}

bool TRemoteReader::ChangeCurrentHolder(int holderIndex, const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(!error.IsOK());

    TGuard<TSpinLock> guard(SpinLock);

    YASSERT(holderIndex <= CurrentHolderIndex);

    if (holderIndex == CurrentHolderIndex) {
        CumulativeErrorMessage += Sprintf("\n[%s] %s",
            ~HolderAddresses[holderIndex],
            ~error.ToString());

        ++CurrentHolderIndex;

        if (CurrentHolderIndex >= HolderAddresses.ysize()) {
            CumulativeError = TError(Sprintf("Remote chunk reader failed, details follow (ChunkId: %s)%s",
                ~ChunkId.ToString(),
                ~CumulativeErrorMessage));

            LOG_WARNING("%s", ~CumulativeError.ToString());
        }
    }

    return CumulativeError.IsOK();
}

TError TRemoteReader::GetCumulativeError() const
{
    YASSERT(!CumulativeError.IsOK());
    return CumulativeError;
}

///////////////////////////////////////////////////////////////////////////////

class TRemoteReaderFactory
    : public IRemoteReaderFactory
{
public:
    TRemoteReaderFactory(TRemoteReaderConfig* config)
        : Config(config)
    { }

    virtual IAsyncReader::TPtr Create(
        const TChunkId& chunkId,
        const yvector<Stroka>& holderAddresses)
    {
        return New<TRemoteReader>(
            ~Config,
            chunkId,
            holderAddresses);
    }

private:
    TRemoteReaderConfig::TPtr Config;

};

IRemoteReaderFactory::TPtr CreateRemoteReaderFactory(TRemoteReaderConfig* config)
{
    YASSERT(config);
    return New<TRemoteReaderFactory>(config);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
