#include "stdafx.h"
#include "helpers.h"
#include "private.h"

#include <core/misc/string.h>

#include <core/concurrency/parallel_awaiter.h>

#include <core/logging/log.h>

#include <ytlib/chunk_client/private.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/data_node_service_proxy.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <atomic>

namespace NYT {
namespace NJournalClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JournalClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TAbortSessionsQuorumSession
    : public TRefCounted
{
public:
    TAbortSessionsQuorumSession(
        const TChunkId& chunkId,
        const std::vector<TNodeDescriptor>& replicas,
        TDuration timeout,
        int quorum)
        : ChunkId_(chunkId)
        , Replicas_(replicas)
        , Timeout_(timeout)
        , Quorum_(quorum)
        , Logger(JournalClientLogger)
    { }

    TAsyncError Run()
    {
        BIND(&TAbortSessionsQuorumSession::DoRun, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
        return Promise_;
    }

private:
    TChunkId ChunkId_;
    std::vector<TNodeDescriptor> Replicas_;
    TDuration Timeout_;
    int Quorum_;

    int SuccessCounter_ = 0;
    int ResponseCounter_ = 0;

    std::vector<TError> InnerErrors_;

    TAsyncErrorPromise Promise_ = NewPromise<TError>();

    NLog::TLogger Logger;


    void DoRun()
    {
        LOG_INFO("Aborting journal chunk session quroum (ChunkId: %s, Addresses: [%s])",
            ~ToString(ChunkId_),
            ~JoinToString(Replicas_));

        if (Replicas_.size() < Quorum_) {
            auto error = TError("Unable to abort sessions quorum for journal chunk %s: too few replicas known, %d given, %d needed",
                ~ToString(ChunkId_),
                static_cast<int>(Replicas_.size()),
                Quorum_);
            Promise_.Set(error);
            return;
        }

        for (const auto& descriptor : Replicas_) {
            auto channel = LightNodeChannelFactory->CreateChannel(descriptor.GetDefaultAddress());
            TDataNodeServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Timeout_);
            auto req = proxy.FinishChunk();
            ToProto(req->mutable_chunk_id(), ChunkId_);
            req->Invoke().Subscribe(BIND(&TAbortSessionsQuorumSession::OnResponse, MakeStrong(this), descriptor)
                .Via(GetCurrentInvoker()));
        }
    }

    void OnResponse(const TNodeDescriptor& descriptor, TDataNodeServiceProxy::TRspFinishChunkPtr rsp)
    {
        ++ResponseCounter_;
        // NB: Missing session is also OK.
        if (rsp->IsOK() || rsp->GetError().GetCode() == NChunkClient::EErrorCode::NoSuchSession) {
            LOG_INFO("Journal chunk session aborted successfully (ChunkId: %s, Address: %s)",
                ~ToString(ChunkId_),
                ~descriptor.GetDefaultAddress());

            if (++SuccessCounter_ == Quorum_) {
                LOG_INFO("Journal chunk session quroum aborted successfully (ChunkId: %s)",
                    ~ToString(ChunkId_));
                Promise_.TrySet(TError());
            }
        } else {
            auto error = rsp->GetError();
            InnerErrors_.push_back(error);

            LOG_WARNING(error, "Failed to abort journal chunk session (ChunkId: %s, Address: %s)",
                ~ToString(ChunkId_),
                ~descriptor.GetDefaultAddress());
           
            if (ResponseCounter_ == Replicas_.size()) {
                auto combinedError = TError("Unable to abort sessions quorum for journal chunk %s",
                    ~ToString(ChunkId_))
                    << InnerErrors_;
                Promise_.TrySet(combinedError);
            }
        }
    }

};

TAsyncError AbortSessionsQuorum(
    const TChunkId& chunkId,
    const std::vector<TNodeDescriptor>& replicas,
    TDuration timeout,
    int quorum)
{
    return New<TAbortSessionsQuorumSession>(chunkId, replicas, timeout, quorum)
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TComputeQuorumRecordCountSession
    : public TRefCounted
{
public:
    TComputeQuorumRecordCountSession(
        const TChunkId& chunkId,
        const std::vector<TNodeDescriptor>& replicas,
        TDuration timeout,
        int quorum)
        : ChunkId_(chunkId)
        , Replicas_(replicas)
        , Timeout_(timeout)
        , Quorum_(quorum)
        , Logger(JournalClientLogger)
    { }

    TFuture<TErrorOr<int>> Run()
    {
        BIND(&TComputeQuorumRecordCountSession::DoRun, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
        return Promise_;
    }

private:
    TChunkId ChunkId_;
    std::vector<TNodeDescriptor> Replicas_;
    TDuration Timeout_;
    int Quorum_;

    std::vector<int> RecordCounts_;
    std::vector<TError> InnerErrors_;

    TPromise<TErrorOr<int>> Promise_ = NewPromise<TErrorOr<int>>();

    NLog::TLogger Logger;


    void DoRun()
    {
        if (Replicas_.size() < Quorum_) {
            auto error = TError("Unable to compute quorum record count for journal chunk %s: too few replicas known, %d given, %d needed",
                ~ToString(ChunkId_),
                static_cast<int>(Replicas_.size()),
                Quorum_);
            Promise_.Set(error);
            return;
        }

        LOG_INFO("Computing quorum record count for journal chunk (ChunkId: %s, Addresses: [%s])",
            ~ToString(ChunkId_),
            ~JoinToString(Replicas_));

        auto awaiter = New<TParallelAwaiter>(GetCurrentInvoker());
        for (const auto& descriptor : Replicas_) {
            auto channel = LightNodeChannelFactory->CreateChannel(descriptor.GetDefaultAddress());
            TDataNodeServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Timeout_);
            auto req = proxy.GetChunkMeta();
            ToProto(req->mutable_chunk_id(), ChunkId_);
            req->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            awaiter->Await(
                req->Invoke(),
                BIND(&TComputeQuorumRecordCountSession::OnResponse, MakeStrong(this), descriptor));
        }

        awaiter->Complete(
            BIND(&TComputeQuorumRecordCountSession::OnComplete, MakeStrong(this)));
    }

    void OnResponse(const TNodeDescriptor& descriptor, TDataNodeServiceProxy::TRspGetChunkMetaPtr rsp)
    {
        if (rsp->IsOK()) {
            auto miscExt = GetProtoExtension<TMiscExt>(rsp->chunk_meta().extensions());
            int recordCount = miscExt.record_count();
            RecordCounts_.push_back(recordCount);

            LOG_INFO("Received record count for journal chunk (ChunkId: %s, Address: %s, RecordCount: %d)",
                ~ToString(ChunkId_),
                ~descriptor.GetDefaultAddress(),
                recordCount);
        } else {
            auto error = rsp->GetError();
            InnerErrors_.push_back(error);

            LOG_WARNING(error, "Failed to get journal chunk record count (ChunkId: %s, Address: %s)",
                ~ToString(ChunkId_),
                ~descriptor.GetDefaultAddress());
           
        }
    }

    void OnComplete()
    {
        if (RecordCounts_.size() < Quorum_) {
            auto error = TError("Unable to compute quorum record count for journal chunk %s: too few replicas alive, %d found, %d needed",
                ~ToString(ChunkId_),
                static_cast<int>(RecordCounts_.size()),
                Quorum_)
                << InnerErrors_;
            Promise_.Set(error);
            return;
        }

        std::sort(RecordCounts_.begin(), RecordCounts_.end());
        int quorumRecordCount = RecordCounts_[Quorum_ - 1];

        LOG_INFO("Quorum record count for journal chunk computed successfully (ChunkId: %s, QuorumRecordCount: %d)",
            ~ToString(ChunkId_),
            quorumRecordCount);

        Promise_.Set(quorumRecordCount);
    }

};

TFuture<TErrorOr<int>> ComputeQuorumRecordCount(
    const TChunkId& chunkId,
    const std::vector<TNodeDescriptor>& replicas,
    TDuration timeout,
    int quorum)
{
    return New<TComputeQuorumRecordCountSession>(chunkId, replicas, timeout, quorum)
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalClient
} // namespace NYT

